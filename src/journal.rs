//! Durable storage: an append-only journal plus snapshots.
//!
//! `journal.log` holds one JSON line per [`Effect`], tagged with a sequence
//! number. When it grows past the rewrite threshold the journal is set aside as
//! `journal-<seq>.log`, a fresh one is started, and the state as of that moment
//! is serialized: the agents and broadcasts at once, the rooms, the messages,
//! and the keyspace a run at a time while requests go on (see
//! [`crate::capture`] and [`crate::db`]). A background thread then writes
//! `snapshot.json` (temp file, fsync, atomic rename) and deletes the journals it
//! covers. Recovery loads the snapshot and replays every journal line newer than
//! it, from the set-aside journals and then `journal.log`, so a crash at any
//! point of a rewrite loses nothing.
//!
//! Each line carries a checksum of itself. A torn final line (a crash
//! mid-write) is cut off; corruption anywhere else, a line the disk changed
//! included, stops the broker rather than silently dropping messages or
//! replaying what the disk made of them. `postino.lock` is held with an OS
//! file lock so one broker owns a data dir.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::store::{Effect, SnapshotOwned, SnapshotRef, Store};
use crate::util::{now_ms, private_dir, private_file};

const SNAPSHOT_VERSION: u32 = 1;
/// Never rewrite before the journal has grown this far, even when the last
/// snapshot was small: rewriting a small keyspace every few megabytes writes
/// the whole snapshot out again and again for little recovery time saved.
/// Unit tests keep the old floor so they exercise the automatic trigger.
const REWRITE_MIN_BYTES: u64 = if cfg!(test) {
    4 * 1024 * 1024
} else {
    64 * 1024 * 1024
};
/// How long writes wait for a sync running in the background to end. Past
/// it they go ahead, and wait behind the sync, rather than pile up. Tests
/// wait less, to get past it.
const POSTPONE_LIMIT: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(2)
};

/// Milliseconds on a clock that only goes forward.
fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// The syncs running in the background, the tick's and a snapshot's. Some
/// disks hold up every write while one runs, and a write held up under the
/// broker's lock holds up every request: unless every write is to be synced,
/// writes wait in memory for the syncs to end instead, for up to
/// [`POSTPONE_LIMIT`], and replies go out meanwhile.
#[derive(Clone, Default)]
pub(crate) struct Syncs(Arc<(AtomicUsize, AtomicU64)>);

impl Syncs {
    /// Runs `sync`, counted as running meanwhile.
    pub(crate) fn run<R>(&self, sync: impl FnOnce() -> R) -> R {
        let (running, since) = &*self.0;
        if running.fetch_add(1, Ordering::AcqRel) == 0 {
            since.store(monotonic_ms(), Ordering::Release);
        }
        let result = sync();
        running.fetch_sub(1, Ordering::AcqRel);
        result
    }

    /// Whether writes are to wait: a sync has been running for less than
    /// the limit.
    fn holding_up(&self) -> bool {
        let (running, since) = &*self.0;
        running.load(Ordering::Acquire) > 0
            && monotonic_ms().saturating_sub(since.load(Ordering::Acquire))
                < POSTPONE_LIMIT.as_millis() as u64
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fsync {
    Always,
    EverySec,
    Never,
}

impl std::str::FromStr for Fsync {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "always" => Ok(Fsync::Always),
            "everysec" => Ok(Fsync::EverySec),
            "never" | "no" => Ok(Fsync::Never),
            _ => Err(format!(
                "invalid fsync policy {s:?} (always, everysec, never)"
            )),
        }
    }
}

/// What each line starts with: a CRC-32C of the rest of the line, in hex,
/// filled in once the rest is written.
const CHECKSUM: &[u8] = b"{\"c\":\"00000000\",";

/// Writes the line for `effect`, numbered `seq`, onto the end of `out`.
fn write_line(out: &mut Vec<u8>, seq: u64, effect: &impl Serialize) -> io::Result<()> {
    let start = out.len();
    out.extend_from_slice(CHECKSUM);
    write!(out, "\"s\":{seq},\"e\":")?;
    if let Err(e) = serde_json::to_writer(&mut *out, effect) {
        out.truncate(start);
        return Err(e.into());
    }
    out.push(b'}');
    let crc = crc32c(&out[start + CHECKSUM.len()..]);
    write_hex(&mut out[start + 6..start + 14], crc);
    out.push(b'\n');
    Ok(())
}

/// Writes a checksum over the eight digits that hold it.
fn write_hex(digits: &mut [u8], crc: u32) {
    for (i, digit) in digits.iter_mut().enumerate() {
        *digit = b"0123456789abcdef"[(crc >> (28 - 4 * i)) as usize & 15];
    }
}

/// The checksum eight hexadecimal digits hold.
fn read_hex(digits: &[u8]) -> Option<u32> {
    digits.iter().try_fold(0, |crc, &digit| {
        Some(crc << 4 | char::from(digit).to_digit(16)?)
    })
}

/// Whether a line is as it was written, matching the checksum it starts with.
fn intact(line: &[u8]) -> bool {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let Some(rest) = line.strip_prefix(&CHECKSUM[..6]) else {
        return false;
    };
    let written = rest.get(..8).and_then(read_hex);
    let body = rest.get(8..).and_then(|rest| rest.strip_prefix(b"\","));
    matches!((written, body), (Some(written), Some(body)) if written == crc32c(body))
}

/// What a snapshot ends with, closing the object it holds: a CRC-32C of the
/// rest of the file, in hex.
const SNAPSHOT_END: &[u8] = b",\"c\":\"00000000\"}";

/// Reads a snapshot, as long as it matches the checksum it ends with.
fn read_snapshot(raw: &[u8]) -> Result<SnapshotOwned, String> {
    let intact = raw.len().checked_sub(SNAPSHOT_END.len()).is_some_and(|at| {
        let (body, end) = raw.split_at(at);
        end.starts_with(&SNAPSHOT_END[..6])
            && end.ends_with(&SNAPSHOT_END[14..])
            && read_hex(&end[6..14]) == Some(crc32c(body))
    });
    if !intact {
        return Err("it does not match its checksum".into());
    }
    serde_json::from_slice(raw).map_err(|e| e.to_string())
}

/// CRC-32C, the checksum storage formats commonly check their records with.
fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_extend(0, bytes)
}

/// The CRC-32C of the bytes `crc` is the CRC-32C of, followed by `bytes`:
/// with the instructions most processors have for it, eight bytes a step,
/// or else a byte at a time from a table.
fn crc32c_extend(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: the processor has the instructions.
        return unsafe { crc32c_arm(crc, bytes) };
    }
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        // SAFETY: the processor has the instructions.
        return unsafe { crc32c_x86(crc, bytes) };
    }
    crc32c_table(crc, bytes)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
fn crc32c_arm(crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd};
    let (words, rest) = bytes.as_chunks();
    let mut crc = !crc;
    for &word in words {
        crc = __crc32cd(crc, u64::from_le_bytes(word));
    }
    for &byte in rest {
        crc = __crc32cb(crc, byte);
    }
    !crc
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
fn crc32c_x86(crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};
    let (words, rest) = bytes.as_chunks();
    let mut crc = u64::from(!crc);
    for &word in words {
        crc = _mm_crc32_u64(crc, u64::from_le_bytes(word));
    }
    let mut crc = crc as u32;
    for &byte in rest {
        crc = _mm_crc32_u8(crc, byte);
    }
    !crc
}

fn crc32c_table(crc: u32, bytes: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ 0x82f6_3b78
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };
    !bytes.iter().fold(!crc, |crc, &byte| {
        TABLE[((crc ^ u32::from(byte)) & 0xff) as usize] ^ (crc >> 8)
    })
}

/// [`Effect::Db`] as it is journaled, borrowing the commands.
#[derive(Serialize)]
#[serde(tag = "op", rename = "db")]
struct CommandsRef<'a> {
    at: u64,
    commands: &'a [&'a [String]],
}

#[derive(Deserialize)]
struct LineOwned {
    s: u64,
    e: Effect,
}

#[derive(Serialize)]
struct SnapshotFileRef<'a> {
    version: u32,
    seq: u64,
    saved_at: u64,
    #[serde(flatten)]
    state: SnapshotRef<'a>,
}

pub struct Journal {
    dir: PathBuf,
    seq: u64,
    /// Bytes of lines handed to the tail since the journal was started.
    journaled: u64,
    snapshot_bytes: u64,
    fsync: Fsync,
    /// Journals set aside holding writes the `everysec` tick has yet to sync.
    retired: Vec<File>,
    /// Lines for changes already made in memory, not handed to the tail
    /// yet: they go with the next write.
    pending: Vec<u8>,
    /// Why the tick's last sync failed, until one succeeds again.
    unsynced: Option<String>,
    /// A sync failed: what it was to sync may not be on the disk even once
    /// one succeeds, so a snapshot writes the state again.
    resnapshot: bool,
    /// Makes the tick's syncs fail, as on a failing disk.
    #[cfg(test)]
    fail_syncs: bool,
    /// Set while a snapshot is being taken or written in the background.
    writing: Arc<AtomicBool>,
    syncs: Syncs,
    writer: Writer,
    _lock: File,
}

/// The end of the journal, where lines are appended. Its lock is apart from
/// the broker's: a thread takes the lines waiting under the broker's lock,
/// locks the tail before releasing it, and writes them after, so requests go
/// on while the disk takes them, and no lines taken later go before them.
struct Tail {
    file: File,
    dir: PathBuf,
    fsync: Fsync,
    /// Bytes of whole lines in the file.
    bytes: u64,
    dirty: bool,
    /// A fresh journal was started: the directory holding its name must
    /// reach the disk before what is written to it can be counted on.
    dir_unsynced: bool,
    /// Lines for changes already made in memory that the disk refused. They
    /// are written, in order, before anything else.
    refused: Vec<u8>,
    /// A write was cut short and could not be cut off again: nothing more
    /// may be appended, or the torn line would end up mid-journal.
    torn: bool,
    /// Why the last write failed, until one succeeds again.
    failure: Option<String>,
    /// Makes writes stop after this many bytes, as on a full disk.
    #[cfg(test)]
    fail_after: Option<usize>,
    /// Makes each write take this much longer, as on a slow disk.
    #[cfg(test)]
    stall: Option<Duration>,
}

/// The journal's tail, for the threads that write the lines they take.
#[derive(Clone)]
pub struct Writer(Arc<Shared>);

struct Shared {
    tail: Mutex<Tail>,
    /// Whether changes are refused, refused lines waiting or a write torn,
    /// and whether refused lines wait, readable without the tail's lock.
    failing: AtomicBool,
    refused: AtomicBool,
    /// Holders of the tail: while there is one, lines already taken may be
    /// on their way to the disk.
    busy: AtomicUsize,
}

/// The tail, locked.
pub struct TailGuard<'a> {
    tail: MutexGuard<'a, Tail>,
    shared: &'a Shared,
}

/// A snapshot on its way to disk. Its parts, serialized under the broker's
/// lock, go to a temporary file as they come, without the lock, so only a few
/// of them are in memory at once; the file replaces `snapshot.json` once the
/// snapshot is complete.
#[must_use = "a snapshot does nothing until it is written"]
pub struct Snapshot {
    dir: PathBuf,
    /// The keyspace capture that feeds it.
    pub capture: u64,
    seq: u64,
    parts: Vec<Vec<u8>>,
    bytes: usize,
    file: Option<File>,
    /// The CRC-32C of what has gone to the file.
    crc: u32,
    writing: Arc<AtomicBool>,
    syncs: Syncs,
}

pub fn lock_path(dir: &Path) -> PathBuf {
    dir.join("postino.lock")
}

/// A journal set aside by a rewrite, named after its last sequence number.
fn rotated_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("journal-{seq:020}.log"))
}

/// Set-aside journals, oldest first, with the last sequence number each holds.
fn rotated_journals(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let seq = path.file_name().and_then(|n| n.to_str()).and_then(|n| {
            n.strip_prefix("journal-")?
                .strip_suffix(".log")?
                .parse()
                .ok()
        });
        if let Some(seq) = seq {
            found.push((seq, path));
        }
    }
    found.sort();
    Ok(found)
}

impl Snapshot {
    /// Its size in bytes, once written.
    pub fn len(&self) -> usize {
        self.bytes + SNAPSHOT_END.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    pub fn add(&mut self, parts: Vec<Vec<u8>>) {
        self.bytes += parts.iter().map(Vec::len).sum::<usize>();
        self.parts.extend(parts);
    }

    /// Its own temporary file: a snapshot given up may still be writing to
    /// its file while the next one, at the same sequence number, starts.
    fn tmp(&self) -> PathBuf {
        self.dir
            .join(format!("snapshot.{}.{}.tmp", self.seq, self.capture))
    }

    /// Writes the parts added so far to the temporary file.
    pub fn flush(&mut self) -> io::Result<()> {
        let file = match &mut self.file {
            Some(file) => file,
            None => self.file.insert(
                private_file()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(self.tmp())?,
            ),
        };
        for part in self.parts.drain(..) {
            file.write_all(&part)?;
            self.crc = crc32c_extend(self.crc, &part);
        }
        Ok(())
    }

    /// Makes the snapshot durable, then deletes the journals it covers. Runs
    /// on a background thread; on failure the journals stay, so nothing is lost.
    pub fn write(mut self) {
        if let Err(e) = self.persist() {
            eprintln!("postino: snapshot write failed: {e}; the journal keeps every change");
            let _ = fs::remove_file(self.tmp());
        }
        self.writing.store(false, Ordering::Release);
    }

    /// Gives the snapshot up, removing what it wrote. The journals keep
    /// every change.
    pub fn abandon(mut self) {
        self.file = None;
        let _ = fs::remove_file(self.tmp());
        self.writing.store(false, Ordering::Release);
    }

    fn persist(&mut self) -> io::Result<()> {
        let covered: Vec<(u64, PathBuf)> = rotated_journals(&self.dir)?
            .into_iter()
            .filter(|(seq, _)| *seq <= self.seq)
            .collect();
        // The set-aside journals hold these changes until the snapshot does.
        for (_, path) in &covered {
            let file = File::open(path)?;
            self.syncs.run(|| file.sync_data())?;
        }
        self.flush()?;
        if let Some(mut file) = self.file.take() {
            let mut end = SNAPSHOT_END.to_vec();
            write_hex(&mut end[6..14], self.crc);
            file.write_all(&end)?;
            self.syncs.run(|| file.sync_all())?;
        }
        fs::rename(self.tmp(), self.dir.join("snapshot.json"))?;
        self.syncs.run(|| sync_dir(&self.dir));
        for (_, path) in covered {
            fs::remove_file(path)?;
        }
        self.syncs.run(|| sync_dir(&self.dir));
        Ok(())
    }
}

/// The once-a-second sync, taken under the broker's lock and run after it.
pub struct Sync {
    retired: Vec<File>,
    dir: Option<PathBuf>,
    file: Option<File>,
    syncs: Syncs,
    #[cfg(test)]
    fail: bool,
}

impl Sync {
    /// Syncs in the order the writes were made, so that what is on the disk
    /// when one fails never has a hole in it. What a failure leaves unsynced
    /// stays here, for [`Journal::synced`] to take back.
    pub fn run(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if self.fail {
            return Err(io::Error::other("input/output error"));
        }
        while let Some(file) = self.retired.first() {
            self.syncs.run(|| file.sync_data())?;
            self.retired.remove(0);
        }
        if let Some(dir) = self.dir.take() {
            self.syncs.run(|| sync_dir(&dir));
        }
        if let Some(file) = &self.file {
            self.syncs.run(|| file.sync_data())?;
            self.file = None;
        }
        Ok(())
    }
}

fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(handle) = File::open(dir) {
        let _ = handle.sync_all();
    }
}

impl Tail {
    /// Appends whole lines, or nothing: a write cut short, by a full disk
    /// say, is cut off again, so no torn line ends up mid-journal.
    fn append(&mut self, lines: &[u8]) -> io::Result<()> {
        if self.torn {
            return Err(io::Error::other(
                "an earlier write was cut short; restart the server to recover",
            ));
        }
        if let Err(e) = self.attempt(lines) {
            if self.file.set_len(self.bytes).is_err() {
                self.torn = true;
            }
            if self.failure.is_none() {
                eprintln!(
                    "postino: cannot write the journal: {e}; changes are refused until it can"
                );
            }
            self.failure = Some(e.to_string());
            return Err(e);
        }
        if self.failure.take().is_some() {
            eprintln!("postino: the journal is written again");
        }
        self.bytes += lines.len() as u64;
        self.dirty |= self.fsync != Fsync::Always;
        Ok(())
    }

    fn attempt(&mut self, lines: &[u8]) -> io::Result<()> {
        #[cfg(test)]
        if let Some(stall) = self.stall {
            std::thread::sleep(stall);
        }
        #[cfg(test)]
        if let Some(n) = self.fail_after {
            self.file.write_all(&lines[..n.min(lines.len())])?;
            return Err(io::Error::other("no space left on device"));
        }
        self.file.write_all(lines)?;
        if self.fsync == Fsync::Always {
            if std::mem::take(&mut self.dir_unsynced) {
                sync_dir(&self.dir);
            }
            self.file.sync_data()?;
        }
        Ok(())
    }
}

impl Writer {
    /// Locks the tail. Lines taken under the broker's lock while it is held
    /// can be written once that lock is released: none taken later are
    /// written before them.
    pub fn lock(&self) -> TailGuard<'_> {
        let tail = self.0.tail.lock().unwrap_or_else(|p| p.into_inner());
        self.0.busy.fetch_add(1, Ordering::AcqRel);
        TailGuard {
            tail,
            shared: &self.0,
        }
    }

    /// Waits for the lines other threads have taken to be written.
    pub fn wait(&self) {
        if self.0.busy.load(Ordering::Acquire) > 0 {
            drop(self.0.tail.lock());
        }
    }

    fn failing(&self) -> bool {
        self.0.failing.load(Ordering::Acquire)
    }

    fn refused(&self) -> bool {
        self.0.refused.load(Ordering::Acquire)
    }

    fn busy(&self) -> bool {
        self.0.busy.load(Ordering::Acquire) > 0
    }
}

impl TailGuard<'_> {
    /// Writes `lines` after any the disk refused before; when it refuses
    /// them too, they wait with those.
    pub fn write(&mut self, lines: Vec<u8>) -> io::Result<()> {
        let tail = &mut *self.tail;
        let lines = if tail.refused.is_empty() {
            lines
        } else {
            let mut all = std::mem::take(&mut tail.refused);
            all.extend_from_slice(&lines);
            all
        };
        let written = if lines.is_empty() {
            Ok(())
        } else {
            tail.append(&lines)
        };
        if written.is_err() {
            tail.refused = lines;
        }
        self.note();
        written
    }

    /// Writes the line for a change not made yet, after any refused before:
    /// when the disk refuses it, it is dropped.
    fn write_new(&mut self, line: &[u8]) -> io::Result<()> {
        let written = self.write(Vec::new()).and_then(|()| self.tail.append(line));
        self.note();
        written
    }

    /// Notes how writes go, for the checks made without the tail's lock.
    /// A line the disk refused for a change not made leaves nothing to wait
    /// for: the next change tries the disk, rather than be refused until
    /// some other kind of change gets through.
    fn note(&self) {
        let tail = &*self.tail;
        let failing = tail.torn || !tail.refused.is_empty();
        self.shared.failing.store(failing, Ordering::Release);
        self.shared
            .refused
            .store(!tail.refused.is_empty(), Ordering::Release);
    }
}

impl Drop for TailGuard<'_> {
    fn drop(&mut self) {
        self.shared.busy.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Journal {
    pub fn open(dir: &Path, fsync: Fsync) -> Result<(Journal, Store), String> {
        private_dir(dir).map_err(|e| format!("cannot create data dir {}: {e}", dir.display()))?;
        let lock = lock(dir)?;

        let snapshot_path = dir.join("snapshot.json");
        let (mut store, snapshot_seq, snapshot_bytes) = match fs::read(&snapshot_path) {
            Ok(raw) => {
                let snapshot = read_snapshot(&raw)
                    .map_err(|e| format!("corrupt snapshot {}: {e}", snapshot_path.display()))?;
                if snapshot.version != SNAPSHOT_VERSION {
                    return Err(format!(
                        "unsupported snapshot version {} in {}",
                        snapshot.version,
                        snapshot_path.display()
                    ));
                }
                let seq = snapshot.seq;
                (Store::from_snapshot(snapshot), seq, raw.len() as u64)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => (Store::default(), 0, 0),
            Err(e) => return Err(format!("cannot read snapshot: {e}")),
        };

        // Journals a finished rewrite did not get to delete hold only lines the
        // snapshot covers; the others hold changes made since.
        let mut seq = snapshot_seq;
        for (last, path) in
            rotated_journals(dir).map_err(|e| format!("cannot list {}: {e}", dir.display()))?
        {
            if last <= snapshot_seq {
                let _ = fs::remove_file(&path);
                continue;
            }
            let mut file =
                File::open(&path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
            let (replayed, _) = replay(&mut file, &mut store, snapshot_seq, &path)?;
            seq = seq.max(replayed);
        }
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if (name.starts_with("snapshot.") && name.ends_with(".tmp"))
                    || name == "journal.next"
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }

        let journal_path = dir.join("journal.log");
        let mut file = private_file()
            .read(true)
            .append(true)
            .create(true)
            .open(&journal_path)
            .map_err(|e| format!("cannot open journal: {e}"))?;
        let (replayed, valid_bytes) = replay(&mut file, &mut store, snapshot_seq, &journal_path)?;
        let seq = seq.max(replayed);
        let length = file.metadata().map(|m| m.len()).unwrap_or(valid_bytes);
        if valid_bytes < length {
            eprintln!(
                "postino: truncating torn journal tail ({} bytes) in {}",
                length - valid_bytes,
                journal_path.display()
            );
            file.set_len(valid_bytes)
                .map_err(|e| format!("cannot truncate journal: {e}"))?;
        }
        file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;

        Ok((
            Journal {
                dir: dir.to_path_buf(),
                seq: seq.max(snapshot_seq),
                journaled: valid_bytes,
                snapshot_bytes,
                fsync,
                retired: Vec::new(),
                pending: Vec::new(),
                unsynced: None,
                resnapshot: false,
                #[cfg(test)]
                fail_syncs: false,
                writing: Arc::new(AtomicBool::new(false)),
                syncs: Syncs::default(),
                writer: Writer(Arc::new(Shared {
                    tail: Mutex::new(Tail {
                        file,
                        dir: dir.to_path_buf(),
                        fsync,
                        bytes: valid_bytes,
                        dirty: false,
                        dir_unsynced: false,
                        refused: Vec::new(),
                        torn: false,
                        failure: None,
                        #[cfg(test)]
                        fail_after: None,
                        #[cfg(test)]
                        stall: None,
                    }),
                    failing: AtomicBool::new(false),
                    refused: AtomicBool::new(false),
                    busy: AtomicUsize::new(0),
                })),
                _lock: lock,
            },
            store,
        ))
    }

    fn line(&self, effect: &Effect) -> io::Result<Vec<u8>> {
        let mut line = Vec::new();
        write_line(&mut line, self.seq + 1, effect)?;
        Ok(line)
    }

    /// Makes writes stop after `n` bytes, as on a full disk, or work again.
    #[cfg(test)]
    pub fn fail_writes_after(&mut self, n: Option<usize>) {
        self.writer.lock().tail.fail_after = n;
    }

    /// Makes each write take `stall` longer, as on a slow disk, or not.
    #[cfg(test)]
    pub fn stall_writes(&mut self, stall: Option<Duration>) {
        self.writer.lock().tail.stall = stall;
    }

    /// Makes the tick's syncs fail, as on a failing disk, or work again.
    #[cfg(test)]
    pub fn fail_syncs(&mut self, fail: bool) {
        self.fail_syncs = fail;
    }

    /// The syncs running in the background, to run one alongside.
    #[cfg(test)]
    pub(crate) fn syncs(&self) -> Syncs {
        self.syncs.clone()
    }

    /// The journal's tail, for writing what [`Journal::take_pending`] hands
    /// out.
    pub fn writer(&self) -> Writer {
        self.writer.clone()
    }

    /// Journals an effect before it is applied. On error nothing was
    /// written, and the caller must not apply it.
    pub fn append(&mut self, effect: &Effect) -> io::Result<()> {
        if let Some(why) = &self.unsynced {
            return Err(io::Error::other(why.clone()));
        }
        let line = self.line(effect)?;
        let pending = std::mem::take(&mut self.pending);
        self.journaled += pending.len() as u64;
        let mut tail = self.writer.lock();
        tail.write(pending)?;
        tail.write_new(&line)?;
        drop(tail);
        self.journaled += line.len() as u64;
        self.seq += 1;
        Ok(())
    }

    /// Journals an effect already applied in memory without writing it yet:
    /// it goes out with the next write, or [flush](Journal::flush_pending),
    /// so that the changes behind many replies take one write.
    pub fn defer_applied(&mut self, effect: &Effect) -> io::Result<()> {
        self.push(effect)
    }

    /// [`Journal::defer_applied`] for keyspace commands run at `at`.
    pub fn defer_commands(&mut self, at: u64, commands: &[&[String]]) -> io::Result<()> {
        self.push(&CommandsRef { at, commands })
    }

    /// Adds the line for `effect` to the changes waiting to be written.
    fn push(&mut self, effect: &impl Serialize) -> io::Result<()> {
        write_line(&mut self.pending, self.seq + 1, effect)?;
        self.seq += 1;
        Ok(())
    }

    /// Whether changes may wait to be written together. Not when each one is
    /// to be synced to disk before its reply.
    pub fn defers(&self) -> bool {
        self.fsync != Fsync::Always && !self.refusing()
    }

    /// Journals an effect already applied in memory. When the disk refuses
    /// it, it waits and is written before anything else; until then the
    /// journal is [failing](Journal::failing).
    pub fn append_applied(&mut self, effect: &Effect) -> io::Result<()> {
        self.push(effect)?;
        self.flush_pending()
    }

    /// [`Journal::append_applied`] for keyspace commands run at `at`.
    pub fn append_commands(&mut self, at: u64, commands: &[&[String]]) -> io::Result<()> {
        self.push(&CommandsRef { at, commands })?;
        self.flush_pending()
    }

    /// Writes the changes waiting to be written: deferred ones, and ones the
    /// disk refused before, if it takes them now. Lines another thread took
    /// to write are written first.
    pub fn flush_pending(&mut self) -> io::Result<()> {
        if self.pending.is_empty() && !self.writer.refused() && !self.writer.busy() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.journaled += pending.len() as u64;
        self.writer.lock().write(pending)
    }

    /// Takes the changes waiting to be written, for the caller to write once
    /// it has released the broker's lock, through the tail it locked before.
    /// None when there are none, or when they are written under the lock:
    /// with every write synced, while a sync holds writes up, or while the
    /// disk refuses them.
    pub fn take_pending(&mut self) -> Option<Vec<u8>> {
        if self.pending.is_empty()
            || self.fsync == Fsync::Always
            || self.postponing()
            || self.writer.failing()
        {
            return None;
        }
        let pending = std::mem::take(&mut self.pending);
        self.journaled += pending.len() as u64;
        Some(pending)
    }

    /// Writes the changes waiting to be written, unless a sync running in the
    /// background would hold the write up: then they wait for it to end.
    pub fn flush_soon(&mut self) -> io::Result<()> {
        if self.postponing() {
            return Ok(());
        }
        self.flush_pending()
    }

    /// Whether writes wait for a sync to end, when not every write is synced.
    fn postponing(&self) -> bool {
        self.fsync != Fsync::Always && self.syncs.holding_up()
    }

    /// Whether changes wait to be written.
    pub fn waiting(&self) -> bool {
        !self.pending.is_empty() || self.writer.refused()
    }

    /// Why changes cannot be journaled now, if they cannot: some made in
    /// memory are still waiting for the disk, or the disk did not sync the
    /// ones it took.
    pub fn failing(&self) -> Option<String> {
        if !self.writer.failing() {
            return self.unsynced.clone();
        }
        let tail = self.writer.lock();
        let tail = &*tail.tail;
        tail.failure
            .clone()
            .or_else(|| self.unsynced.clone())
            .or_else(|| tail.torn.then(|| "the disk refused a write".into()))
    }

    /// Whether changes are refused: [`Journal::failing`], without why.
    fn refusing(&self) -> bool {
        self.writer.failing() || self.unsynced.is_some()
    }

    /// What the `everysec` tick has to sync: journals set aside since the
    /// last tick, with the directory that renamed them, and the journal, when
    /// writes wait for it. The caller syncs after releasing the broker's lock.
    pub fn take_sync(&mut self) -> Option<Sync> {
        if self.fsync == Fsync::Never {
            return None;
        }
        let mut guard = self.writer.lock();
        let tail = &mut *guard.tail;
        // A handle it cannot have now, with every descriptor in use say, the
        // next tick asks for again.
        let file = tail.dirty.then(|| tail.file.try_clone().ok()).flatten();
        tail.dirty &= file.is_none();
        let dir = std::mem::take(&mut tail.dir_unsynced).then(|| tail.dir.clone());
        drop(guard);
        let sync = Sync {
            retired: std::mem::take(&mut self.retired),
            dir,
            file,
            syncs: self.syncs.clone(),
            #[cfg(test)]
            fail: self.fail_syncs,
        };
        (!sync.retired.is_empty() || sync.dir.is_some() || sync.file.is_some()).then_some(sync)
    }

    /// Takes back the sync the tick ran. When it failed, what it left is
    /// synced at the next tick, and changes are refused until a sync
    /// succeeds: the disk may not hold the ones already made.
    pub fn synced(&mut self, mut sync: Sync, result: io::Result<()>) {
        sync.retired.append(&mut self.retired);
        self.retired = sync.retired;
        let mut guard = self.writer.lock();
        guard.tail.dir_unsynced |= sync.dir.is_some();
        guard.tail.dirty |= sync.file.is_some();
        drop(guard);
        match result {
            Err(e) => {
                if self.unsynced.is_none() {
                    eprintln!(
                        "postino: cannot sync the journal: {e}; changes are refused until it can"
                    );
                }
                self.unsynced = Some(format!("cannot sync the journal: {e}"));
                self.resnapshot = true;
            }
            Ok(()) => {
                if self.unsynced.take().is_some() {
                    eprintln!("postino: the journal is synced again");
                }
            }
        }
    }

    /// Whether to start a rewrite: not while a sync runs, as setting the
    /// journal aside writes what waits.
    pub fn needs_rewrite(&self) -> bool {
        !self.writing.load(Ordering::Acquire)
            && !self.refusing()
            && !self.syncs.holding_up()
            && (self.resnapshot || self.journaled > REWRITE_MIN_BYTES.max(self.snapshot_bytes * 2))
    }

    /// Serializes the state and starts a fresh journal. Writing the returned
    /// snapshot, which can happen on another thread, completes the rewrite.
    pub fn begin_rewrite(&mut self, store: &mut Store) -> io::Result<Snapshot> {
        let mut snapshot = self.start_rewrite(store)?;
        if let Err(e) = self.finish_rewrite(store, &mut snapshot) {
            snapshot.abandon();
            return Err(e);
        }
        Ok(snapshot)
    }

    /// Starts a fresh journal and a snapshot of the state as it is now: the
    /// forum is serialized at once, the keyspace by [`crate::db::Db`] as
    /// requests go on, its parts handed to the returned snapshot.
    /// [`Journal::finish_rewrite`] ends it.
    pub fn start_rewrite(&mut self, store: &mut Store) -> io::Result<Snapshot> {
        // Every change up to the snapshot's number goes in the journal set
        // aside, which replay reads if the snapshot never lands: what waits,
        // and what other threads took to write, is written first, and nothing
        // more until the fresh journal takes over.
        let writer = self.writer.clone();
        let mut tail = writer.lock();
        let pending = std::mem::take(&mut self.pending);
        self.journaled += pending.len() as u64;
        tail.write(pending)?;
        let mut head = serde_json::to_vec(&SnapshotFileRef {
            version: SNAPSHOT_VERSION,
            seq: self.seq,
            saved_at: now_ms(),
            state: store.head(),
        })?;
        // The rooms, the messages, and the keys follow, inside the same object.
        head.pop();
        // The fresh journal exists before the current one is set aside, so a
        // failure at any step leaves every write in a journal replay will read.
        let (active, next) = (self.dir.join("journal.log"), self.dir.join("journal.next"));
        let rotated = rotated_path(&self.dir, self.seq);
        // A journal already set aside under this number holds every change up
        // to it, and nothing has been journaled since, the number being the
        // same: the current journal is replaced rather than renamed over it.
        let set_aside = rotated.try_exists()?;
        let _ = fs::remove_file(&next);
        let fresh = private_file()
            .read(true)
            .append(true)
            .create_new(true)
            .open(&next)?;
        if !set_aside && let Err(e) = fs::rename(&active, &rotated) {
            let _ = fs::remove_file(&next);
            return Err(e);
        }
        if let Err(e) = fs::rename(&next, &active) {
            if !set_aside {
                let _ = fs::rename(&rotated, &active);
            }
            let _ = fs::remove_file(&next);
            return Err(e);
        }
        // Writes go to the fresh journal from here on. Replay drops a
        // journal.next it finds, so the renames must reach the disk before
        // what the fresh journal holds is counted on: the next sync syncs the
        // directory first, the tick's or, with every write synced, the next
        // write's, and no request waits on a slow disk for it here.
        tail.tail.dir_unsynced = true;
        let previous = std::mem::replace(&mut tail.tail.file, fresh);
        // Its last writes are synced at the next tick, as they would have
        // been, rather than once the snapshot is written, which can be long
        // after the ones the fresh journal holds.
        if std::mem::take(&mut tail.tail.dirty) && self.fsync == Fsync::EverySec {
            self.retired.push(previous);
        }
        tail.tail.bytes = 0;
        // A torn line, if any, ends the journal just set aside, where replay
        // expects one.
        tail.tail.torn = false;
        tail.note();
        drop(tail);
        self.journaled = 0;
        self.resnapshot = false;
        self.writing.store(true, Ordering::Release);
        let capture = store.begin_capture(head);
        Ok(Snapshot {
            dir: self.dir.clone(),
            capture,
            seq: self.seq,
            parts: Vec::new(),
            bytes: 0,
            file: None,
            crc: 0,
            writing: Arc::clone(&self.writing),
            syncs: self.syncs.clone(),
        })
    }

    /// Ends the snapshot [`Journal::start_rewrite`] began, serializing the
    /// keys it has not reached. Writing the snapshot completes the rewrite.
    pub fn finish_rewrite(&mut self, store: &mut Store, snapshot: &mut Snapshot) -> io::Result<()> {
        let parts = store.end_capture().map_err(io::Error::other)?;
        snapshot.add(parts);
        self.snapshot_bytes = snapshot.len() as u64;
        Ok(())
    }

    /// Waits for a background snapshot to finish, so two never race. Returns
    /// whether it did, within a minute.
    pub fn wait_for_snapshot(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        while self.writing.load(Ordering::Acquire) {
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    }

    /// A rewrite done in place, for shutdown. A snapshot still being taken is
    /// dropped: this one covers everything it would have.
    pub fn rewrite(&mut self, store: &mut Store) -> io::Result<()> {
        // What waits to be written is, whatever comes of the snapshot.
        self.flush_pending()?;
        if store.capturing() {
            store.abandon_capture();
            self.writing.store(false, Ordering::Release);
        }
        // Written alongside it, the older snapshot could land last, after this
        // one deleted the journals that hold what came between them.
        if !self.wait_for_snapshot() {
            return Err(io::Error::other(
                "a snapshot is still being written; the journal keeps every change",
            ));
        }
        let mut snapshot = self.begin_rewrite(store)?;
        let written = snapshot.persist();
        self.writing.store(false, Ordering::Release);
        written
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Holds the data directory for one process at a time.
fn lock(dir: &Path) -> Result<File, String> {
    let lock = private_file()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(dir))
        .map_err(|e| format!("cannot open lock file: {e}"))?;
    lock.try_lock().map_err(|_| {
        format!(
            "another postino broker is already using {} (lock held on postino.lock)",
            dir.display()
        )
    })?;
    Ok(lock)
}

/// What reading a journal found: the last sequence number, how many bytes
/// of whole lines make sense, and the number of the first line that does
/// not, unless it is the last and only torn, by a crash mid-write.
struct Scan {
    seq: u64,
    valid: u64,
    lines: usize,
    corrupt: Option<usize>,
}

fn scan(
    file: &mut File,
    snapshot_seq: u64,
    mut apply: impl FnMut(&LineOwned),
) -> Result<Scan, String> {
    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(&*file);
    let mut seq = snapshot_seq;
    let mut offset = 0u64;
    let mut line = Vec::new();
    let mut lines = 0usize;
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| format!("cannot read journal: {e}"))?;
        if read == 0 {
            return Ok(Scan {
                seq,
                valid: offset,
                lines,
                corrupt: None,
            });
        }
        let complete = line.ends_with(b"\n");
        match serde_json::from_slice::<LineOwned>(&line) {
            Ok(entry) if complete && intact(&line) => {
                if entry.s > snapshot_seq {
                    apply(&entry);
                }
                seq = seq.max(entry.s);
                offset += read as u64;
                lines += 1;
            }
            _ => {
                let mut rest = Vec::new();
                let _ = io::Read::read_to_end(&mut reader, &mut rest);
                let torn = rest.iter().all(u8::is_ascii_whitespace);
                return Ok(Scan {
                    seq,
                    valid: offset,
                    lines,
                    corrupt: (!torn).then_some(lines + 1),
                });
            }
        }
    }
}

fn replay(
    file: &mut File,
    store: &mut Store,
    snapshot_seq: u64,
    path: &Path,
) -> Result<(u64, u64), String> {
    let scan = scan(file, snapshot_seq, |entry| store.apply(&entry.e))?;
    if let Some(line) = scan.corrupt {
        return Err(format!(
            "corrupt journal entry at {}:{line}; refusing to start. Run `postino repair` \
             to start from the changes before it, or move the file aside to recover from \
             the last snapshot.",
            path.display()
        ));
    }
    Ok((scan.seq, scan.valid))
}

/// Cuts the journal at its first corrupt entry, so that the server starts
/// from the changes made before it. What follows is set aside next to the
/// journal rather than deleted: the rest of that file, and every journal
/// after it, whose changes were made on top of the ones lost. Returns what
/// it did, or None when there was nothing to cut.
pub fn repair(dir: &Path) -> Result<Option<String>, String> {
    if !dir.is_dir() {
        return Err(format!("there is no data directory at {}", dir.display()));
    }
    let _lock = lock(dir)?;
    let snapshot_path = dir.join("snapshot.json");
    let snapshot_seq = match fs::read(&snapshot_path) {
        Ok(raw) => {
            read_snapshot(&raw)
                .map_err(|e| {
                    format!(
                        "corrupt snapshot {}: {e}; restore it from a backup",
                        snapshot_path.display()
                    )
                })?
                .seq
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => return Err(format!("cannot read snapshot: {e}")),
    };
    let mut journals: Vec<PathBuf> = rotated_journals(dir)
        .map_err(|e| format!("cannot list {}: {e}", dir.display()))?
        .into_iter()
        .filter(|(last, _)| *last > snapshot_seq)
        .map(|(_, path)| path)
        .collect();
    journals.push(dir.join("journal.log"));
    for (i, path) in journals.iter().enumerate() {
        let mut file = match File::options().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("cannot open {}: {e}", path.display())),
        };
        let found = scan(&mut file, snapshot_seq, |_| {})?;
        let Some(line) = found.corrupt else {
            continue;
        };
        let cut = |e: io::Error| format!("cannot set aside the rest of {}: {e}", path.display());
        let mut rest = Vec::new();
        file.seek(SeekFrom::Start(found.valid)).map_err(cut)?;
        io::Read::read_to_end(&mut file, &mut rest).map_err(cut)?;
        let rest_at = aside(path);
        let mut kept = private_file()
            .write(true)
            .create_new(true)
            .open(&rest_at)
            .map_err(cut)?;
        kept.write_all(&rest)
            .and_then(|()| kept.sync_all())
            .map_err(cut)?;
        file.set_len(found.valid)
            .and_then(|()| file.sync_all())
            .map_err(cut)?;
        let mut report = format!(
            "{}: kept the {} lines before line {line}, and set aside the rest in {}",
            path.display(),
            found.lines,
            name(&rest_at)
        );
        for later in &journals[i + 1..] {
            let moved = aside(later);
            match fs::rename(later, &moved) {
                Ok(()) => report += &format!(", and {} as {}", name(later), name(&moved)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("cannot set aside {}: {e}", later.display())),
            }
        }
        sync_dir(dir);
        return Ok(Some(report));
    }
    Ok(None)
}

/// Where a journal, or what was cut from one, is set aside: next to it,
/// under a name no file has yet.
fn aside(path: &Path) -> PathBuf {
    let mut aside = path.with_extension("log.corrupt");
    let mut n = 1;
    while aside.exists() {
        aside = path.with_extension(format!("log.corrupt.{n}"));
        n += 1;
    }
    aside
}

fn name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::id::Id;
    use crate::store::{Message, State};
    use std::collections::{BTreeMap, BTreeSet};

    pub fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "postino-test-{name}-{}-{}",
            std::process::id(),
            crate::util::random_hex(4)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn message(ms: u64) -> Effect {
        Effect::Message {
            message: Message {
                id: Id { ms, seq: 0 },
                from: "a".into(),
                to: "b".into(),
                body: format!("body {ms}"),
                created: ms,
                expires: ms + 1000,
                thread: Id { ms, seq: 0 },
                reply_to: None,
                key: None,
                state: State::Queued,
                deliveries: 0,
                lease: None,
                settled: None,
            },
        }
    }

    #[test]
    fn keyspace_commands_are_journaled_as_their_effect() {
        let commands = [
            vec!["SET".to_string(), "k".into(), "a \"line\"\n".into()],
            vec!["SET".to_string(), "j".into(), "v".into()],
        ];
        let borrowed: Vec<&[String]> = commands.iter().map(Vec::as_slice).collect();
        let effect = Effect::Db {
            at: 7,
            commands: commands.to_vec(),
        };
        assert_eq!(
            serde_json::to_string(&CommandsRef {
                at: 7,
                commands: &borrowed
            })
            .unwrap(),
            serde_json::to_string(&effect).unwrap()
        );
        let dir = temp_dir("commands");
        {
            let (mut journal, _) = Journal::open(&dir, Fsync::Never).unwrap();
            journal.defer_commands(7, &borrowed[..1]).unwrap();
            journal.append_commands(8, &borrowed[1..]).unwrap();
        }
        let (_, store) = Journal::open(&dir, Fsync::Never).unwrap();
        let value = |key| {
            store
                .db
                .get(key, 9)
                .map(|e| serde_json::to_value(e).unwrap())
        };
        assert_eq!(value("k").unwrap()["data"], "a \"line\"\n");
        assert_eq!(value("j").unwrap()["data"], "v");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn replays_journal_after_restart() {
        let dir = temp_dir("replay");
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Always).unwrap();
            for ms in [1, 2, 3] {
                let effect = message(ms);
                journal.append(&effect).unwrap();
                store.apply(&effect);
            }
        }
        let (_, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 3);
        fs::remove_dir_all(dir).unwrap();
    }

    /// Writes `count` messages to a journal in `dir`, starting at `first`.
    fn journal_messages(dir: &Path, first: u64, count: u64) {
        let (mut journal, mut store) = Journal::open(dir, Fsync::Always).unwrap();
        for ms in first..first + count {
            let effect = message(ms);
            journal.append(&effect).unwrap();
            store.apply(&effect);
        }
    }

    /// Puts garbage in place of the given line of a journal.
    fn damage(path: &Path, line: usize) {
        let text = fs::read_to_string(path).unwrap();
        let mut lines: Vec<&str> = text.lines().collect();
        lines[line - 1] = "{\"s\":garbage";
        fs::write(path, lines.join("\n") + "\n").unwrap();
    }

    #[test]
    fn a_repair_keeps_the_changes_before_the_first_damaged_line() {
        let dir = temp_dir("repair");
        journal_messages(&dir, 1, 100);
        assert_eq!(repair(&dir).unwrap(), None, "a whole journal is left alone");
        damage(&dir.join("journal.log"), 51);
        let refused = Journal::open(&dir, Fsync::Always).err().unwrap();
        assert!(refused.contains("journal.log:51") && refused.contains("postino repair"));

        let report = repair(&dir).unwrap().unwrap();
        assert!(
            report.contains("kept the 50 lines before line 51"),
            "{report}"
        );
        let (_, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 50);
        let aside = fs::read_to_string(dir.join("journal.log.corrupt")).unwrap();
        assert_eq!(
            aside.lines().count(),
            50,
            "the rest is kept, damage and all"
        );
        assert_eq!(repair(&dir).unwrap(), None);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_repair_sets_aside_the_journals_after_a_damaged_one() {
        let dir = temp_dir("repair-rotated");
        journal_messages(&dir, 1, 100);
        fs::rename(dir.join("journal.log"), dir.join("journal-100.log")).unwrap();
        journal_messages(&dir, 101, 10);
        damage(&dir.join("journal-100.log"), 51);

        let report = repair(&dir).unwrap().unwrap();
        assert!(report.contains("journal-100.log"), "{report}");
        assert!(
            report.contains("journal.log as journal.log.corrupt"),
            "{report}"
        );
        let (_, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(
            store.counts("b").queued,
            50,
            "none of the changes made after the damaged ones"
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_full_disk_tears_nothing_and_loses_nothing() {
        let dir = temp_dir("full");
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Always).unwrap();
            let effect = message(1);
            journal.append(&effect).unwrap();
            store.apply(&effect);
            let length = fs::metadata(dir.join("journal.log")).unwrap().len();
            journal.fail_writes_after(Some(7));
            // A change already made waits for the disk; one not yet made is refused.
            let made = message(2);
            store.apply(&made);
            assert!(journal.append_applied(&made).is_err());
            assert!(journal.failing().unwrap().contains("no space"));
            assert!(journal.append(&message(3)).is_err());
            assert_eq!(
                fs::metadata(dir.join("journal.log")).unwrap().len(),
                length,
                "the torn writes are cut off"
            );
            assert!(!journal.needs_rewrite());
            journal.fail_writes_after(None);
            journal.flush_pending().unwrap();
            assert!(journal.failing().is_none());
            let effect = message(4);
            journal.append(&effect).unwrap();
            store.apply(&effect);
        }
        let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 3, "1, 2, and 4, in order");
        assert_eq!(journal.seq, 3);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rewrite_compacts_and_keeps_state() {
        let dir = temp_dir("rewrite");
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Never).unwrap();
            for ms in [1, 2] {
                let effect = message(ms);
                journal.append(&effect).unwrap();
                store.apply(&effect);
            }
            journal.rewrite(&mut store).unwrap();
            let effect = message(3);
            journal.append(&effect).unwrap();
        }
        assert!(fs::metadata(dir.join("journal.log")).unwrap().len() < 400);
        let (journal, store) = Journal::open(&dir, Fsync::Never).unwrap();
        assert_eq!(store.counts("b").queued, 3);
        assert_eq!(journal.seq, 3);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_crash_at_any_point_of_a_background_rewrite_loses_nothing() {
        let dir = temp_dir("background");
        let write = |journal: &mut Journal, store: &mut Store, ms| {
            let effect = message(ms);
            journal.append(&effect).unwrap();
            store.apply(&effect);
        };
        // The process dies after setting the journal aside, before the snapshot.
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Never).unwrap();
            write(&mut journal, &mut store, 1);
            write(&mut journal, &mut store, 2);
            let unwritten = journal.begin_rewrite(&mut store).unwrap();
            write(&mut journal, &mut store, 3);
            drop(unwritten);
        }
        let (mut journal, mut store) = Journal::open(&dir, Fsync::Never).unwrap();
        assert_eq!(store.counts("b").queued, 3);
        assert_eq!(journal.seq, 3);
        // The snapshot lands, but the process dies before deleting the journal
        // it covers: replay must skip that journal's lines, not apply them twice.
        let snapshot = journal.begin_rewrite(&mut store).unwrap();
        let covered = rotated_path(&dir, 3);
        let kept = fs::read(&covered).unwrap();
        snapshot.write();
        assert!(
            !covered.exists(),
            "a written snapshot deletes what it covers"
        );
        fs::write(&covered, kept).unwrap();
        write(&mut journal, &mut store, 4);
        drop(journal);
        let (journal, store) = Journal::open(&dir, Fsync::Never).unwrap();
        assert_eq!(store.counts("b").queued, 4);
        assert_eq!(journal.seq, 4);
        assert!(!covered.exists(), "startup deletes covered journals");
        drop(journal);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_rewrite_with_nothing_new_keeps_the_journal_set_aside() {
        let dir = temp_dir("set-aside");
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Never).unwrap();
            for ms in [1, 2, 3] {
                let effect = message(ms);
                journal.append(&effect).unwrap();
                store.apply(&effect);
            }
            // A snapshot sets the journal aside and fails, on a full disk say.
            let failed = journal.start_rewrite(&mut store).unwrap();
            store.abandon_capture();
            failed.abandon();
            // With nothing written since, the next one is set aside under the
            // same number; the process dies before that snapshot lands.
            drop(journal.begin_rewrite(&mut store).unwrap());
        }
        let (journal, store) = Journal::open(&dir, Fsync::Never).unwrap();
        assert_eq!(store.counts("b").queued, 3);
        assert_eq!(journal.seq, 3);
        drop(journal);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_tick_syncs_what_a_rotation_leaves_unsynced() {
        for fsync in [Fsync::EverySec, Fsync::Always, Fsync::Never] {
            let dir = temp_dir("rotation-sync");
            let (mut journal, mut store) = Journal::open(&dir, fsync).unwrap();
            let write = |journal: &mut Journal, store: &mut Store, ms| {
                let effect = message(ms);
                journal.append(&effect).unwrap();
                store.apply(&effect);
            };
            write(&mut journal, &mut store, 1);
            journal.begin_rewrite(&mut store).unwrap().write();
            write(&mut journal, &mut store, 2);
            let sync = journal.take_sync();
            if fsync == Fsync::EverySec {
                // The last write to the journal set aside, the renames, then
                // the write to the fresh one.
                let mut sync = sync.unwrap();
                assert_eq!(sync.retired.len(), 1);
                assert!(sync.dir.is_some() && sync.file.is_some());
                sync.run().unwrap();
            } else {
                // Each write was synced as it was made, the renames before
                // the first one after them, or none ever is.
                assert!(sync.is_none() && journal.retired.is_empty());
                assert_eq!(
                    journal.writer.lock().tail.dir_unsynced,
                    fsync == Fsync::Never
                );
            }
            assert!(journal.take_sync().is_none());
            drop(journal);
            let (_, store) = Journal::open(&dir, fsync).unwrap();
            assert_eq!(store.counts("b").queued, 2);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn a_failed_sync_refuses_changes_until_one_succeeds() {
        let dir = temp_dir("sync-failure");
        let (mut journal, mut store) = Journal::open(&dir, Fsync::EverySec).unwrap();
        let effect = message(1);
        journal.append(&effect).unwrap();
        store.apply(&effect);
        let tick = |journal: &mut Journal| {
            let mut sync = journal.take_sync().expect("something to sync");
            let result = sync.run();
            let failed = result.is_err();
            journal.synced(sync, result);
            failed
        };
        journal.fail_syncs(true);
        assert!(tick(&mut journal));
        assert!(journal.failing().unwrap().contains("cannot sync"));
        assert!(
            journal.append(&message(2)).is_err(),
            "a change not yet made is refused"
        );
        assert!(!journal.needs_rewrite());
        assert!(tick(&mut journal), "what failed is synced at the next tick");
        journal.fail_syncs(false);
        assert!(!tick(&mut journal));
        assert!(journal.failing().is_none());
        assert!(journal.take_sync().is_none(), "nothing is left to sync");
        assert!(
            journal.needs_rewrite(),
            "a snapshot writes again what the failed syncs may have lost"
        );
        journal.begin_rewrite(&mut store).unwrap().write();
        assert!(!journal.needs_rewrite());
        let effect = message(2);
        journal.append(&effect).unwrap();
        store.apply(&effect);
        drop(journal);
        let (_, store) = Journal::open(&dir, Fsync::EverySec).unwrap();
        assert_eq!(store.counts("b").queued, 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn writes_wait_for_a_sync_running_in_the_background() {
        for fsync in [Fsync::EverySec, Fsync::Always] {
            let dir = temp_dir("postponed");
            let (mut journal, _) = Journal::open(&dir, fsync).unwrap();
            let length = || fs::metadata(dir.join("journal.log")).unwrap().len();
            journal.resnapshot = true;
            journal.syncs().run(|| {
                journal.defer_applied(&message(1)).unwrap();
                journal.flush_soon().unwrap();
                // With every write synced before its reply, none waits.
                let postponed = fsync == Fsync::EverySec;
                assert_eq!(journal.waiting(), postponed);
                assert_eq!(length() == 0, postponed);
                assert!(!journal.needs_rewrite(), "a rewrite waits too");
                std::thread::sleep(POSTPONE_LIMIT + Duration::from_millis(20));
                assert!(journal.needs_rewrite());
                journal.flush_soon().unwrap();
                assert!(!journal.waiting(), "a sync past the limit holds up none");
            });
            journal.defer_applied(&message(2)).unwrap();
            journal.flush_soon().unwrap();
            assert!(!journal.waiting());
            drop(journal);
            let (_, store) = Journal::open(&dir, fsync).unwrap();
            assert_eq!(store.counts("b").queued, 2);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn a_snapshot_taken_while_the_forum_changes_restores_it_as_it_is() {
        let dir = temp_dir("live-forum");
        let (mut journal, mut store) = Journal::open(&dir, Fsync::Never).unwrap();
        let write = |journal: &mut Journal, store: &mut Store, effect: Effect| {
            journal.append(&effect).unwrap();
            store.apply(&effect);
        };
        for ms in 1..=300 {
            write(&mut journal, &mut store, message(ms));
        }
        let room = |name: &str| crate::store::Room {
            name: name.into(),
            topic: String::new(),
            created: 1,
            created_by: "a".into(),
            private: false,
            closed: None,
            members: BTreeMap::new(),
            board: BTreeMap::new(),
            posts: BTreeMap::new(),
            open: BTreeSet::new(),
        };
        write(&mut journal, &mut store, Effect::Room { room: room("r") });
        let set = |key: String, round: u64| Effect::Db {
            at: 1,
            commands: vec![vec!["SET".into(), key, format!("v{round}")]],
        };
        for key in 0..100 {
            write(&mut journal, &mut store, set(format!("k{key:03}"), 0));
        }
        let mut snapshot = journal.start_rewrite(&mut store).unwrap();
        for round in 0..300u64 {
            let id = Id {
                ms: round * 7919 % 300 + 1,
                seq: 0,
            };
            let effect = match round % 5 {
                0 => Effect::Settle {
                    id,
                    state: State::Acked,
                    at: 1000 + round,
                },
                1 => Effect::Drop { ids: vec![id] },
                2 => message(1000 + round),
                3 => Effect::Lease {
                    id,
                    consumer: "c".into(),
                    token: format!("t{round}"),
                    at: 5,
                    until: 10,
                },
                _ => Effect::Topic {
                    room: "r".into(),
                    topic: format!("round {round}"),
                },
            };
            write(&mut journal, &mut store, effect);
            // Keys change while the rooms and the messages are captured too.
            let key = format!("k{:03}", round * 31 % 100);
            write(&mut journal, &mut store, set(key, round));
            store.capture_step(256);
            snapshot.add(store.drain_capture().unwrap());
        }
        journal.finish_rewrite(&mut store, &mut snapshot).unwrap();
        snapshot.write();
        let live = serde_json::to_string(&store.snapshot()).unwrap();
        // Dropped without a shutdown, as in a crash: what the snapshot holds
        // and what the journal holds after it make the same forum.
        drop(journal);
        let (_, restored) = Journal::open(&dir, Fsync::Never).unwrap();
        assert_eq!(serde_json::to_string(&restored.snapshot()).unwrap(), live);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncates_a_torn_tail() {
        let dir = temp_dir("torn");
        {
            let (mut journal, _) = Journal::open(&dir, Fsync::Always).unwrap();
            journal.append(&message(1)).unwrap();
        }
        let path = dir.join("journal.log");
        let mut raw = fs::read(&path).unwrap();
        raw.extend_from_slice(b"{\"s\":2,\"e\":{\"op\":\"mess");
        fs::write(&path, &raw).unwrap();
        let (mut journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 1);
        journal.append(&message(2)).unwrap();
        drop(journal);
        let (_, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn refuses_mid_file_corruption() {
        let dir = temp_dir("corrupt");
        {
            let (mut journal, _) = Journal::open(&dir, Fsync::Always).unwrap();
            journal.append(&message(1)).unwrap();
        }
        let path = dir.join("journal.log");
        let mut raw = b"garbage\n".to_vec();
        raw.extend(fs::read(&path).unwrap());
        fs::write(&path, raw).unwrap();
        assert!(Journal::open(&dir, Fsync::Always).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn crc32c_gives_its_check_value_whichever_way_it_is_worked_out() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c_table(0, b"123456789"), 0xe306_9283);
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut bytes = Vec::new();
        for len in 0..300 {
            bytes.clear();
            for _ in 0..len {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                bytes.push(seed as u8);
            }
            let crc = crc32c(&bytes);
            assert_eq!(crc, crc32c_table(0, &bytes), "{len} bytes");
            // A checksum carried on over the rest of the bytes is that of all of them.
            let (first, rest) = bytes.split_at(len / 3);
            assert_eq!(crc32c_extend(crc32c(first), rest), crc, "{len} bytes");
            assert_eq!(crc32c_table(crc32c(first), rest), crc, "{len} bytes");
        }
    }

    #[test]
    fn a_snapshot_the_disk_changed_or_without_a_checksum_stops_the_server() {
        let dir = temp_dir("snapshot-checksum");
        {
            let (mut journal, mut store) = Journal::open(&dir, Fsync::Always).unwrap();
            for ms in [1, 2] {
                let effect = message(ms);
                journal.append(&effect).unwrap();
                store.apply(&effect);
            }
            journal.rewrite(&mut store).unwrap();
        }
        let path = dir.join("snapshot.json");
        let raw = fs::read(&path).unwrap();
        let (body, end) = raw.split_at(raw.len() - SNAPSHOT_END.len());
        assert!(end.starts_with(&SNAPSHOT_END[..6]) && end.ends_with(b"\"}"));
        assert_eq!(read_hex(&end[6..14]), Some(crc32c(body)));
        let (journal, store) = Journal::open(&dir, Fsync::Always).unwrap();
        assert_eq!(store.counts("b").queued, 2);
        drop(journal);
        // A byte the disk changed, in a snapshot that still parses, stops the
        // server, and repair, instead of loading what the disk made of it; so
        // does a snapshot without its checksum, or too short to hold one.
        let changed = String::from_utf8(raw.clone()).unwrap();
        let unchecked = [body, b"}"].concat();
        let damaged = [
            changed.replacen("body 1", "body 7", 1).into_bytes(),
            unchecked,
            b",\"c\":\"0\"}".to_vec(),
        ];
        for damaged in damaged {
            fs::write(&path, &damaged).unwrap();
            for err in [Journal::open(&dir, Fsync::Always).err(), repair(&dir).err()] {
                assert!(err.unwrap().contains("does not match its checksum"));
            }
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_line_the_disk_changed_or_without_a_checksum_stops_replay() {
        let dir = temp_dir("checksum");
        {
            let (mut journal, _) = Journal::open(&dir, Fsync::Always).unwrap();
            journal.append(&message(1)).unwrap();
            journal.append(&message(2)).unwrap();
        }
        let path = dir.join("journal.log");
        let text = fs::read_to_string(&path).unwrap();
        for line in text.lines() {
            assert!(line.as_bytes().starts_with(&CHECKSUM[..6]), "{line}");
        }
        // A byte the disk changed in the first line, which still parses,
        // stops replay instead of replaying what the line was changed to; so
        // does a first line without its checksum.
        let (first, rest) = text.split_once('\n').unwrap();
        let unchecked = format!("{{{}\n{rest}", &first[CHECKSUM.len()..]);
        for damaged in [text.replacen("body 1", "body 7", 1), unchecked] {
            fs::write(&path, &damaged).unwrap();
            let err = Journal::open(&dir, Fsync::Always).err().unwrap();
            assert!(err.contains("corrupt journal entry"), "{err}");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_data_dir_has_one_owner() {
        let dir = temp_dir("lock");
        let first = Journal::open(&dir, Fsync::Never).unwrap();
        let second = Journal::open(&dir, Fsync::Never);
        assert!(second.is_err());
        drop(first);
        assert!(Journal::open(&dir, Fsync::Never).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }
}
