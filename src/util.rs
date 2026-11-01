use std::collections::hash_map::RandomState;
use std::fs::{self, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Creates `dir` and its parents so only the current user can enter it, and
/// narrows an existing directory that others can read. The data directory
/// holds every message and key in plain text.
pub fn private_dir(dir: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        builder.mode(0o700);
        builder.create(dir)?;
        let mode = fs::metadata(dir)?.permissions().mode();
        if mode & 0o077 != 0
            && fs::set_permissions(dir, fs::Permissions::from_mode(mode & 0o700)).is_err()
        {
            eprintln!(
                "postino: {} is readable by other users and cannot be narrowed; restrict it with chmod 700",
                dir.display()
            );
        }
        Ok(())
    }
    #[cfg(not(unix))]
    builder.create(dir)
}

/// Open options that create files readable only by the current user.
pub fn private_file() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Formats milliseconds since the epoch as RFC 3339 UTC, e.g. `2026-10-30T09:15:02.481Z`.
pub fn iso8601(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60,
        ms % 1000
    )
}

// Howard Hinnant's days-to-civil algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Random hex string for a secret, such as a token: from the operating
/// system's generator, or an error when it cannot be read, since nothing
/// weaker may stand in for it.
pub fn secret_hex(bytes: usize) -> io::Result<String> {
    let mut buf = vec![0u8; bytes];
    system_random(&mut buf)?;
    Ok(to_hex(&buf))
}

/// Fills `buf` from the operating system's cryptographic generator.
#[cfg(not(windows))]
fn system_random(buf: &mut [u8]) -> io::Result<()> {
    use std::io::Read;
    fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(buf))
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("cannot read /dev/urandom for a secret: {e}"),
            )
        })
}

/// Fills `buf` from the operating system's cryptographic generator, the
/// one the standard library seeds its own hashing from.
#[cfg(windows)]
fn system_random(buf: &mut [u8]) -> io::Result<()> {
    #[link(name = "bcryptprimitives", kind = "raw-dylib")]
    unsafe extern "system" {
        fn ProcessPrng(data: *mut u8, len: usize) -> i32;
    }
    // SAFETY: `buf` is valid for writes of its whole length. The call
    // cannot fail: it fills the buffer and returns TRUE whatever happens.
    unsafe { ProcessPrng(buf.as_mut_ptr(), buf.len()) };
    Ok(())
}

/// Random hex string for a name that must not repeat but need not be
/// secret. Reads the operating system's generator, as [`secret_hex`] does,
/// and falls back to SipHash keyed by `RandomState`, whose keys come from
/// the OS on every platform.
pub fn random_hex(bytes: usize) -> String {
    if let Ok(hex) = secret_hex(bytes) {
        return hex;
    }
    let mut buf = vec![0u8; bytes];
    for (i, chunk) in buf.chunks_mut(8).enumerate() {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        hasher.write_usize(i);
        let value = hasher.finish().to_le_bytes();
        chunk.copy_from_slice(&value[..chunk.len()]);
    }
    to_hex(&buf)
}

pub fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 15)] as char);
    }
    out
}

pub fn from_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A non-cryptographic random number below `bound`, for picking set members.
pub fn random_below(bound: usize) -> usize {
    if bound == 0 {
        return 0;
    }
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    (hasher.finish() % bound as u64) as usize
}

pub fn fnv1a(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Agent names: 1-64 chars, alphanumeric first, then alphanumerics, `-`, `_`, or `.`.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Idempotency keys and session ids: 1-128 printable ASCII characters without spaces.
pub fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| b.is_ascii_graphic())
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn home_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from)
}

pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Raises the limit on open files to `wanted`, as far as the hard limit
/// lets it, if it is lower; returns the limit then, or `None` where it is not
/// known.
#[cfg(all(
    any(target_os = "macos", target_os = "linux"),
    target_pointer_width = "64"
))]
pub fn open_files_limit(wanted: u64) -> Option<u64> {
    use std::ffi::c_int;

    /// `struct rlimit`, with the 64-bit `rlim_t` of both.
    #[repr(C)]
    struct Rlimit {
        cur: u64,
        max: u64,
    }
    unsafe extern "C" {
        fn getrlimit(resource: c_int, limit: *mut Rlimit) -> c_int;
        fn setrlimit(resource: c_int, limit: *const Rlimit) -> c_int;
    }
    #[cfg(target_os = "linux")]
    const RLIMIT_NOFILE: c_int = 7;
    #[cfg(target_os = "macos")]
    const RLIMIT_NOFILE: c_int = 8;

    let mut limit = Rlimit { cur: 0, max: 0 };
    // SAFETY: getrlimit writes one rlimit into the struct it is given.
    if unsafe { getrlimit(RLIMIT_NOFILE, &mut limit) } != 0 {
        return None;
    }
    if limit.cur < wanted {
        let raised = Rlimit {
            cur: wanted.min(limit.max),
            max: limit.max,
        };
        // SAFETY: setrlimit only reads the struct it is given.
        if unsafe { setrlimit(RLIMIT_NOFILE, &raised) } == 0 {
            return Some(raised.cur);
        }
    }
    Some(limit.cur)
}

#[cfg(not(all(
    any(target_os = "macos", target_os = "linux"),
    target_pointer_width = "64"
)))]
pub fn open_files_limit(_wanted: u64) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(
        any(target_os = "macos", target_os = "linux"),
        target_pointer_width = "64"
    ))]
    #[test]
    fn the_limit_on_open_files_is_read_and_never_lowered() {
        let limit = open_files_limit(0).unwrap();
        assert!(limit > 0);
        assert_eq!(open_files_limit(limit), Some(limit));
    }

    #[test]
    fn formats_timestamps() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(951_782_400_123), "2000-02-29T00:00:00.123Z");
        assert_eq!(iso8601(1_793_351_702_481), "2026-10-30T09:15:02.481Z");
    }

    #[test]
    fn validates_names() {
        assert!(valid_name("reviewer"));
        assert!(valid_name("my-agent_v2.0"));
        assert!(!valid_name(""));
        assert!(!valid_name("-leading"));
        assert!(!valid_name("agent<script>"));
        assert!(!valid_name(&"a".repeat(65)));
    }

    #[test]
    fn random_hex_has_requested_length() {
        let a = random_hex(8);
        assert_eq!(a.len(), 16);
        assert_ne!(a, random_hex(8));
    }

    #[test]
    fn secrets_come_from_the_system_generator() {
        let a = secret_hex(32).unwrap();
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(a, secret_hex(32).unwrap());
    }
}
