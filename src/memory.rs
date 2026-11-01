//! How much memory the process holds, so the server can refuse to grow past
//! `POSTINO_MAX_MEMORY` instead of being killed by the system.
//!
//! The binary installs [`Counting`] as its allocator. Code that runs without it,
//! such as unit tests, sees zero bytes in use and is never refused.
//!
//! Each thread adds up what it allocates and frees, and passes the total on to
//! the shared count once it comes to [`BATCH`] bytes either way. One counter
//! updated on every allocation by every thread costs more than the allocations
//! themselves: with 32 clients it took three quarters of the server's time.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, Ordering};

/// Bytes allocated, as passed on by the threads. A block freed by another
/// thread than allocated it can take it briefly below zero.
static IN_USE: AtomicIsize = AtomicIsize::new(0);

/// What a thread counts before passing it on: the shared count is short of
/// the truth by at most this much per running thread.
const BATCH: isize = 32 * 1024;

thread_local! {
    /// This thread's count, not yet passed on. No destructor: the allocator
    /// may run while the thread is being torn down.
    static PENDING: Cell<isize> = const { Cell::new(0) };
}

fn count(bytes: isize) {
    let passed = PENDING
        .try_with(|pending| {
            let total = pending.get() + bytes;
            if total.abs() < BATCH {
                pending.set(total);
                0
            } else {
                pending.set(0);
                total
            }
        })
        .unwrap_or(bytes);
    if passed != 0 {
        IN_USE.fetch_add(passed, Ordering::Relaxed);
    }
}

/// Passes on what this thread has counted, before it ends.
pub fn flush() {
    let pending = PENDING.try_with(|pending| pending.replace(0)).unwrap_or(0);
    if pending != 0 {
        IN_USE.fetch_add(pending, Ordering::Relaxed);
    }
}

/// Flushes when dropped, for a thread that ends in more than one place.
pub struct FlushOnDrop;

impl Drop for FlushOnDrop {
    fn drop(&mut self) {
        flush();
    }
}

/// The system allocator, counting the bytes it hands out.
pub struct Counting;

// SAFETY: every call is forwarded unchanged to the system allocator; the
// counter only observes sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `alloc`'s contract, which we pass on.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: as for `alloc`.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(layout.size() as isize);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from this allocator with this layout.
        unsafe { System.dealloc(ptr, layout) };
        count(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr` came from this allocator with this layout.
        let moved = unsafe { System.realloc(ptr, layout, new_size) };
        if !moved.is_null() {
            count(new_size as isize - layout.size() as isize);
        }
        moved
    }
}

/// Bytes currently allocated through [`Counting`].
pub fn in_use() -> usize {
    IN_USE.load(Ordering::Relaxed).max(0) as usize
}

/// The default memory limit: 1 GiB, or three quarters of a smaller container
/// limit, so the server refuses writes before the container is killed.
pub fn default_limit() -> usize {
    const GIB: usize = 1 << 30;
    let container = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .filter_map(|path| std::fs::read_to_string(path).ok())
    .find_map(|text| text.trim().parse::<usize>().ok());
    match container {
        Some(limit) if limit / 4 * 3 < GIB => limit / 4 * 3,
        _ => GIB,
    }
}

/// `512mb`, `2gb`, `1048576`: a size in bytes, with an optional unit.
pub fn parse_size(text: &str) -> Option<usize> {
    let text = text.trim().to_ascii_lowercase();
    let digits = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(digits);
    let scale: usize = match unit.trim() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1 << 10,
        "m" | "mb" | "mib" => 1 << 20,
        "g" | "gb" | "gib" => 1 << 30,
        _ => return None,
    };
    number.parse::<usize>().ok()?.checked_mul(scale)
}

/// Megabytes, for messages.
pub fn mib(bytes: usize) -> String {
    format!("{:.1} MiB", bytes as f64 / (1 << 20) as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_take_units() {
        assert_eq!(parse_size("1048576"), Some(1 << 20));
        assert_eq!(parse_size("512mb"), Some(512 << 20));
        assert_eq!(parse_size("2GB"), Some(2 << 30));
        assert_eq!(parse_size("64 KiB"), Some(64 << 10));
        assert_eq!(parse_size("lots"), None);
        assert_eq!(parse_size("12tb"), None);
    }
}
