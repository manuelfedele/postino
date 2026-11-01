//! Waiting on many sockets at once: what lets one thread serve many
//! connections (see [`crate::event`]), and the one thing the server needs
//! that the standard library does not wrap. A [`Poller`] is told once which
//! descriptors to watch, and for what, and reports the ones that are ready:
//! with kqueue on macOS, epoll on Linux, and `poll(2)` on other Unixes.

use std::ffi::c_int;
use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

/// Watch for data to read, or for the peer closing its end.
pub const READ: u8 = 1;
/// Watch for room to write.
pub const WRITE: u8 = 2;

/// A descriptor that is ready. One that failed, or whose peer hung up, is
/// reported both readable and writable, so the next read or write returns
/// what happened.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Event {
    pub token: usize,
    pub readable: bool,
    pub writable: bool,
}

/// What a system call returned, or the error it set.
fn check(result: c_int) -> io::Result<c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

/// Milliseconds to wait, rounded up, so that a deadline less than one away
/// is waited for rather than polled for in a loop.
#[cfg(not(target_os = "macos"))]
fn millis(timeout: Duration) -> c_int {
    timeout.as_micros().div_ceil(1000).min(c_int::MAX as u128) as c_int
}

#[cfg(target_os = "macos")]
mod backend {
    use super::*;
    use std::ffi::c_void;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    /// `struct kevent`, as it is on 64-bit macOS.
    #[repr(C)]
    struct Kevent {
        ident: usize,
        filter: i16,
        flags: u16,
        fflags: u32,
        data: isize,
        udata: *mut c_void,
    }

    /// `struct timespec`.
    #[repr(C)]
    struct Timespec {
        tv_sec: i64,
        tv_nsec: i64,
    }

    unsafe extern "C" {
        fn kqueue() -> c_int;
        fn kevent(
            kq: c_int,
            changes: *const Kevent,
            nchanges: c_int,
            events: *mut Kevent,
            nevents: c_int,
            timeout: *const Timespec,
        ) -> c_int;
    }

    const EVFILT_READ: i16 = -1;
    const EVFILT_WRITE: i16 = -2;
    const EV_ADD: u16 = 0x1;
    const EV_DELETE: u16 = 0x2;
    const EV_ERROR: u16 = 0x4000;
    const EV_EOF: u16 = 0x8000;

    pub struct Poller {
        kq: OwnedFd,
        events: Vec<Kevent>,
    }

    impl Poller {
        pub fn new() -> io::Result<Poller> {
            // SAFETY: kqueue takes no arguments; a descriptor it returns is
            // owned from here on, and closed with the poller.
            let kq = unsafe { OwnedFd::from_raw_fd(check(kqueue())?) };
            Ok(Poller {
                kq,
                events: Vec::with_capacity(256),
            })
        }

        pub fn watch(&mut self, fd: RawFd, token: usize, old: u8, new: u8) -> io::Result<()> {
            let change = |filter, flags| Kevent {
                ident: fd as usize,
                filter,
                flags,
                fflags: 0,
                data: 0,
                udata: std::ptr::without_provenance_mut(token),
            };
            let mut changes = Vec::with_capacity(2);
            for (bit, filter) in [(READ, EVFILT_READ), (WRITE, EVFILT_WRITE)] {
                match (old & bit != 0, new & bit != 0) {
                    (false, true) => changes.push(change(filter, EV_ADD)),
                    (true, false) => changes.push(change(filter, EV_DELETE)),
                    _ => {}
                }
            }
            if changes.is_empty() {
                return Ok(());
            }
            // SAFETY: `changes` holds `changes.len()` initialized kevents;
            // no events are asked for, so none are written.
            check(unsafe {
                kevent(
                    self.kq.as_raw_fd(),
                    changes.as_ptr(),
                    changes.len() as c_int,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            })?;
            Ok(())
        }

        pub fn wait(&mut self, ready: &mut Vec<Event>, timeout: Duration) -> io::Result<()> {
            ready.clear();
            let timeout = Timespec {
                tv_sec: timeout.as_secs() as i64,
                tv_nsec: timeout.subsec_nanos() as i64,
            };
            let capacity = self.events.capacity();
            // SAFETY: up to `capacity` kevents are written into the buffer's
            // spare room, and only the number kevent reports are kept.
            let n = check(unsafe {
                kevent(
                    self.kq.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    self.events.as_mut_ptr(),
                    capacity as c_int,
                    &timeout,
                )
            });
            let n = match n {
                Ok(n) => n as usize,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => return Ok(()),
                Err(e) => return Err(e),
            };
            // SAFETY: kevent initialized the first `n` entries.
            unsafe { self.events.set_len(n) };
            for event in self.events.drain(..) {
                let failed = event.flags & (EV_ERROR | EV_EOF) != 0;
                ready.push(Event {
                    token: event.udata.addr(),
                    readable: event.filter == EVFILT_READ || failed,
                    writable: event.filter == EVFILT_WRITE || failed,
                });
            }
            Ok(())
        }
    }

    // SAFETY: the raw pointers in the buffer are tokens, never dereferenced.
    unsafe impl Send for Poller {}
}

#[cfg(target_os = "linux")]
mod backend {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    /// `struct epoll_event`, packed on x86_64 alone.
    #[repr(C)]
    #[cfg_attr(target_arch = "x86_64", repr(packed))]
    #[derive(Clone, Copy)]
    struct EpollEvent {
        events: u32,
        data: u64,
    }

    unsafe extern "C" {
        fn epoll_create1(flags: c_int) -> c_int;
        fn epoll_ctl(epfd: c_int, op: c_int, fd: c_int, event: *mut EpollEvent) -> c_int;
        fn epoll_wait(epfd: c_int, events: *mut EpollEvent, max: c_int, timeout: c_int) -> c_int;
    }

    const EPOLL_CLOEXEC: c_int = 0o2000000;
    const EPOLL_CTL_ADD: c_int = 1;
    const EPOLL_CTL_DEL: c_int = 2;
    const EPOLL_CTL_MOD: c_int = 3;
    const EPOLLIN: u32 = 0x1;
    const EPOLLOUT: u32 = 0x4;
    const EPOLLERR: u32 = 0x8;
    const EPOLLHUP: u32 = 0x10;
    const EPOLLRDHUP: u32 = 0x2000;

    pub struct Poller {
        epfd: OwnedFd,
        events: Vec<EpollEvent>,
    }

    impl Poller {
        pub fn new() -> io::Result<Poller> {
            // SAFETY: a descriptor epoll_create1 returns is owned from here
            // on, and closed with the poller.
            let epfd = unsafe { OwnedFd::from_raw_fd(check(epoll_create1(EPOLL_CLOEXEC))?) };
            Ok(Poller {
                epfd,
                events: Vec::with_capacity(256),
            })
        }

        pub fn watch(&mut self, fd: RawFd, token: usize, old: u8, new: u8) -> io::Result<()> {
            let mut event = EpollEvent {
                events: if new & READ != 0 {
                    EPOLLIN | EPOLLRDHUP
                } else {
                    0
                } | if new & WRITE != 0 { EPOLLOUT } else { 0 },
                data: token as u64,
            };
            let op = match (old, new) {
                (0, 0) => return Ok(()),
                (0, _) => EPOLL_CTL_ADD,
                (_, 0) => EPOLL_CTL_DEL,
                _ if old == new => return Ok(()),
                _ => EPOLL_CTL_MOD,
            };
            // SAFETY: `event` is a valid epoll_event for the call to read.
            check(unsafe { epoll_ctl(self.epfd.as_raw_fd(), op, fd, &mut event) })?;
            Ok(())
        }

        pub fn wait(&mut self, ready: &mut Vec<Event>, timeout: Duration) -> io::Result<()> {
            ready.clear();
            let capacity = self.events.capacity();
            // SAFETY: up to `capacity` events are written into the buffer's
            // spare room, and only the number epoll_wait reports are kept.
            let n = check(unsafe {
                epoll_wait(
                    self.epfd.as_raw_fd(),
                    self.events.as_mut_ptr(),
                    capacity as c_int,
                    millis(timeout),
                )
            });
            let n = match n {
                Ok(n) => n as usize,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => return Ok(()),
                Err(e) => return Err(e),
            };
            // SAFETY: epoll_wait initialized the first `n` entries.
            unsafe { self.events.set_len(n) };
            for event in self.events.drain(..) {
                let (bits, token) = (event.events, event.data);
                let failed = bits & (EPOLLERR | EPOLLHUP) != 0;
                ready.push(Event {
                    token: token as usize,
                    readable: bits & (EPOLLIN | EPOLLRDHUP) != 0 || failed,
                    writable: bits & EPOLLOUT != 0 || failed,
                });
            }
            Ok(())
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod backend {
    use super::*;
    use std::ffi::{c_short, c_uint};

    /// `struct pollfd`, the same on every Unix.
    #[repr(C)]
    struct PollFd {
        fd: c_int,
        events: c_short,
        revents: c_short,
    }

    unsafe extern "C" {
        fn poll(fds: *mut PollFd, count: c_uint, timeout: c_int) -> c_int;
    }

    const POLLIN: c_short = 0x1;
    const POLLOUT: c_short = 0x4;
    const POLLFAILED: c_short = 0x8 | 0x10 | 0x20;

    /// Watches kept in user space, all handed to the kernel at each wait.
    pub struct Poller {
        watched: Vec<(RawFd, usize, u8)>,
        fds: Vec<PollFd>,
    }

    impl Poller {
        pub fn new() -> io::Result<Poller> {
            Ok(Poller {
                watched: Vec::new(),
                fds: Vec::new(),
            })
        }

        pub fn watch(&mut self, fd: RawFd, token: usize, _old: u8, new: u8) -> io::Result<()> {
            self.watched.retain(|(_, t, _)| *t != token);
            if new != 0 {
                self.watched.push((fd, token, new));
            }
            Ok(())
        }

        pub fn wait(&mut self, ready: &mut Vec<Event>, timeout: Duration) -> io::Result<()> {
            ready.clear();
            self.fds.clear();
            for &(fd, _, interest) in &self.watched {
                let events = if interest & READ != 0 { POLLIN } else { 0 }
                    | if interest & WRITE != 0 { POLLOUT } else { 0 };
                self.fds.push(PollFd {
                    fd,
                    events,
                    revents: 0,
                });
            }
            // SAFETY: `fds` is an exclusively borrowed array of pollfds, as
            // long as the count passed.
            let n = unsafe {
                poll(
                    self.fds.as_mut_ptr(),
                    self.fds.len() as c_uint,
                    millis(timeout),
                )
            };
            match check(n) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => return Ok(()),
                Err(e) => return Err(e),
            }
            for (fd, &(_, token, _)) in self.fds.iter().zip(&self.watched) {
                let failed = fd.revents & POLLFAILED != 0;
                if fd.revents != 0 {
                    ready.push(Event {
                        token,
                        readable: fd.revents & POLLIN != 0 || failed,
                        writable: fd.revents & POLLOUT != 0 || failed,
                    });
                }
            }
            Ok(())
        }
    }
}

pub use backend::Poller;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    fn wait(poller: &mut Poller, timeout: Duration) -> Vec<Event> {
        let mut ready = Vec::new();
        poller.wait(&mut ready, timeout).unwrap();
        ready
    }

    #[test]
    fn reports_what_can_be_read_and_written_until_told_otherwise() {
        let mut poller = Poller::new().unwrap();
        let (mut a, b) = UnixStream::pair().unwrap();
        let fd = b.as_raw_fd();
        poller.watch(fd, 7, 0, READ).unwrap();
        assert!(wait(&mut poller, Duration::from_millis(10)).is_empty());

        a.write_all(b"x").unwrap();
        let ready = wait(&mut poller, Duration::from_secs(1));
        assert_eq!(ready.len(), 1);
        assert!(ready[0].token == 7 && ready[0].readable);
        // Unread data is reported again: readiness is a state, not an edge.
        assert_eq!(wait(&mut poller, Duration::from_secs(1)).len(), 1);

        poller.watch(fd, 7, READ, READ | WRITE).unwrap();
        let ready = wait(&mut poller, Duration::from_secs(1));
        assert!(ready.iter().any(|e| e.readable) && ready.iter().any(|e| e.writable));

        poller.watch(fd, 7, READ | WRITE, 0).unwrap();
        assert!(wait(&mut poller, Duration::from_millis(10)).is_empty());

        poller.watch(fd, 7, 0, READ).unwrap();
        drop(a);
        let ready = wait(&mut poller, Duration::from_secs(1));
        assert!(
            ready.iter().all(|e| e.token == 7 && e.readable),
            "a closed peer reads as the end"
        );
        assert!(!ready.is_empty());
    }
}
