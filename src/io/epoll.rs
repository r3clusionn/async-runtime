//! Readiness on Linux through epoll, edge-triggered: each socket is registered once for both
//! directions, and an event is reported when its state changes. Written to the same interface
//! as the Windows selector; compiled for Linux in this project's checks but not run there.

use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{Direction, ScheduledIo, ERROR, READABLE, READ_CLOSED, WRITABLE, WRITE_CLOSED};

const WAKE_TOKEN: u64 = 0;

pub(crate) struct Selector {
    ep: OwnedFd,
    wake_fd: OwnedFd,
    /// Registered sockets by token. Events carry the token, not a pointer, so an event for a
    /// socket closed meanwhile finds nothing instead of freed memory.
    registry: Mutex<HashMap<u64, Arc<ScheduledIo>>>,
    next: AtomicU64,
}

pub(crate) struct Events {
    buf: Vec<libc::epoll_event>,
    len: usize,
}

impl Events {
    pub(crate) fn with_capacity(n: usize) -> Events {
        Events { buf: vec![libc::epoll_event { events: 0, u64: 0 }; n], len: 0 }
    }
}

fn cvt(r: libc::c_int) -> io::Result<libc::c_int> {
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(r)
    }
}

impl Selector {
    pub(crate) fn new() -> io::Result<Selector> {
        // SAFETY: plain system calls; the descriptors are owned right away.
        unsafe {
            let ep = OwnedFd::from_raw_fd(cvt(libc::epoll_create1(libc::EPOLL_CLOEXEC))?);
            let wake_fd = OwnedFd::from_raw_fd(cvt(libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC))?);
            let mut ev = libc::epoll_event { events: libc::EPOLLIN as u32, u64: WAKE_TOKEN };
            cvt(libc::epoll_ctl(ep.as_raw_fd(), libc::EPOLL_CTL_ADD, wake_fd.as_raw_fd(), &mut ev))?;
            Ok(Selector { ep, wake_fd, registry: Mutex::new(HashMap::new()), next: AtomicU64::new(1) })
        }
    }

    pub(crate) fn register(&self, fd: RawFd, io: &Arc<ScheduledIo>) -> io::Result<u64> {
        let token = self.next.fetch_add(1, Relaxed);
        self.registry.lock().unwrap().insert(token, io.clone());
        let mut ev =
            libc::epoll_event { events: (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLRDHUP | libc::EPOLLET) as u32, u64: token };
        // SAFETY: plain system call.
        if let Err(e) = cvt(unsafe { libc::epoll_ctl(self.ep.as_raw_fd(), libc::EPOLL_CTL_ADD, fd, &mut ev) }) {
            self.registry.lock().unwrap().remove(&token);
            return Err(e);
        }
        Ok(token)
    }

    /// Nothing to do: the edge-triggered registration covers both directions for good.
    pub(crate) fn arm(&self, _io: &Arc<ScheduledIo>, _dir: Direction) {}

    pub(crate) fn deregister(&self, fd: RawFd, token: u64) {
        // SAFETY: plain system call; failure (already closed) is harmless.
        unsafe { libc::epoll_ctl(self.ep.as_raw_fd(), libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut()) };
        self.registry.lock().unwrap().remove(&token);
    }

    pub(crate) fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let ms = match timeout {
            None => -1,
            Some(d) => (d.as_millis() + u128::from(d.subsec_nanos() % 1_000_000 != 0)).min(i32::MAX as u128) as i32,
        };
        // SAFETY: the buffer holds `len` entries.
        let n = unsafe { libc::epoll_wait(self.ep.as_raw_fd(), events.buf.as_mut_ptr(), events.buf.len() as i32, ms) };
        if n < 0 {
            events.len = 0;
            let e = io::Error::last_os_error();
            return if e.kind() == io::ErrorKind::Interrupted { Ok(()) } else { Err(e) };
        }
        events.len = n as usize;
        Ok(())
    }

    pub(crate) fn dispatch(&self, events: &Events) {
        let mut found = Vec::with_capacity(events.len);
        {
            let reg = self.registry.lock().unwrap();
            for ev in &events.buf[..events.len] {
                let (flags, token) = (ev.events, ev.u64);
                if token == WAKE_TOKEN {
                    let mut buf = [0u8; 8];
                    // SAFETY: reads the counter to reset it.
                    unsafe { libc::read(self.wake_fd.as_raw_fd(), buf.as_mut_ptr().cast(), 8) };
                    continue;
                }
                if let Some(io) = reg.get(&token) {
                    found.push((io.clone(), flags));
                }
            }
        }
        for (io, flags) in found {
            let f = |x: libc::c_int| flags & x as u32 != 0;
            let mut ready = 0;
            if f(libc::EPOLLIN) || f(libc::EPOLLPRI) {
                ready |= READABLE;
            }
            if f(libc::EPOLLOUT) {
                ready |= WRITABLE;
            }
            if f(libc::EPOLLRDHUP) || f(libc::EPOLLHUP) {
                ready |= READABLE | READ_CLOSED;
            }
            if f(libc::EPOLLHUP) {
                ready |= WRITABLE | WRITE_CLOSED;
            }
            if f(libc::EPOLLERR) {
                ready |= READABLE | WRITABLE | ERROR;
            }
            io.set_readiness(ready);
        }
    }

    pub(crate) fn wake(&self) {
        let one = 1u64.to_ne_bytes();
        // SAFETY: writes the 8-byte counter increment.
        unsafe { libc::write(self.wake_fd.as_raw_fd(), one.as_ptr().cast(), 8) };
    }
}
