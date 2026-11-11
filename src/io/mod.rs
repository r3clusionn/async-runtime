//! Readiness-based I/O: the driver learns that a socket can be read or written, wakes the task
//! waiting for that, and the task retries its non-blocking call.
//!
//! Each registered socket has a [`ScheduledIo`]: a readiness word and a waker for each
//! direction. A task tries the call first; on `WouldBlock` it clears the readiness bit it acted
//! on and waits for the next event. The readiness word carries a counter that every event
//! increments, so a clear only applies if no new event arrived since the task looked: an event
//! that lands between the failed call and the clear is not lost.

#[cfg(windows)]
mod afd;
#[cfg(target_os = "linux")]
mod epoll;

#[cfg(windows)]
pub(crate) use afd::{AfdIo, Events, Selector};
#[cfg(target_os = "linux")]
pub(crate) use epoll::{Events, Selector};

use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::runtime::Handle;
use crate::waker::AtomicWaker;

pub(crate) const READABLE: usize = 1;
pub(crate) const WRITABLE: usize = 2;
pub(crate) const READ_CLOSED: usize = 4;
pub(crate) const WRITE_CLOSED: usize = 8;
pub(crate) const ERROR: usize = 16;
const READY_MASK: usize = 0xff;
const TICK_SHIFT: u32 = 8;

#[cfg(windows)]
pub(crate) type RawSource = std::os::windows::io::RawSocket;
#[cfg(unix)]
pub(crate) type RawSource = std::os::fd::RawFd;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

impl Direction {
    fn mask(self) -> usize {
        match self {
            Direction::Read => READABLE | READ_CLOSED | ERROR,
            Direction::Write => WRITABLE | WRITE_CLOSED | ERROR,
        }
    }
}

/// Readiness seen by a task, with the event counter at that moment.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReadyEvent {
    ready: usize,
    tick: usize,
}

pub(crate) struct ScheduledIo {
    readiness: AtomicUsize,
    reader: AtomicWaker,
    writer: AtomicWaker,
    #[cfg(windows)]
    pub(crate) afd: AfdIo,
}

impl ScheduledIo {
    fn new() -> ScheduledIo {
        ScheduledIo {
            readiness: AtomicUsize::new(0),
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            #[cfg(windows)]
            afd: AfdIo::new(),
        }
    }

    /// Records an event from the driver and wakes whoever waits for it.
    pub(crate) fn set_readiness(&self, ready: usize) {
        let _ = self.readiness.fetch_update(AcqRel, Acquire, |cur| {
            let tick = (cur >> TICK_SHIFT).wrapping_add(1);
            Some((cur & READY_MASK) | ready | (tick << TICK_SHIFT))
        });
        if ready & Direction::Read.mask() != 0 {
            self.reader.wake();
        }
        if ready & Direction::Write.mask() != 0 {
            self.writer.wake();
        }
    }

    fn clear(&self, ev: ReadyEvent) {
        // Closed and error states stay: once seen they are final.
        let bits = ev.ready & (READABLE | WRITABLE);
        let _ = self.readiness.fetch_update(AcqRel, Acquire, |cur| ((cur >> TICK_SHIFT) == ev.tick).then_some(cur & !bits));
    }

    fn waker(&self, dir: Direction) -> &AtomicWaker {
        match dir {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }

    fn ready_now(&self, dir: Direction) -> Option<ReadyEvent> {
        let cur = self.readiness.load(Acquire);
        let ready = cur & dir.mask();
        (ready != 0).then_some(ReadyEvent { ready, tick: cur >> TICK_SHIFT })
    }
}

/// A socket registered with the current runtime's driver.
pub(crate) struct Registration {
    io: Arc<ScheduledIo>,
    handle: Handle,
    #[cfg(unix)]
    fd: RawSource,
    #[cfg(unix)]
    token: u64,
}

impl Registration {
    /// Registers a non-blocking socket with the current runtime.
    pub(crate) fn new(source: RawSource) -> io::Result<Registration> {
        let handle = Handle::try_current().ok_or_else(|| io::Error::other("not inside a strand runtime"))?;
        let io = Arc::new(ScheduledIo::new());
        #[cfg(unix)]
        let token = handle.driver().selector().register(source, &io)?;
        #[cfg(windows)]
        handle.driver().selector().register(source, &io)?;
        Ok(Registration {
            io,
            handle,
            #[cfg(unix)]
            fd: source,
            #[cfg(unix)]
            token,
        })
    }

    fn poll_ready(&self, cx: &mut Context<'_>, dir: Direction) -> Poll<ReadyEvent> {
        if let Some(ev) = self.io.ready_now(dir) {
            return Poll::Ready(ev);
        }
        self.io.waker(dir).register(cx.waker());
        self.handle.driver().selector().arm(&self.io, dir);
        match self.io.ready_now(dir) {
            Some(ev) => Poll::Ready(ev),
            None => Poll::Pending,
        }
    }

    /// Runs the non-blocking call `f` when the socket is ready in `dir`, retrying after each
    /// `WouldBlock` once the driver reports readiness again.
    pub(crate) fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        dir: Direction,
        mut f: impl FnMut() -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        loop {
            let ev = match self.poll_ready(cx, dir) {
                Poll::Ready(ev) => ev,
                Poll::Pending => return Poll::Pending,
            };
            match f() {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.io.clear(ev),
                r => {
                    crate::coop::spend();
                    return Poll::Ready(r);
                }
            }
        }
    }

    /// Waits until the socket is ready in `dir` (used for connect, which has no call to retry).
    pub(crate) async fn ready(&self, dir: Direction) {
        std::future::poll_fn(|cx| self.poll_ready(cx, dir).map(|_| ())).await
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.handle.driver().selector().deregister(self.fd, self.token);
        #[cfg(windows)]
        self.handle.driver().selector().deregister(&self.io);
    }
}
