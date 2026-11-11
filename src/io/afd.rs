//! Readiness on Windows through the Ancillary Function Driver.
//!
//! Windows has no epoll, but every socket belongs to the AFD driver, and AFD accepts a poll
//! request (`IOCTL_AFD_POLL`) for a set of events on a socket. Sent asynchronously on a handle
//! that is bound to an I/O completion port, the request completes on that port when one of the
//! events happens. That gives epoll's readiness model on top of completion ports; mio (tokio)
//! and wepoll do the same. A request is one-shot, so the driver sends a new one whenever a task
//! is waiting for something the last one did not report.
//!
//! The poll's status block and event buffer live inside the socket's `ScheduledIo`, which is
//! kept alive by a reference handed to the kernel with the request and taken back when its
//! completion is dequeued. A socket that is closed while a poll is pending has that poll
//! cancelled; the memory is freed when the cancellation's completion arrives.

use std::cell::UnsafeCell;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Release};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::{NtCancelIoFileEx, NtCreateFile, FILE_OPEN};
use windows_sys::Wdk::System::IO::NtDeviceIoControlFile;
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS, UNICODE_STRING, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{WSAIoctl, SIO_BASE_HANDLE, SOCKET, SOCKET_ERROR};
use windows_sys::Win32::Storage::FileSystem::{
    SetFileCompletionNotificationModes, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
};
use windows_sys::Win32::System::Threading::{
    CreateWaitableTimerExW, SetWaitableTimer, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus, IO_STATUS_BLOCK, OVERLAPPED_ENTRY,
};

use super::{Direction, ScheduledIo, ERROR, READABLE, READ_CLOSED, WRITABLE, WRITE_CLOSED};

const IOCTL_AFD_POLL: u32 = 0x0001_2024;
const STATUS_PENDING: NTSTATUS = 0x0000_0103;
const STATUS_CANCELLED: NTSTATUS = 0xC000_0120_u32 as NTSTATUS;
const FILE_SKIP_SET_EVENT_ON_HANDLE: u8 = 2;

const AFD_POLL_RECEIVE: u32 = 0x0001;
const AFD_POLL_SEND: u32 = 0x0004;
const AFD_POLL_DISCONNECT: u32 = 0x0008;
const AFD_POLL_ABORT: u32 = 0x0010;
const AFD_POLL_LOCAL_CLOSE: u32 = 0x0020;
const AFD_POLL_ACCEPT: u32 = 0x0080;
const AFD_POLL_CONNECT_FAIL: u32 = 0x0100;

const READ_EVENTS: u32 = AFD_POLL_RECEIVE | AFD_POLL_ACCEPT | AFD_POLL_DISCONNECT | AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL;
const WRITE_EVENTS: u32 = AFD_POLL_SEND | AFD_POLL_ABORT | AFD_POLL_CONNECT_FAIL;

/// Completion key of the posts that wake the driver; AFD completions use key 0.
const WAKE_KEY: usize = 1;

#[repr(C)]
#[derive(Clone, Copy)]
struct PollHandle {
    handle: HANDLE,
    events: u32,
    status: NTSTATUS,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PollInfo {
    timeout: i64,
    number_of_handles: u32,
    exclusive: u32,
    handles: [PollHandle; 1],
}

struct PollState {
    /// AFD events some task waits for.
    wanted: u32,
    /// The events of the request the kernel holds, if any.
    in_flight: Option<u32>,
    cancelling: bool,
    closed: bool,
}

/// The Windows part of a socket's `ScheduledIo`.
pub(crate) struct AfdIo {
    base: AtomicUsize,
    state: Mutex<PollState>,
    // Written by the kernel while a request is in flight; touched by us only when none is.
    iosb: UnsafeCell<IO_STATUS_BLOCK>,
    info: UnsafeCell<PollInfo>,
}

// SAFETY: the cells are only accessed with the state lock held and no request in flight, or by
// the driver after the request's completion was dequeued.
unsafe impl Send for AfdIo {}
unsafe impl Sync for AfdIo {}

impl AfdIo {
    pub(crate) fn new() -> AfdIo {
        AfdIo {
            base: AtomicUsize::new(0),
            state: Mutex::new(PollState { wanted: 0, in_flight: None, cancelling: false, closed: false }),
            iosb: UnsafeCell::new(IO_STATUS_BLOCK::default()),
            info: UnsafeCell::new(PollInfo {
                timeout: 0,
                number_of_handles: 0,
                exclusive: 0,
                handles: [PollHandle { handle: std::ptr::null_mut(), events: 0, status: 0 }],
            }),
        }
    }
}

pub(crate) struct Selector {
    iocp: HANDLE,
    afd: HANDLE,
    /// A high-resolution waitable timer and a wait completion packet that posts to the port
    /// when it fires, so a timed wait ends on time instead of on the next system timer tick
    /// (up to 15.6 ms late). Null if the system lacks them; waits then use millisecond timeouts.
    timer: HANDLE,
    packet: HANDLE,
}

#[link(name = "ntdll", kind = "raw-dylib")]
extern "system" {
    fn NtCreateWaitCompletionPacket(packet: *mut HANDLE, access: u32, attributes: *const std::ffi::c_void) -> NTSTATUS;
    fn NtAssociateWaitCompletionPacket(
        packet: HANDLE,
        iocp: HANDLE,
        target: HANDLE,
        key: *const std::ffi::c_void,
        apc_context: *const std::ffi::c_void,
        status: NTSTATUS,
        information: usize,
        already_signaled: *mut u8,
    ) -> NTSTATUS;
    fn NtCancelWaitCompletionPacket(packet: HANDLE, remove_signaled: u8) -> NTSTATUS;
}

/// Completion key of the timer's packet.
const TIMER_KEY: usize = 2;
const GENERIC_ALL: u32 = 0x1000_0000;

// SAFETY: both handles may be used from any thread.
unsafe impl Send for Selector {}
unsafe impl Sync for Selector {}

impl Drop for Selector {
    fn drop(&mut self) {
        // SAFETY: handles owned by this selector.
        unsafe {
            if !self.packet.is_null() {
                NtCancelWaitCompletionPacket(self.packet, 1);
                CloseHandle(self.packet);
            }
            if !self.timer.is_null() {
                CloseHandle(self.timer);
            }
            CloseHandle(self.afd);
            CloseHandle(self.iocp);
        }
    }
}

/// Buffer for dequeued completions.
pub(crate) struct Events {
    entries: Vec<OVERLAPPED_ENTRY>,
    len: usize,
}

// SAFETY: the entries are plain data filled by the kernel; the pointers in them are only
// dereferenced by the driver that dequeued them.
unsafe impl Send for Events {}

impl Events {
    pub(crate) fn with_capacity(n: usize) -> Events {
        Events { entries: vec![OVERLAPPED_ENTRY::default(); n], len: 0 }
    }
}

fn millis(d: Duration) -> u32 {
    d.as_millis().min(u32::MAX as u128 - 1) as u32 + u32::from(!d.subsec_nanos().is_multiple_of(1_000_000))
}

/// The timer and packet for precise waits (Windows 10 1803 and later), or nulls.
fn precise_timer() -> (HANDLE, HANDLE) {
    // SAFETY: plain object creation; on failure everything made is closed.
    unsafe {
        let timer =
            CreateWaitableTimerExW(std::ptr::null(), std::ptr::null(), CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
        if timer.is_null() {
            return (std::ptr::null_mut(), std::ptr::null_mut());
        }
        let mut packet: HANDLE = std::ptr::null_mut();
        if NtCreateWaitCompletionPacket(&mut packet, GENERIC_ALL, std::ptr::null()) < 0 || packet.is_null() {
            CloseHandle(timer);
            return (std::ptr::null_mut(), std::ptr::null_mut());
        }
        (timer, packet)
    }
}

fn status_error(status: NTSTATUS) -> io::Error {
    // SAFETY: plain conversion.
    let code = unsafe { windows_sys::Win32::Foundation::RtlNtStatusToDosError(status) };
    io::Error::from_raw_os_error(code as i32)
}

impl Selector {
    pub(crate) fn new() -> io::Result<Selector> {
        // SAFETY: Win32/NT calls with valid arguments; handles are closed on failure.
        unsafe {
            let iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0);
            if iocp.is_null() {
                return Err(io::Error::last_os_error());
            }
            // Any name under \Device\Afd opens the driver.
            let name: Vec<u16> = "\\Device\\Afd\\Strand".encode_utf16().collect();
            let us = UNICODE_STRING {
                Length: (name.len() * 2) as u16,
                MaximumLength: (name.len() * 2) as u16,
                Buffer: name.as_ptr() as *mut u16,
            };
            let oa = OBJECT_ATTRIBUTES {
                Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
                RootDirectory: std::ptr::null_mut(),
                ObjectName: &us,
                Attributes: 0,
                SecurityDescriptor: std::ptr::null(),
                SecurityQualityOfService: std::ptr::null(),
            };
            let mut afd: HANDLE = std::ptr::null_mut();
            let mut iosb = IO_STATUS_BLOCK::default();
            let st = NtCreateFile(
                &mut afd,
                SYNCHRONIZE,
                &oa,
                &mut iosb,
                std::ptr::null(),
                0,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                FILE_OPEN,
                0,
                std::ptr::null(),
                0,
            );
            if st < 0 {
                CloseHandle(iocp);
                return Err(status_error(st));
            }
            if CreateIoCompletionPort(afd, iocp, 0, 0).is_null()
                || SetFileCompletionNotificationModes(afd, FILE_SKIP_SET_EVENT_ON_HANDLE) == 0
            {
                let e = io::Error::last_os_error();
                CloseHandle(afd);
                CloseHandle(iocp);
                return Err(e);
            }
            let (timer, packet) = precise_timer();
            Ok(Selector { iocp, afd, timer, packet })
        }
    }

    /// Prepares a socket: AFD must be given the base socket, beneath any layered providers.
    pub(crate) fn register(&self, socket: super::RawSource, io: &Arc<ScheduledIo>) -> io::Result<()> {
        let mut base: SOCKET = 0;
        let mut bytes = 0u32;
        // SAFETY: the output buffer is a SOCKET.
        let r = unsafe {
            WSAIoctl(
                socket as SOCKET,
                SIO_BASE_HANDLE,
                std::ptr::null(),
                0,
                (&mut base as *mut SOCKET).cast(),
                std::mem::size_of::<SOCKET>() as u32,
                &mut bytes,
                std::ptr::null_mut(),
                None,
            )
        };
        if r == SOCKET_ERROR || base == 0 {
            base = socket as SOCKET;
        }
        io.afd.base.store(base, Release);
        Ok(())
    }

    /// Makes sure a poll request covers `dir` while a task waits for it.
    pub(crate) fn arm(&self, io: &Arc<ScheduledIo>, dir: Direction) {
        let events = match dir {
            Direction::Read => READ_EVENTS,
            Direction::Write => WRITE_EVENTS,
        };
        let mut st = io.afd.state.lock().unwrap();
        if st.closed {
            return;
        }
        st.wanted |= events;
        match st.in_flight {
            Some(cur) if cur & st.wanted == st.wanted => {}
            // The pending request lacks these events: cancel it; its completion sends a new one.
            Some(_) => {
                if !st.cancelling {
                    st.cancelling = true;
                    self.cancel(io);
                }
            }
            None => self.submit(io, &mut st),
        }
    }

    fn cancel(&self, io: &Arc<ScheduledIo>) {
        let mut out = IO_STATUS_BLOCK::default();
        // SAFETY: cancels the request identified by its status block; the block stays valid.
        unsafe { NtCancelIoFileEx(self.afd, io.afd.iosb.get(), &mut out) };
    }

    /// Sends a poll request for the wanted events. Called with the state locked and nothing in flight.
    fn submit(&self, io: &Arc<ScheduledIo>, st: &mut PollState) {
        let base = io.afd.base.load(Acquire) as HANDLE;
        // SAFETY: no request is in flight, so the cells are ours; the reference passed as the
        // context keeps them alive until the completion is dequeued.
        unsafe {
            *io.afd.info.get() = PollInfo {
                timeout: i64::MAX,
                number_of_handles: 1,
                exclusive: 0,
                handles: [PollHandle { handle: base, events: st.wanted | AFD_POLL_LOCAL_CLOSE, status: 0 }],
            };
            (*io.afd.iosb.get()).Anonymous.Status = STATUS_PENDING;
            let ctx = Arc::into_raw(io.clone());
            let info = io.afd.info.get();
            let size = std::mem::size_of::<PollInfo>() as u32;
            let status = NtDeviceIoControlFile(
                self.afd,
                std::ptr::null_mut(),
                None,
                ctx.cast(),
                io.afd.iosb.get(),
                IOCTL_AFD_POLL,
                info.cast(),
                size,
                info.cast(),
                size,
            );
            if status == STATUS_PENDING || status >= 0 {
                // Pending, or done at once: either way a completion will be queued.
                st.in_flight = Some(st.wanted);
            } else {
                drop(Arc::from_raw(ctx));
                st.wanted = 0;
                io.set_readiness(ERROR | READ_CLOSED | WRITE_CLOSED);
            }
        }
    }

    /// The socket is about to close: stop polling it.
    pub(crate) fn deregister(&self, io: &Arc<ScheduledIo>) {
        let mut st = io.afd.state.lock().unwrap();
        st.closed = true;
        st.wanted = 0;
        if st.in_flight.is_some() && !st.cancelling {
            st.cancelling = true;
            self.cancel(io);
        }
    }

    /// Waits for completions; `Some(ZERO)` only collects those already queued.
    pub(crate) fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        let mut armed = false;
        let ms = match timeout {
            None => u32::MAX, // INFINITE
            Some(d) if d.is_zero() => 0,
            Some(d) if !self.timer.is_null() => {
                // Relative due time in 100 ns units (negative means relative).
                let due = -((d.as_nanos() / 100).clamp(1, i64::MAX as u128) as i64);
                let mut signaled = 0u8;
                // SAFETY: handles owned by this selector; the packet posts TIMER_KEY to our port.
                armed = unsafe {
                    SetWaitableTimer(self.timer, &due, 0, None, std::ptr::null(), 0) != 0
                        && NtAssociateWaitCompletionPacket(
                            self.packet,
                            self.iocp,
                            self.timer,
                            TIMER_KEY as *const _,
                            std::ptr::null(),
                            0,
                            0,
                            &mut signaled,
                        ) >= 0
                };
                if armed {
                    u32::MAX
                } else {
                    millis(d)
                }
            }
            Some(d) => millis(d),
        };
        let mut n = 0u32;
        // SAFETY: the buffer holds `len` entries.
        let ok = unsafe {
            GetQueuedCompletionStatusEx(self.iocp, events.entries.as_mut_ptr(), events.entries.len() as u32, &mut n, ms, 0)
        };
        if armed {
            // Woken by something else: take the timer's packet back so it cannot end a later wait.
            // SAFETY: our packet handle.
            unsafe { NtCancelWaitCompletionPacket(self.packet, 1) };
        }
        if ok == 0 {
            events.len = 0;
            // SAFETY: reads the thread's last error.
            return if unsafe { GetLastError() } == WAIT_TIMEOUT { Ok(()) } else { Err(io::Error::last_os_error()) };
        }
        events.len = n as usize;
        Ok(())
    }

    /// Turns dequeued completions into readiness, and sends new requests where tasks still wait.
    pub(crate) fn dispatch(&self, events: &Events) {
        for e in &events.entries[..events.len] {
            if e.lpCompletionKey == WAKE_KEY || e.lpCompletionKey == TIMER_KEY || e.lpOverlapped.is_null() {
                continue;
            }
            // SAFETY: the context of every AFD request is a reference made by `submit`.
            let io = unsafe { Arc::from_raw(e.lpOverlapped as *const ScheduledIo) };
            let mut st = io.afd.state.lock().unwrap();
            st.in_flight = None;
            st.cancelling = false;
            // SAFETY: the request completed, so the kernel is done with the cells.
            let (status, info) = unsafe { ((*io.afd.iosb.get()).Anonymous.Status, *io.afd.info.get()) };
            let afd = if status == STATUS_CANCELLED || info.number_of_handles == 0 { 0 } else { info.handles[0].events };
            let mut ready = 0;
            if status < 0 && status != STATUS_CANCELLED {
                ready |= ERROR | READABLE | WRITABLE;
            }
            if afd & (AFD_POLL_RECEIVE | AFD_POLL_ACCEPT) != 0 {
                ready |= READABLE;
            }
            if afd & AFD_POLL_SEND != 0 {
                ready |= WRITABLE;
            }
            if afd & AFD_POLL_DISCONNECT != 0 {
                ready |= READABLE | READ_CLOSED;
            }
            if afd & AFD_POLL_ABORT != 0 {
                ready |= READABLE | WRITABLE | READ_CLOSED | WRITE_CLOSED;
            }
            if afd & AFD_POLL_CONNECT_FAIL != 0 {
                // The error itself is read with SO_ERROR by whoever waits.
                ready |= WRITABLE | ERROR;
            }
            if afd & AFD_POLL_LOCAL_CLOSE != 0 {
                st.closed = true;
            }
            if ready & (READABLE | READ_CLOSED | ERROR) != 0 {
                st.wanted &= !READ_EVENTS;
            }
            if ready & (WRITABLE | WRITE_CLOSED | ERROR) != 0 {
                st.wanted &= !WRITE_EVENTS;
            }
            if !st.closed && st.wanted != 0 {
                self.submit(&io, &mut st);
            }
            drop(st);
            if ready != 0 {
                io.set_readiness(ready);
            }
        }
    }

    /// Makes a blocked `select` return.
    pub(crate) fn wake(&self) {
        // SAFETY: posting a packet to our own port.
        unsafe { PostQueuedCompletionStatus(self.iocp, 0, WAKE_KEY, std::ptr::null()) };
    }
}
