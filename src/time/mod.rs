//! Sleeping, timeouts and intervals, driven by the runtime's timing wheel.

pub(crate) mod wheel;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicU8};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::runtime::Handle;
use crate::waker::AtomicWaker;
use wheel::Wheel;

const PENDING: u8 = 0;
const FIRED: u8 = 1;

/// One registered timer, shared between its `Sleep` and the wheel.
pub(crate) struct TimerEntry {
    deadline: AtomicU64,
    slot: AtomicU16,
    state: AtomicU8,
    waker: AtomicWaker,
}

impl wheel::Entry for TimerEntry {
    fn deadline(&self) -> u64 {
        self.deadline.load(Relaxed)
    }

    fn set_slot(&self, s: Option<(u8, u8)>) {
        self.slot.store(s.map_or(u16::MAX, |(l, s)| (l as u16) << 8 | s as u16), Relaxed);
    }

    fn slot(&self) -> Option<(u8, u8)> {
        let v = self.slot.load(Relaxed);
        (v != u16::MAX).then_some(((v >> 8) as u8, v as u8))
    }
}

impl TimerEntry {
    fn fire(&self) {
        self.state.store(FIRED, Release);
        self.waker.wake();
    }
}

/// The runtime's timers: a wheel in milliseconds since the runtime started.
pub(crate) struct Timers {
    start: Instant,
    wheel: Mutex<Wheel<TimerEntry>>,
    /// When the driver's current wait ends (milliseconds): `u64::MAX` if it waits for I/O only,
    /// 0 if nobody waits in it. A timer due before this must wake it. Changed and read under
    /// the wheel lock.
    wait_until: AtomicU64,
}

impl Timers {
    pub(crate) fn new() -> Timers {
        Timers { start: Instant::now(), wheel: Mutex::new(Wheel::new(0)), wait_until: AtomicU64::new(0) }
    }

    /// Milliseconds since start, rounded down: a timer fires once this reaches its deadline.
    pub(crate) fn now(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// The deadline in wheel milliseconds, rounded up so a timer never fires early.
    fn ms(&self, t: Instant) -> u64 {
        let d = t.saturating_duration_since(self.start);
        d.as_millis() as u64 + u64::from(!d.subsec_nanos().is_multiple_of(1_000_000))
    }

    /// Registers a timer. Returns true if the driver must be woken to wait less.
    fn insert(&self, e: &Arc<TimerEntry>) -> bool {
        let deadline = e.deadline.load(Relaxed);
        let mut w = self.wheel.lock().unwrap();
        match w.insert(e.clone()) {
            Ok(()) => deadline < self.wait_until.load(Acquire),
            Err(e) => {
                drop(w);
                e.fire();
                false
            }
        }
    }

    fn remove(&self, e: &Arc<TimerEntry>) {
        self.wheel.lock().unwrap().remove(e);
    }

    /// How long the driver may block before the next timer is due; `None` if there is none.
    /// Records the end of the wait so an earlier timer added meanwhile can wake the driver.
    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        let w = self.wheel.lock().unwrap();
        match w.next_deadline() {
            Some(d) => {
                self.wait_until.store(d, Release);
                // Exactly until the millisecond the timer is due, not a rounded count of them.
                Some((self.start + Duration::from_millis(d)).saturating_duration_since(Instant::now()))
            }
            None => {
                self.wait_until.store(u64::MAX, Release);
                None
            }
        }
    }

    /// Fires every timer that is due. Called by the driver after each wait.
    pub(crate) fn process(&self) {
        let now = self.now();
        let mut fired = Vec::new();
        {
            let mut w = self.wheel.lock().unwrap();
            w.advance(now, &mut fired);
            self.wait_until.store(0, Release);
        }
        for e in fired {
            e.fire();
        }
    }
}

/// Waits until `deadline`.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep { deadline, entry: None, handle: None }
}

/// Waits for `duration`.
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(Instant::now().checked_add(duration).unwrap_or_else(far_future))
}

fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(86_400 * 365 * 30)
}

/// The future returned by [`sleep`]. Precision is one millisecond plus the operating system's
/// wait granularity; it never completes early.
pub struct Sleep {
    deadline: Instant,
    entry: Option<Arc<TimerEntry>>,
    handle: Option<Handle>,
}

impl Sleep {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn is_elapsed(&self) -> bool {
        self.entry.as_ref().is_some_and(|e| e.state.load(Acquire) == FIRED)
    }

    /// Moves the deadline (used by [`Interval`]); the next poll registers it again.
    pub fn reset(&mut self, deadline: Instant) {
        self.cancel();
        self.deadline = deadline;
    }

    fn cancel(&mut self) {
        if let (Some(e), Some(h)) = (self.entry.take(), &self.handle) {
            if e.state.load(Acquire) == PENDING {
                h.driver().timers().remove(&e);
            }
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let this = &mut *self;
        let handle = this.handle.get_or_insert_with(Handle::current);
        let entry = match &this.entry {
            Some(e) => e.clone(),
            None => {
                let timers = handle.driver().timers();
                let e = Arc::new(TimerEntry {
                    deadline: AtomicU64::new(timers.ms(this.deadline)),
                    slot: AtomicU16::new(u16::MAX),
                    state: AtomicU8::new(PENDING),
                    waker: AtomicWaker::new(),
                });
                e.waker.register(cx.waker());
                if timers.insert(&e) {
                    handle.driver().wake();
                }
                this.entry = Some(e.clone());
                e
            }
        };
        if entry.state.load(Acquire) == FIRED {
            crate::coop::spend();
            return Poll::Ready(());
        }
        entry.waker.register(cx.waker());
        if entry.state.load(Acquire) == FIRED {
            crate::coop::spend();
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep").field("deadline", &self.deadline).finish()
    }
}

/// Returned by [`timeout`] when the time ran out first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

/// Runs `future` for at most `duration`.
pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, Elapsed> {
    let mut future = std::pin::pin!(future);
    let mut sleep = std::pin::pin!(sleep(duration));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(v) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(v));
        }
        sleep.as_mut().poll(cx).map(|_| Err(Elapsed))
    })
    .await
}

/// Ticks every `period`, starting now. Missed ticks are skipped rather than bunched up.
pub fn interval(period: Duration) -> Interval {
    assert!(period > Duration::ZERO, "an interval needs a period above zero");
    Interval { period, next: Instant::now(), sleep: None }
}

pub struct Interval {
    period: Duration,
    next: Instant,
    sleep: Option<Pin<Box<Sleep>>>,
}

impl Interval {
    /// Waits for the next tick and returns its scheduled time.
    pub async fn tick(&mut self) -> Instant {
        let at = self.next;
        let sleep = self.sleep.get_or_insert_with(|| Box::pin(sleep_until(at)));
        if sleep.deadline() != at {
            sleep.as_mut().get_mut().reset(at);
        }
        sleep.as_mut().await;
        let now = Instant::now();
        self.next = at + self.period;
        if self.next <= now {
            // Behind by more than a period: skip ahead instead of firing a burst.
            let behind = now.duration_since(self.next).as_nanos() / self.period.as_nanos() + 1;
            self.next += self.period * behind as u32;
        }
        at
    }
}
