//! A slot for one waker that one side registers and another side wakes, without a lock.

use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::task::Waker;

const WAITING: usize = 0;
const REGISTERING: usize = 1;
const WAKING: usize = 2;

/// Holds the waker of the task waiting on something (a channel, a socket, a timer, a join).
///
/// `register` and `wake` may race: if a wake arrives while a registration is in progress, the
/// registering side sees it and wakes the new waker itself, so no wake is lost. Two concurrent
/// `register` calls are not allowed (each user has one waiting side).
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: the waker cell is only touched by whoever moved the state out of WAITING.
unsafe impl Send for AtomicWaker {}
unsafe impl Sync for AtomicWaker {}

impl Default for AtomicWaker {
    fn default() -> Self {
        AtomicWaker::new()
    }
}

impl AtomicWaker {
    pub const fn new() -> AtomicWaker {
        AtomicWaker { state: AtomicUsize::new(WAITING), waker: UnsafeCell::new(None) }
    }

    pub fn register(&self, waker: &Waker) {
        match self.state.compare_exchange(WAITING, REGISTERING, Acquire, Acquire) {
            Ok(_) => {
                // SAFETY: REGISTERING gives exclusive access to the cell.
                unsafe {
                    let slot = &mut *self.waker.get();
                    if !slot.as_ref().is_some_and(|w| w.will_wake(waker)) {
                        *slot = Some(waker.clone());
                    }
                }
                // A wake that came in meanwhile set WAKING too; it could not take the waker, so we do.
                if let Err(actual) = self.state.compare_exchange(REGISTERING, WAITING, AcqRel, Acquire) {
                    debug_assert_eq!(actual, REGISTERING | WAKING);
                    // SAFETY: still exclusive until the state is reset below.
                    let w = unsafe { (*self.waker.get()).take() };
                    self.state.swap(WAITING, AcqRel);
                    if let Some(w) = w {
                        w.wake();
                    }
                }
            }
            Err(WAKING) => {
                // Being woken right now: the old waker is gone, so wake the new one directly.
                waker.wake_by_ref();
            }
            Err(_) => {
                // Another register is running; not supported, but harmless: wake to retry.
                waker.wake_by_ref();
            }
        }
    }

    /// Takes the registered waker, if any, for the caller to wake.
    pub fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKING, AcqRel) {
            WAITING => {
                // SAFETY: WAKING gives exclusive access to the cell.
                let w = unsafe { (*self.waker.get()).take() };
                self.state.fetch_and(!WAKING, Release);
                w
            }
            // A registration is in progress (it will see WAKING) or another wake is taking it.
            _ => None,
        }
    }

    pub fn wake(&self) {
        if let Some(w) = self.take() {
            w.wake();
        }
    }
}
