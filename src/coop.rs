//! Cooperative budget: how many operations a task may complete in one poll before it has to
//! give the worker back.
//!
//! A task that loops over a channel that always has a message, or a socket that always has data,
//! never returns `Pending` on its own, so nothing else would run on its worker (timers included).
//! Each poll of a task starts with a budget of 128; every operation that completes (a message
//! received, bytes read, a sleep finished) spends one unit; when it is gone, operations report
//! `Pending` and wake the task at once, so it goes to the back of the run queue. Outside a task
//! (in `block_on`), there is no budget.

use std::cell::Cell;
use std::task::{Context, Poll};

const BUDGET: u8 = 128;

thread_local! {
    static LEFT: Cell<Option<u8>> = const { Cell::new(None) };
}

/// Runs one poll of a task with a fresh budget.
pub(crate) fn budget<R>(f: impl FnOnce() -> R) -> R {
    struct Reset(Option<u8>);
    impl Drop for Reset {
        fn drop(&mut self) {
            LEFT.with(|b| b.set(self.0));
        }
    }
    let _reset = Reset(LEFT.with(|b| b.replace(Some(BUDGET))));
    f()
}

/// Ready if the task may complete another operation. When the budget is spent, wakes the task
/// (so it is polled again after the others) and returns `Pending`.
pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<()> {
    if LEFT.with(|b| b.get()) == Some(0) {
        cx.waker().wake_by_ref();
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}

/// Spends one unit for an operation that completed.
pub(crate) fn spend() {
    LEFT.with(|b| {
        if let Some(n) = b.get() {
            b.set(Some(n.saturating_sub(1)));
        }
    });
}

/// Budget left in the current poll, or `None` outside a task. For tests.
pub fn remaining() -> Option<u8> {
    LEFT.with(|b| b.get())
}
