//! Tasks: one allocation that holds the future, then its output, and an atomic state that says
//! who may touch them.
//!
//! The state is a small set of bits. `SCHEDULED` means the task sits in a run queue (so a wake
//! does nothing), `RUNNING` means a worker is polling it (so a wake only sets `NOTIFIED`, and the
//! worker queues it again when the poll returns), and `COMPLETE` means the future is gone and the
//! output waits for the `JoinHandle`. A task is in a queue at most once, and only the thread that
//! set `RUNNING` touches the future.

use std::any::Any;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use crate::waker::AtomicWaker;

const SCHEDULED: usize = 1;
const RUNNING: usize = 2;
const NOTIFIED: usize = 4;
const COMPLETE: usize = 8;
const CANCELLED: usize = 16;

/// What a task needs from the runtime.
pub(crate) trait Schedule: Send + Sync + 'static {
    /// Puts a task in a run queue.
    fn schedule(&self, task: Task);
    /// The task finished; forget it.
    fn release(&self, id: u64);
}

/// A task as the run queues see it.
pub(crate) type Task = Arc<dyn Runnable>;

pub(crate) trait Runnable: Send + Sync + 'static {
    /// Polls the future once. Only called on a task taken from a run queue.
    fn run(self: Arc<Self>);
    /// Drops the future without polling it, at runtime shutdown when no worker is running.
    fn shutdown(&self);
}

enum Stage<F: Future> {
    Running(F),
    Finished(Result<F::Output, JoinError>),
    Consumed,
}

pub(crate) struct Cell<F: Future, S: Schedule> {
    state: AtomicUsize,
    id: u64,
    sched: Arc<S>,
    stage: UnsafeCell<Stage<F>>,
    join_waker: AtomicWaker,
}

// SAFETY: the stage is only touched by the holder of RUNNING, or by the JoinHandle after
// COMPLETE, and the state transitions between them are acquire/release.
unsafe impl<F: Future + Send, S: Schedule> Send for Cell<F, S> where F::Output: Send {}
unsafe impl<F: Future + Send, S: Schedule> Sync for Cell<F, S> where F::Output: Send {}

/// Creates a task in the scheduled state. The caller puts the returned task in a run queue.
pub(crate) fn new<F, S>(future: F, sched: Arc<S>, id: u64) -> (Task, JoinHandle<F::Output>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    let cell = Arc::new(Cell {
        state: AtomicUsize::new(SCHEDULED),
        id,
        sched,
        stage: UnsafeCell::new(Stage::Running(future)),
        join_waker: AtomicWaker::new(),
    });
    let task: Task = cell.clone();
    (task, JoinHandle { raw: Some(cell) })
}

impl<F, S> Cell<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    fn wake_ref(this: &Arc<Self>) {
        let mut s = this.state.load(Acquire);
        loop {
            if s & (COMPLETE | SCHEDULED) != 0 || (s & RUNNING != 0 && s & NOTIFIED != 0) {
                return;
            }
            // While it runs, a wake means "poll it again once this poll returns".
            let next = if s & RUNNING != 0 { s | NOTIFIED } else { s | SCHEDULED };
            match this.state.compare_exchange_weak(s, next, AcqRel, Acquire) {
                Ok(_) => {
                    if next & SCHEDULED != 0 {
                        this.sched.schedule(this.clone());
                    }
                    return;
                }
                Err(actual) => s = actual,
            }
        }
    }

    /// Stores the result and wakes the joiner. The caller holds RUNNING.
    fn finish(&self, out: Result<F::Output, JoinError>) {
        // Drop the future first, while still RUNNING: its destructor may wake tasks, this one
        // included, and those wakes must not queue it again. A panic in a destructor is not
        // allowed to take the worker down with it.
        // SAFETY: RUNNING gives exclusive access to the stage.
        let old = unsafe { std::mem::replace(&mut *self.stage.get(), Stage::Consumed) };
        let _ = catch_unwind(AssertUnwindSafe(move || drop(old)));
        unsafe { *self.stage.get() = Stage::Finished(out) };
        self.state.fetch_update(AcqRel, Acquire, |s| Some((s & !(RUNNING | NOTIFIED | SCHEDULED)) | COMPLETE)).ok();
        self.join_waker.wake();
        self.sched.release(self.id);
    }
}

impl<F, S> Wake for Cell<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    fn wake(self: Arc<Self>) {
        Self::wake_ref(&self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        Self::wake_ref(self);
    }
}

impl<F, S> Runnable for Cell<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    fn run(self: Arc<Self>) {
        let prev = self.state.fetch_update(AcqRel, Acquire, |s| Some((s & !SCHEDULED) | RUNNING)).unwrap();
        debug_assert!(prev & SCHEDULED != 0 && prev & RUNNING == 0, "state {prev:#b}");
        if prev & COMPLETE != 0 {
            return;
        }
        if prev & CANCELLED != 0 {
            self.finish(Err(JoinError::Cancelled));
            return;
        }
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);
        let result = catch_unwind(AssertUnwindSafe(|| {
            // SAFETY: RUNNING gives exclusive access; the cell never moves (it is in an Arc).
            let Stage::Running(fut) = (unsafe { &mut *self.stage.get() }) else { unreachable!("a running task has its future") };
            let fut = unsafe { Pin::new_unchecked(fut) };
            crate::coop::budget(|| fut.poll(&mut cx))
        }));
        match result {
            Ok(Poll::Ready(v)) => self.finish(Ok(v)),
            Err(panic) => self.finish(Err(JoinError::Panic(panic))),
            Ok(Poll::Pending) => {
                let mut s = self.state.load(Acquire);
                loop {
                    if s & CANCELLED != 0 {
                        self.finish(Err(JoinError::Cancelled));
                        return;
                    }
                    let requeue = s & NOTIFIED != 0;
                    let next = if requeue { (s & !(RUNNING | NOTIFIED)) | SCHEDULED } else { s & !RUNNING };
                    match self.state.compare_exchange_weak(s, next, AcqRel, Acquire) {
                        Ok(_) => {
                            if requeue {
                                self.sched.schedule(self.clone());
                            }
                            return;
                        }
                        Err(actual) => s = actual,
                    }
                }
            }
        }
    }

    fn shutdown(&self) {
        // Take RUNNING as a worker would; skip tasks already done (or, impossibly, running).
        let got =
            self.state.fetch_update(AcqRel, Acquire, |s| (s & (RUNNING | COMPLETE) == 0).then_some(s | RUNNING | CANCELLED));
        if got.is_ok() {
            self.finish(Err(JoinError::Cancelled));
        }
    }
}

/// The typed side of a task, seen by its `JoinHandle`.
trait Join<T>: Send + Sync {
    fn poll_join(&self, cx: &mut Context<'_>) -> Poll<Result<T, JoinError>>;
    fn abort(self: Arc<Self>);
    fn is_finished(&self) -> bool;
}

impl<F, S> Join<F::Output> for Cell<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    fn poll_join(&self, cx: &mut Context<'_>) -> Poll<Result<F::Output, JoinError>> {
        if self.state.load(Acquire) & COMPLETE == 0 {
            self.join_waker.register(cx.waker());
            if self.state.load(Acquire) & COMPLETE == 0 {
                return Poll::Pending;
            }
        }
        // SAFETY: after COMPLETE nothing but the (single) JoinHandle touches the stage.
        match std::mem::replace(unsafe { &mut *self.stage.get() }, Stage::Consumed) {
            Stage::Finished(r) => Poll::Ready(r),
            _ => panic!("JoinHandle polled after it returned"),
        }
    }

    fn abort(self: Arc<Self>) {
        let prev = self.state.fetch_or(CANCELLED, AcqRel);
        if prev & (COMPLETE | CANCELLED) == 0 {
            // Wake it so a worker drops the future; if it is running or queued, that happens anyway.
            Self::wake_ref(&self);
        }
    }

    fn is_finished(&self) -> bool {
        self.state.load(Acquire) & COMPLETE != 0
    }
}

/// Waits for a spawned task and gives its output. Dropping it lets the task run on, detached.
pub struct JoinHandle<T> {
    raw: Option<Arc<dyn Join<T>>>,
}

impl<T> JoinHandle<T> {
    /// Stops the task at its next await point (or before it first runs). Awaiting the handle
    /// then gives `JoinError::Cancelled`, unless the task had already finished.
    pub fn abort(&self) {
        if let Some(r) = &self.raw {
            r.clone().abort();
        }
    }

    pub fn is_finished(&self) -> bool {
        self.raw.as_ref().is_none_or(|r| r.is_finished())
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let raw = self.raw.as_ref().expect("JoinHandle polled after it returned");
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let r = raw.poll_join(cx);
        if r.is_ready() {
            crate::coop::spend();
            self.raw = None;
        }
        r
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle").field("finished", &self.is_finished()).finish()
    }
}

/// Why a task gave no output.
pub enum JoinError {
    /// It was aborted, or the runtime shut down first.
    Cancelled,
    /// It panicked; the payload is what `panic!` was given.
    Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
    pub fn is_cancelled(&self) -> bool {
        matches!(self, JoinError::Cancelled)
    }

    pub fn is_panic(&self) -> bool {
        matches!(self, JoinError::Panic(_))
    }

    /// The panic payload; panics if the task was cancelled instead.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self {
            JoinError::Panic(p) => p,
            JoinError::Cancelled => panic!("the task was cancelled, not panicked"),
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Cancelled => f.write_str("task was cancelled"),
            JoinError::Panic(p) => {
                let msg = p.downcast_ref::<&str>().copied().or_else(|| p.downcast_ref::<String>().map(|s| s.as_str()));
                write!(f, "task panicked: {}", msg.unwrap_or("(not a string)"))
            }
        }
    }
}

impl std::error::Error for JoinError {}

#[cfg(test)]
#[path = "task_tests.rs"]
mod tests;
