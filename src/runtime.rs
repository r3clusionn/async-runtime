//! The multi-threaded scheduler.
//!
//! Each worker thread has a run queue of its own and a one-task "LIFO slot". A task woken by a
//! worker goes into that worker's slot and runs next, which keeps message passing between two
//! tasks on one thread instead of bouncing between threads; the task it displaces goes to the
//! back of the local queue. Tasks woken from outside the runtime go to a shared injector queue.
//! An idle worker steals half of another worker's queue.
//!
//! When there is no work, one worker blocks in the I/O driver (with a timeout for the next
//! timer) and the others sleep on a condition variable. A worker waking another to help is
//! throttled: while one woken worker is still searching for work, no further worker is woken.

use std::cell::{Cell as StdCell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use crate::driver::Driver;
use crate::queue::LocalQueue;
use crate::task::{self, JoinHandle, Runnable, Schedule, Task};

/// Tasks polled between looks at the injector queue and the I/O driver, so neither starves
/// while the local queue never empties.
const CHECK_EVERY: u32 = 61;
/// A task woken into the LIFO slot runs ahead of the queue at most this many times in a row.
const MAX_LIFO_RUNS: u32 = 3;
/// Most tasks a worker moves from the injector to its own queue at once.
const INJECTOR_BATCH: usize = 64;
const REGISTRY_SHARDS: usize = 64;

/// One shard of the set of live tasks, cancelled at shutdown.
type Registry = Mutex<HashMap<u64, Weak<dyn Runnable>>>;

/// Configures and starts a runtime.
pub struct Builder {
    workers: usize,
    name: String,
}

impl Default for Builder {
    fn default() -> Self {
        Builder::new()
    }
}

impl Builder {
    pub fn new() -> Builder {
        Builder { workers: thread::available_parallelism().map_or(4, |n| n.get()), name: "strand-worker".into() }
    }

    /// Number of worker threads (default: one per logical CPU).
    pub fn workers(mut self, n: usize) -> Builder {
        self.workers = n.max(1);
        self
    }

    pub fn thread_name(mut self, name: impl Into<String>) -> Builder {
        self.name = name.into();
        self
    }

    pub fn build(self) -> io::Result<Runtime> {
        let driver = Driver::new()?;
        let n = self.workers;
        let shared = Arc::new(Shared {
            id: NEXT_RUNTIME.fetch_add(1, Relaxed),
            injector: Mutex::new(VecDeque::new()),
            injector_len: AtomicUsize::new(0),
            locals: (0..n).map(|_| LocalQueue::new()).collect(),
            parkers: (0..n).map(|_| Parker::new()).collect(),
            sleepers: Mutex::new(Vec::with_capacity(n)),
            num_sleeping: AtomicUsize::new(0),
            num_searching: AtomicUsize::new(0),
            driver,
            shutdown: AtomicBool::new(false),
            registry: (0..REGISTRY_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
            next_task: AtomicU64::new(1),
            blocking: Blocking::new(format!("{}-blocking", self.name)),
        });
        let mut threads = Vec::with_capacity(n);
        for i in 0..n {
            let s = shared.clone();
            let t = thread::Builder::new().name(format!("{}-{i}", self.name)).spawn(move || run_worker(s, i))?;
            threads.push(t);
        }
        Ok(Runtime { handle: Handle { shared }, threads })
    }
}

static NEXT_RUNTIME: AtomicU64 = AtomicU64::new(1);

/// A running set of worker threads, an I/O driver and timers. Dropping it stops the workers and
/// cancels every task that is still pending.
pub struct Runtime {
    handle: Handle,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Runtime {
    /// A runtime with one worker per logical CPU.
    pub fn new() -> io::Result<Runtime> {
        Builder::new().build()
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Runs `future` on the calling thread until it completes; tasks it spawns run on the workers.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _enter = enter(&self.handle, None);
        let unparker = Arc::new(ThreadUnpark(thread::current()));
        let waker = Waker::from(unparker);
        let mut cx = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(v) = future.as_mut().poll(&mut cx) {
                return v;
            }
            thread::park();
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let s = &self.handle.shared;
        s.shutdown.store(true, SeqCst);
        for p in s.parkers.iter() {
            p.unpark(&s.driver);
        }
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
        // No worker runs now. Drop every pending future: they may hold sockets and timers whose
        // wakers point back at their own task, a cycle only this breaks.
        let tasks: Vec<Task> = s
            .registry
            .iter()
            .flat_map(|shard| shard.lock().unwrap().drain().filter_map(|(_, w)| w.upgrade()).collect::<Vec<_>>())
            .collect();
        for t in &tasks {
            t.shutdown();
        }
        drop(tasks);
        s.injector.lock().unwrap().clear();
        for l in s.locals.iter() {
            // The workers have exited, so this thread may act as each queue's owner.
            while l.pop().is_some() {}
        }
        s.blocking.shutdown();
    }
}

struct ThreadUnpark(thread::Thread);

impl Wake for ThreadUnpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// A cheap, cloneable reference to a runtime, for spawning from anywhere.
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

impl Handle {
    /// The runtime of the calling thread (a worker, a blocking thread, or inside `block_on`).
    ///
    /// # Panics
    ///
    /// Outside a runtime.
    pub fn current() -> Handle {
        Handle::try_current().expect("not inside a strand runtime (use Runtime::block_on or a task)")
    }

    pub fn try_current() -> Option<Handle> {
        CONTEXT.with(|c| c.borrow().as_ref().map(|c| c.handle.clone()))
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let s = &self.shared;
        let id = s.next_task.fetch_add(1, Relaxed);
        let (task, handle) = task::new(future, s.clone(), id);
        s.registry[id as usize % REGISTRY_SHARDS].lock().unwrap().insert(id, Arc::downgrade(&task));
        // Like a woken task, a new one goes into the LIFO slot (as in tokio): a task that
        // spawns another and then waits for it hands over without a trip through the queue.
        s.push(task, true);
        handle
    }

    /// Runs a blocking function on a separate pool of threads, so it does not hold up a worker.
    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let pool = self.shared.blocking.clone();
        let handle = self.clone();
        let mut f = Some(f);
        let (task, join) = task::new(
            std::future::poll_fn(move |_| {
                let _enter = enter(&handle, None);
                Poll::Ready((f.take().expect("blocking task polled twice"))())
            }),
            pool.clone(),
            0,
        );
        pool.schedule(task);
        join
    }

    pub(crate) fn driver(&self) -> &Driver {
        &self.shared.driver
    }
}

/// Spawns a task on the current runtime.
///
/// # Panics
///
/// Outside a runtime.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Handle::current().spawn(future)
}

/// Runs a blocking function on the current runtime's blocking pool.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    Handle::current().spawn_blocking(f)
}

/// Lets other tasks run: returns `Pending` once, after waking itself.
pub async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        // Not into the LIFO slot: a yield should go behind the other tasks.
        YIELDING.with(|y| y.set(true));
        cx.waker().wake_by_ref();
        YIELDING.with(|y| y.set(false));
        Poll::Pending
    })
    .await
}

struct ThreadContext {
    handle: Handle,
    /// The worker index, on a worker thread of this runtime.
    worker: Option<usize>,
}

thread_local! {
    static CONTEXT: RefCell<Option<ThreadContext>> = const { RefCell::new(None) };
    /// The worker's LIFO slot: the task most recently woken on this worker.
    static LIFO: RefCell<Option<Task>> = const { RefCell::new(None) };
    static YIELDING: StdCell<bool> = const { StdCell::new(false) };
}

pub(crate) struct EnterGuard(Option<ThreadContext>);

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CONTEXT.with(|c| *c.borrow_mut() = self.0.take());
    }
}

pub(crate) fn enter(handle: &Handle, worker: Option<usize>) -> EnterGuard {
    EnterGuard(CONTEXT.with(|c| c.borrow_mut().replace(ThreadContext { handle: handle.clone(), worker })))
}

pub(crate) struct Shared {
    id: u64,
    injector: Mutex<VecDeque<Task>>,
    injector_len: AtomicUsize,
    locals: Box<[LocalQueue<Task>]>,
    parkers: Box<[Parker]>,
    sleepers: Mutex<Vec<usize>>,
    num_sleeping: AtomicUsize,
    num_searching: AtomicUsize,
    pub(crate) driver: Driver,
    shutdown: AtomicBool,
    registry: Box<[Registry]>,
    next_task: AtomicU64,
    blocking: Arc<Blocking>,
}

impl Shared {
    /// The worker index if the calling thread is one of this runtime's workers.
    fn current_worker(&self) -> Option<usize> {
        CONTEXT.with(|c| c.borrow().as_ref().filter(|c| c.handle.shared.id == self.id).and_then(|c| c.worker))
    }

    /// Worker `i` only.
    fn push_local(&self, i: usize, task: Task) {
        self.locals[i].push_back(task, &mut |batch| self.inject_batch(batch));
    }

    /// Worker `i` only.
    fn pop_local(&self, i: usize) -> Option<Task> {
        self.locals[i].pop()
    }

    fn inject_batch(&self, batch: Vec<Task>) {
        let mut q = self.injector.lock().unwrap();
        q.extend(batch);
        self.injector_len.store(q.len(), SeqCst);
    }

    /// Takes a task from the injector for worker `me`, and moves a fair share of the rest into
    /// its local queue, so the workers do not take the injector's lock once per task.
    fn pop_injector(&self, me: usize) -> Option<Task> {
        if self.injector_len.load(Acquire) == 0 {
            return None;
        }
        let mut batch = {
            // A worker never waits for this lock: if it is held, by a spawning thread or
            // another worker, the work is not lost (a worker checks for it before sleeping),
            // and queueing up behind it is what makes spawning from outside slow.
            let Ok(mut q) = self.injector.try_lock() else { return None };
            let n = (q.len() / self.locals.len() + 1).min(INJECTOR_BATCH).min(q.len());
            // Only the batch moves; what stays in the injector is not copied.
            let batch: Vec<Task> = q.drain(..n).collect();
            self.injector_len.store(q.len(), Release);
            batch.into_iter()
        };
        let first = batch.next();
        for t in batch {
            self.push_local(me, t);
        }
        first
    }

    /// Takes half of another worker's queue into ours (worker `me` only) and returns one task.
    fn steal(&self, me: usize, start: usize) -> Option<Task> {
        let n = self.locals.len();
        for k in 0..n {
            let victim = (start + k) % n;
            if victim == me || self.locals[victim].is_empty() {
                continue;
            }
            if let Some(t) = self.locals[victim].steal_into(&self.locals[me]) {
                return Some(t);
            }
        }
        None
    }

    fn has_work(&self) -> bool {
        self.injector_len.load(SeqCst) > 0 || self.locals.iter().any(|l| !l.is_empty())
    }

    /// Wakes one sleeping worker, unless one is already awake and looking for work.
    fn notify_one(&self) {
        if self.num_sleeping.load(SeqCst) == 0 || self.num_searching.load(SeqCst) > 0 {
            return;
        }
        let i = {
            let mut s = self.sleepers.lock().unwrap();
            // Checked again under the lock, which every change of the sleeper set holds.
            if self.num_searching.load(SeqCst) > 0 {
                return;
            }
            let Some(i) = s.pop() else { return };
            self.num_sleeping.fetch_sub(1, SeqCst);
            self.num_searching.fetch_add(1, SeqCst);
            i
        };
        self.parkers[i].unpark(&self.driver);
    }
}

impl Shared {
    /// Queues a task: on one of this runtime's workers, into its LIFO slot (`lifo`) or local
    /// queue; elsewhere, into the injector. Wakes a sleeping worker if one may be needed.
    fn push(&self, task: Task, lifo: bool) {
        if self.shutdown.load(Acquire) {
            return;
        }
        match self.current_worker() {
            Some(i) => {
                if lifo && !YIELDING.with(|y| y.get()) {
                    match LIFO.with(|l| l.borrow_mut().replace(task)) {
                        Some(prev) => self.push_local(i, prev),
                        // Only the LIFO slot changed; this worker runs it next.
                        None => return,
                    }
                } else {
                    self.push_local(i, task);
                }
                self.notify_one();
            }
            None => {
                let mut q = self.injector.lock().unwrap();
                q.push_back(task);
                self.injector_len.store(q.len(), SeqCst);
                drop(q);
                self.notify_one();
            }
        }
    }
}

impl Schedule for Shared {
    fn schedule(&self, task: Task) {
        self.push(task, true);
    }

    fn release(&self, id: u64) {
        self.registry[id as usize % REGISTRY_SHARDS].lock().unwrap().remove(&id);
    }
}

fn run_worker(shared: Arc<Shared>, me: usize) {
    let handle = Handle { shared: shared.clone() };
    let _enter = enter(&handle, Some(me));
    let s = &*shared;
    let mut tick: u32 = 0;
    let mut lifo_runs = 0;
    let mut searching = false;
    let mut seed = (me as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    while !s.shutdown.load(Acquire) {
        tick = tick.wrapping_add(1);
        let mut task = None;
        if tick.is_multiple_of(CHECK_EVERY) {
            // Fairness: the injector first now and then, and I/O and timers even when busy.
            if let Some(mut core) = s.driver.try_lock() {
                s.driver.turn(&mut core, Some(Duration::ZERO));
            }
            task = s.pop_injector(me);
        }
        if task.is_none() && lifo_runs < MAX_LIFO_RUNS {
            task = LIFO.with(|l| l.borrow_mut().take());
            if task.is_some() {
                lifo_runs += 1;
            }
        }
        if task.is_none() {
            lifo_runs = 0;
            if let Some(t) = LIFO.with(|l| l.borrow_mut().take()) {
                // The LIFO slot ran too often in a row; send its task behind the others.
                s.push_local(me, t);
            }
            task = s.pop_local(me).or_else(|| s.pop_injector(me)).or_else(|| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                s.steal(me, seed as usize % s.locals.len())
            });
        }
        match task {
            Some(t) => {
                if searching {
                    searching = false;
                    // The last searcher found work: if there is more, wake another to look.
                    if s.num_searching.fetch_sub(1, SeqCst) == 1 && s.has_work() {
                        s.notify_one();
                    }
                }
                t.run();
            }
            None => {
                if searching {
                    s.num_searching.fetch_sub(1, SeqCst);
                }
                searching = park(s, me);
            }
        }
    }
    LIFO.with(|l| l.borrow_mut().take());
}

/// Sleeps until woken; returns whether the wake came from `notify_one` (the worker is then a
/// "searcher" until it finds work or sleeps again).
fn park(s: &Shared, me: usize) -> bool {
    {
        let mut sl = s.sleepers.lock().unwrap();
        sl.push(me);
        s.num_sleeping.fetch_add(1, SeqCst);
    }
    // Work pushed before we were listed as sleeping found nobody to wake; look once more.
    let mut in_driver = false;
    if !s.has_work() && !s.shutdown.load(SeqCst) {
        in_driver = s.parkers[me].park(&s.driver);
    }
    let mut sl = s.sleepers.lock().unwrap();
    match sl.iter().position(|&w| w == me) {
        Some(pos) => {
            // Woken by something else (I/O, a timer, a spurious wake): still listed, so not a searcher.
            sl.swap_remove(pos);
            s.num_sleeping.fetch_sub(1, SeqCst);
            drop(sl);
            // Leaving the driver with work to do: wake a sleeper, which takes over the driver
            // when it finds nothing, so timers and I/O keep being served while we run tasks.
            if in_driver && s.has_work() {
                s.notify_one();
            }
            false
        }
        None => true,
    }
}

const EMPTY: usize = 0;
const PARKED_CONDVAR: usize = 1;
const PARKED_DRIVER: usize = 2;
const NOTIFIED: usize = 3;

/// Puts one worker to sleep, either in the I/O driver (if no other worker is in it) or on a
/// condition variable, and wakes it the right way for where it sleeps.
struct Parker {
    state: AtomicUsize,
    lock: Mutex<()>,
    cv: Condvar,
}

impl Parker {
    fn new() -> Parker {
        Parker { state: AtomicUsize::new(EMPTY), lock: Mutex::new(()), cv: Condvar::new() }
    }

    /// Returns true if it waited in the driver.
    fn park(&self, driver: &Driver) -> bool {
        if self.state.compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst).is_ok() {
            return false;
        }
        if let Some(mut core) = driver.try_lock() {
            if self.state.compare_exchange(EMPTY, PARKED_DRIVER, SeqCst, SeqCst).is_err() {
                self.state.store(EMPTY, SeqCst);
                return false;
            }
            let timeout = driver.next_timeout();
            driver.turn(&mut core, timeout);
            self.state.store(EMPTY, SeqCst);
            return true;
        }
        let mut g = self.lock.lock().unwrap();
        if self.state.compare_exchange(EMPTY, PARKED_CONDVAR, SeqCst, SeqCst).is_err() {
            self.state.store(EMPTY, SeqCst);
            return false;
        }
        loop {
            g = self.cv.wait(g).unwrap();
            if self.state.compare_exchange(NOTIFIED, EMPTY, SeqCst, SeqCst).is_ok() {
                return false;
            }
        }
    }

    fn unpark(&self, driver: &Driver) {
        match self.state.swap(NOTIFIED, SeqCst) {
            PARKED_CONDVAR => {
                // Taking the lock orders this with the parker's check before it waits.
                drop(self.lock.lock().unwrap());
                self.cv.notify_one();
            }
            PARKED_DRIVER => driver.wake(),
            _ => {}
        }
    }
}

/// Threads for blocking calls: started on demand, kept for ten seconds when idle.
pub(crate) struct Blocking {
    /// Each thread holds the pool through this, so the pool outlives its threads.
    me: Weak<Blocking>,
    name: String,
    queue: Mutex<BlockingQueue>,
    cv: Condvar,
}

struct BlockingQueue {
    tasks: VecDeque<Task>,
    idle: usize,
    threads: usize,
    shutdown: bool,
}

const BLOCKING_MAX: usize = 512;
const BLOCKING_KEEP: Duration = Duration::from_secs(10);

impl Blocking {
    fn new(name: String) -> Arc<Blocking> {
        Arc::new_cyclic(|me| Blocking {
            me: me.clone(),
            name,
            queue: Mutex::new(BlockingQueue { tasks: VecDeque::new(), idle: 0, threads: 0, shutdown: false }),
            cv: Condvar::new(),
        })
    }

    fn shutdown(&self) {
        let mut q = self.queue.lock().unwrap();
        q.shutdown = true;
        q.tasks.clear();
        self.cv.notify_all();
    }

    fn work(self: Arc<Self>) {
        let mut q = self.queue.lock().unwrap();
        loop {
            if let Some(t) = q.tasks.pop_front() {
                drop(q);
                t.run();
                q = self.queue.lock().unwrap();
                continue;
            }
            if q.shutdown {
                break;
            }
            q.idle += 1;
            let (g, timeout) = self.cv.wait_timeout(q, BLOCKING_KEEP).unwrap();
            q = g;
            q.idle -= 1;
            if timeout.timed_out() && q.tasks.is_empty() {
                break;
            }
        }
        q.threads -= 1;
    }
}

impl Schedule for Blocking {
    fn schedule(&self, task: Task) {
        let mut q = self.queue.lock().unwrap();
        if q.shutdown {
            return;
        }
        q.tasks.push_back(task);
        if q.idle > 0 {
            self.cv.notify_one();
            return;
        }
        if q.threads >= BLOCKING_MAX {
            return; // queued; the next free thread takes it
        }
        let Some(pool) = self.me.upgrade() else { return };
        q.threads += 1;
        drop(q);
        if thread::Builder::new().name(self.name.clone()).spawn(move || pool.work()).is_err() {
            self.queue.lock().unwrap().threads -= 1;
        }
    }

    fn release(&self, _id: u64) {}
}
