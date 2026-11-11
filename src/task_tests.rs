//! Task state machine tests with a plain queue for a scheduler. They also run under Miri
//! (`cargo +nightly miri test --lib task`) to check the unsafe parts.

use super::*;
use std::collections::VecDeque;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Mutex;

#[derive(Default)]
struct Sched {
    queue: Mutex<VecDeque<Task>>,
    released: AtomicUsize,
}

impl Schedule for Sched {
    fn schedule(&self, task: Task) {
        self.queue.lock().unwrap().push_back(task);
    }
    fn release(&self, _id: u64) {
        self.released.fetch_add(1, SeqCst);
    }
}

impl Sched {
    fn run_all(&self) -> usize {
        let mut n = 0;
        loop {
            let t = self.queue.lock().unwrap().pop_front();
            let Some(t) = t else { return n };
            t.run();
            n += 1;
        }
    }
    fn queued(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, SeqCst);
    }
}

fn poll_join<T>(h: &mut JoinHandle<T>, wakes: &Arc<Count>) -> Poll<Result<T, JoinError>> {
    let w = Waker::from(wakes.clone());
    Pin::new(h).poll(&mut Context::from_waker(&w))
}

struct Flag(Arc<AtomicUsize>);

impl Drop for Flag {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

#[test]
fn completes_and_joins() {
    let s = Arc::new(Sched::default());
    let (t, mut h) = new(async { 40 + 2 }, s.clone(), 1);
    s.schedule(t);
    let wakes = Arc::new(Count::default());
    assert!(poll_join(&mut h, &wakes).is_pending());
    assert_eq!(s.run_all(), 1);
    assert_eq!(wakes.0.load(SeqCst), 1, "the joiner was woken");
    assert!(matches!(poll_join(&mut h, &wakes), Poll::Ready(Ok(42))));
    assert_eq!(s.released.load(SeqCst), 1);
}

#[test]
fn woken_from_another_thread() {
    let s = Arc::new(Sched::default());
    let (tx, rx) = crate::sync::oneshot::channel::<u32>();
    let (t, mut h) = new(async move { rx.await.unwrap() * 2 }, s.clone(), 1);
    s.schedule(t);
    s.run_all();
    assert_eq!(s.queued(), 0, "waiting, not queued");
    std::thread::spawn(move || tx.send(21).unwrap()).join().unwrap();
    assert_eq!(s.queued(), 1);
    s.run_all();
    assert!(matches!(poll_join(&mut h, &Arc::new(Count::default())), Poll::Ready(Ok(42))));
}

#[test]
fn a_wake_during_the_poll_queues_it_again_once() {
    let s = Arc::new(Sched::default());
    let mut polls = 0;
    let fut = std::future::poll_fn(move |cx| {
        polls += 1;
        if polls < 3 {
            cx.waker().wake_by_ref();
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(polls)
        }
    });
    let (t, mut h) = new(fut, s.clone(), 1);
    s.schedule(t);
    assert_eq!(s.run_all(), 3, "two wakes while running queue it once each time");
    assert!(matches!(poll_join(&mut h, &Arc::new(Count::default())), Poll::Ready(Ok(3))));
}

#[test]
fn many_threads_wake_it_but_it_is_queued_once() {
    let s = Arc::new(Sched::default());
    let (wtx, wrx) = std::sync::mpsc::channel::<Waker>();
    let mut sent = false;
    let fut = std::future::poll_fn(move |cx| {
        if !sent {
            sent = true;
            wtx.send(cx.waker().clone()).unwrap();
            return Poll::Pending;
        }
        Poll::Ready(())
    });
    let (t, _h) = new(fut, s.clone(), 1);
    s.schedule(t);
    s.run_all();
    let w = wrx.recv().unwrap();
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let w = w.clone();
            std::thread::spawn(move || w.wake_by_ref())
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(s.queued(), 1);
    s.run_all();
    w.wake();
    assert_eq!(s.queued(), 0, "a finished task is not queued");
}

#[test]
fn abort_while_waiting_and_after_finishing() {
    let s = Arc::new(Sched::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let f = Flag(dropped.clone());
    let (t, mut h) = new(
        async move {
            let _f = f;
            std::future::pending::<()>().await
        },
        s.clone(),
        1,
    );
    s.schedule(t);
    s.run_all();
    h.abort();
    s.run_all();
    assert_eq!(dropped.load(SeqCst), 1);
    assert!(matches!(poll_join(&mut h, &Arc::new(Count::default())), Poll::Ready(Err(JoinError::Cancelled))));

    let (t, mut h) = new(async { 1 }, s.clone(), 2);
    s.schedule(t);
    s.run_all();
    h.abort();
    assert_eq!(s.queued(), 0);
    assert!(matches!(poll_join(&mut h, &Arc::new(Count::default())), Poll::Ready(Ok(1))));
}

#[test]
fn abort_before_first_run() {
    let s = Arc::new(Sched::default());
    let (t, mut h) = new(async { unreachable!("never polled") }, s.clone(), 1);
    s.schedule(t);
    h.abort();
    s.run_all();
    assert!(matches!(poll_join::<()>(&mut h, &Arc::new(Count::default())), Poll::Ready(Err(JoinError::Cancelled))));
}

#[test]
fn panic_is_caught() {
    let s = Arc::new(Sched::default());
    let (t, mut h) = new(async { panic!("inside") }, s.clone(), 1);
    s.schedule(t);
    s.run_all();
    match poll_join::<()>(&mut h, &Arc::new(Count::default())) {
        Poll::Ready(Err(e)) => assert_eq!(e.to_string(), "task panicked: inside"),
        _ => panic!("expected a panic error"),
    }
}

#[test]
fn output_of_a_detached_task_is_dropped_with_it() {
    let s = Arc::new(Sched::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let f = Flag(dropped.clone());
    let (t, h) = new(async move { f }, s.clone(), 1);
    drop(h);
    s.schedule(t);
    s.run_all();
    assert_eq!(dropped.load(SeqCst), 1, "the output is freed when the last reference goes");
}

#[test]
fn shutdown_drops_pending_futures() {
    let s = Arc::new(Sched::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let f = Flag(dropped.clone());
    let (t, mut h) = new(
        async move {
            let _f = f;
            std::future::pending::<()>().await
        },
        s.clone(),
        1,
    );
    s.schedule(t.clone());
    s.run_all();
    t.shutdown();
    assert_eq!(dropped.load(SeqCst), 1);
    assert!(matches!(poll_join(&mut h, &Arc::new(Count::default())), Poll::Ready(Err(JoinError::Cancelled))));
}

#[test]
fn atomic_waker_loses_no_wake() {
    for _ in 0..50 {
        let aw = Arc::new(AtomicWaker::new());
        let count = Arc::new(Count::default());
        let flag = Arc::new(AtomicBool::new(false));
        let (a, f) = (aw.clone(), flag.clone());
        let waker_thread = std::thread::spawn(move || {
            f.store(true, SeqCst);
            a.wake();
        });
        // Register, then check the flag: either we see it, or the wake finds our waker.
        let w = Waker::from(count.clone());
        aw.register(&w);
        let seen = flag.load(SeqCst);
        waker_thread.join().unwrap();
        assert!(seen || count.0.load(SeqCst) == 1);
    }
}
