//! Tasks, scheduling, cancellation, the cooperative budget and timers.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use strand::sync::{mpsc, oneshot};
use strand::time::{interval, sleep, timeout};
use strand::{spawn, spawn_blocking, yield_now, Builder, Runtime};

fn rt(workers: usize) -> Runtime {
    Builder::new().workers(workers).build().unwrap()
}

#[test]
fn block_on_and_spawn() {
    let rt = rt(4);
    let v = rt.block_on(async {
        let hs: Vec<_> = (0..1000u64).map(|i| spawn(async move { i * 2 })).collect();
        let mut sum = 0;
        for h in hs {
            sum += h.await.unwrap();
        }
        sum
    });
    assert_eq!(v, 999 * 1000);
}

#[test]
fn hundred_thousand_tasks_on_every_worker() {
    let rt = rt(8);
    let count = Arc::new(AtomicUsize::new(0));
    let threads = Arc::new(Mutex::new(std::collections::HashSet::new()));
    rt.block_on(async {
        let hs: Vec<_> = (0..100_000)
            .map(|_| {
                let c = count.clone();
                let t = threads.clone();
                spawn(async move {
                    c.fetch_add(1, SeqCst);
                    t.lock().unwrap().insert(std::thread::current().id());
                    yield_now().await;
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
    });
    assert_eq!(count.load(SeqCst), 100_000);
    assert!(threads.lock().unwrap().len() >= 2, "work was spread over workers");
}

#[test]
fn spawn_from_inside_tasks_chains() {
    fn chain(n: u32) -> strand::JoinHandle<u32> {
        spawn(async move {
            if n == 0 {
                0
            } else {
                chain(n - 1).await.unwrap() + 1
            }
        })
    }
    let rt = rt(4);
    assert_eq!(rt.block_on(async { chain(2000).await.unwrap() }), 2000);
}

#[test]
fn panics_are_returned_and_the_runtime_survives() {
    let rt = rt(2);
    rt.block_on(async {
        let h = spawn(async {
            panic!("boom {}", 7);
        });
        let e = h.await.unwrap_err();
        assert!(e.is_panic());
        assert_eq!(e.to_string(), "task panicked: boom 7");
        assert_eq!(spawn(async { 5 }).await.unwrap(), 5);
    });
}

struct DropFlag(Arc<AtomicUsize>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

#[test]
fn abort_drops_the_future() {
    let rt = rt(2);
    let dropped = Arc::new(AtomicUsize::new(0));
    rt.block_on(async {
        let flag = DropFlag(dropped.clone());
        let h = spawn(async move {
            let _f = flag;
            sleep(Duration::from_secs(3600)).await;
        });
        sleep(Duration::from_millis(20)).await;
        h.abort();
        let e = h.await.unwrap_err();
        assert!(e.is_cancelled());
    });
    assert_eq!(dropped.load(SeqCst), 1);
    // Aborting a finished task changes nothing.
    rt.block_on(async {
        let h = spawn(async { 3 });
        sleep(Duration::from_millis(10)).await;
        h.abort();
        assert_eq!(h.await.unwrap(), 3);
    });
}

#[test]
fn dropping_the_runtime_cancels_pending_tasks() {
    let dropped = Arc::new(AtomicUsize::new(0));
    {
        let rt = rt(2);
        for _ in 0..50 {
            let flag = DropFlag(dropped.clone());
            rt.spawn(async move {
                let _f = flag;
                // Waiting on a socket and a timer: both hold wakers back to the task.
                let l = strand::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let _ = timeout(Duration::from_secs(3600), l.accept()).await;
            });
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(dropped.load(SeqCst), 50);
}

#[test]
fn wake_from_a_plain_thread() {
    let rt = rt(2);
    let got = rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            tx.send(42).unwrap();
        });
        spawn(async move { rx.await.unwrap() }).await.unwrap()
    });
    assert_eq!(got, 42);
}

#[test]
fn oneshot_edges() {
    let rt = rt(1);
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<u8>();
        drop(tx);
        assert!(rx.await.is_err());
        let (tx, rx) = oneshot::channel::<String>();
        drop(rx);
        assert!(tx.is_closed());
        assert_eq!(tx.send("back".into()).unwrap_err(), "back");
    });
}

#[test]
fn bounded_channel_waits_for_room() {
    let rt = rt(4);
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel::<u32>(4);
        let producers: Vec<_> = (0..8)
            .map(|p| {
                let tx = tx.clone();
                spawn(async move {
                    for i in 0..1000 {
                        tx.send(p * 1000 + i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);
        let mut got = Vec::new();
        while let Some(v) = rx.recv().await {
            got.push(v);
        }
        for p in producers {
            p.await.unwrap();
        }
        got.sort();
        assert_eq!(got, (0..8000).collect::<Vec<_>>());
    });
}

#[test]
fn spawn_blocking_leaves_workers_free() {
    let rt = rt(1);
    rt.block_on(async {
        let t = Instant::now();
        let slow = spawn_blocking(|| {
            std::thread::sleep(Duration::from_millis(300));
            7
        });
        // The only worker still runs tasks while the blocking call sleeps.
        let quick = spawn(async { Instant::now() }).await.unwrap();
        assert!(quick.duration_since(t) < Duration::from_millis(200));
        assert_eq!(slow.await.unwrap(), 7);
    });
}

/// With one worker, a task that always has a message ready would hold it forever; the budget
/// makes it yield, so a timer on the same worker fires on time.
#[test]
fn budget_keeps_a_busy_task_from_starving_others() {
    let rt = rt(1);
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded::<u32>();
        for i in 0..2_000_000 {
            tx.send(i).unwrap();
        }
        drop(tx);
        let busy = spawn(async move {
            let mut n = 0u32;
            while rx.recv().await.is_some() {
                n += 1;
            }
            (n, Instant::now())
        });
        let timer = spawn(async {
            sleep(Duration::from_millis(5)).await;
            Instant::now()
        });
        let woke = timer.await.unwrap();
        let (n, done) = busy.await.unwrap();
        assert_eq!(n, 2_000_000);
        assert!(woke < done, "the timer fired while the busy task still had messages");
    });
}

#[test]
fn sleeps_never_early_and_in_order() {
    let rt = rt(4);
    rt.block_on(async {
        let order = Arc::new(Mutex::new(Vec::new()));
        let start = Instant::now();
        let hs: Vec<_> = [50u64, 10, 30, 1, 70, 20]
            .into_iter()
            .map(|ms| {
                let o = order.clone();
                spawn(async move {
                    sleep(Duration::from_millis(ms)).await;
                    let el = start.elapsed();
                    assert!(el >= Duration::from_millis(ms), "{ms} ms sleep woke after {el:?}");
                    o.lock().unwrap().push(ms);
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), [1, 10, 20, 30, 50, 70]);
    });
}

#[test]
fn timeout_and_interval() {
    let rt = rt(2);
    rt.block_on(async {
        assert!(timeout(Duration::from_millis(20), sleep(Duration::from_secs(10))).await.is_err());
        assert_eq!(timeout(Duration::from_secs(10), async { 9 }).await, Ok(9));
        let mut iv = interval(Duration::from_millis(20));
        let t0 = iv.tick().await;
        let t1 = iv.tick().await;
        let t2 = iv.tick().await;
        assert_eq!(t1 - t0, Duration::from_millis(20));
        assert_eq!(t2 - t1, Duration::from_millis(20));
        assert!(Instant::now() >= t2);
    });
}

#[test]
fn many_timers_cancelled_and_fired() {
    let rt = rt(4);
    rt.block_on(async {
        // Half are dropped before firing (a timeout that the work beat), half fire.
        let fired = Arc::new(AtomicUsize::new(0));
        let hs: Vec<_> = (0..20_000u64)
            .map(|i| {
                let f = fired.clone();
                spawn(async move {
                    if i % 2 == 0 {
                        let _ = timeout(Duration::from_secs(60), async {}).await;
                    } else {
                        sleep(Duration::from_millis(i % 50)).await;
                        f.fetch_add(1, SeqCst);
                    }
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
        assert_eq!(fired.load(SeqCst), 10_000);
    });
}
