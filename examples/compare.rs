//! The same workloads on strand and on tokio's multi-threaded runtime, alternating, with the
//! same number of workers. Prints the median of several runs of each.
//!
//!     cargo run --release --example compare -- [WORKERS] [RUNS]

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SPAWN: usize = 100_000;
const CHAIN: usize = 10_000;
const PAIRS: usize = 100;
const ROUNDS: usize = 1_000;
const YIELDERS: usize = 1_000;
const YIELDS: usize = 1_000;
const TIMERS: usize = 100_000;
const STEADY_TASKS: usize = 1_000;
const STEADY_SLEEPS: usize = 20;
const CONNS: usize = 64;
const ECHOS: usize = 2_000;
const BULK: usize = 1 << 30;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn lateness_stats(mut v: Vec<f64>) -> (f64, f64) {
    v.sort_by(f64::total_cmp);
    (v[v.len() / 2], v[v.len() * 99 / 100])
}

mod s {
    use super::*;
    use strand::net::{TcpListener, TcpStream};
    use strand::sync::mpsc;

    pub fn rt(n: usize) -> strand::Runtime {
        strand::Builder::new().workers(n).build().unwrap()
    }

    pub async fn spawn_many() {
        let hs: Vec<_> = (0..SPAWN).map(|i| strand::spawn(async move { i })).collect();
        for h in hs {
            std::hint::black_box(h.await.unwrap());
        }
    }

    fn chain(n: usize, done: Arc<AtomicUsize>) {
        strand::spawn(async move {
            if n == 0 {
                done.store(1, Relaxed);
            } else {
                chain(n - 1, done);
            }
        });
    }

    pub async fn chained_spawn() {
        let done = Arc::new(AtomicUsize::new(0));
        chain(CHAIN, done.clone());
        while done.load(Relaxed) == 0 {
            strand::yield_now().await;
        }
    }

    pub async fn ping_pong() {
        let hs: Vec<_> = (0..PAIRS)
            .map(|_| {
                strand::spawn(async {
                    let (ping_tx, mut ping_rx) = mpsc::channel::<u32>(1);
                    let (pong_tx, mut pong_rx) = mpsc::channel::<u32>(1);
                    let other = strand::spawn(async move {
                        while let Some(v) = ping_rx.recv().await {
                            pong_tx.send(v).await.unwrap();
                        }
                    });
                    for i in 0..ROUNDS as u32 {
                        ping_tx.send(i).await.unwrap();
                        assert_eq!(pong_rx.recv().await, Some(i));
                    }
                    drop(ping_tx);
                    other.await.unwrap();
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
    }

    pub async fn yield_many() {
        let hs: Vec<_> = (0..YIELDERS)
            .map(|_| {
                strand::spawn(async {
                    for _ in 0..YIELDS {
                        strand::yield_now().await;
                    }
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
    }

    pub async fn timers() -> Vec<f64> {
        let hs: Vec<_> = (0..TIMERS)
            .map(|i| {
                strand::spawn(async move {
                    let want = Duration::from_millis(1 + (i * 7919 % 100) as u64);
                    let t = Instant::now();
                    strand::time::sleep(want).await;
                    (t.elapsed() - want).as_secs_f64() * 1000.0
                })
            })
            .collect();
        let mut late = Vec::with_capacity(TIMERS);
        for h in hs {
            late.push(h.await.unwrap());
        }
        late
    }

    pub async fn steady_timers() -> Vec<f64> {
        let hs: Vec<_> = (0..STEADY_TASKS)
            .map(|i| {
                strand::spawn(async move {
                    let mut late = Vec::with_capacity(STEADY_SLEEPS);
                    for k in 0..STEADY_SLEEPS {
                        let want = Duration::from_millis(1 + ((i * 31 + k * 7) % 20) as u64);
                        let t = Instant::now();
                        strand::time::sleep(want).await;
                        late.push((t.elapsed() - want).as_secs_f64() * 1000.0);
                    }
                    late
                })
            })
            .collect();
        let mut all = Vec::new();
        for h in hs {
            all.extend(h.await.unwrap());
        }
        all
    }

    pub async fn echo() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let server = strand::spawn(async move {
            for _ in 0..CONNS {
                let (c, _) = l.accept().await.unwrap();
                c.set_nodelay(true).unwrap();
                strand::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match c.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => c.write_all(&buf[..n]).await.unwrap(),
                        }
                    }
                });
            }
        });
        let clients: Vec<_> = (0..CONNS)
            .map(|_| {
                strand::spawn(async move {
                    let c = TcpStream::connect(addr).await.unwrap();
                    c.set_nodelay(true).unwrap();
                    let msg = [7u8; 64];
                    let mut back = [0u8; 64];
                    for _ in 0..ECHOS {
                        c.write_all(&msg).await.unwrap();
                        c.read_exact(&mut back).await.unwrap();
                    }
                })
            })
            .collect();
        for c in clients {
            c.await.unwrap();
        }
        server.await.unwrap();
    }

    pub async fn bulk() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let tx = strand::spawn(async move {
            let (c, _) = l.accept().await.unwrap();
            let chunk = vec![1u8; 256 << 10];
            let mut sent = 0;
            while sent < BULK {
                c.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
        });
        let c = TcpStream::connect(addr).await.unwrap();
        let mut buf = vec![0u8; 256 << 10];
        let mut got = 0;
        while got < BULK {
            got += c.read(&mut buf).await.unwrap();
        }
        tx.await.unwrap();
    }
}

mod t {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    pub fn rt(n: usize) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(n).enable_all().build().unwrap()
    }

    pub async fn spawn_many() {
        let hs: Vec<_> = (0..SPAWN).map(|i| tokio::spawn(async move { i })).collect();
        for h in hs {
            std::hint::black_box(h.await.unwrap());
        }
    }

    fn chain(n: usize, done: Arc<AtomicUsize>) {
        tokio::spawn(async move {
            if n == 0 {
                done.store(1, Relaxed);
            } else {
                chain(n - 1, done);
            }
        });
    }

    pub async fn chained_spawn() {
        let done = Arc::new(AtomicUsize::new(0));
        chain(CHAIN, done.clone());
        while done.load(Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    }

    pub async fn ping_pong() {
        let hs: Vec<_> = (0..PAIRS)
            .map(|_| {
                tokio::spawn(async {
                    let (ping_tx, mut ping_rx) = mpsc::channel::<u32>(1);
                    let (pong_tx, mut pong_rx) = mpsc::channel::<u32>(1);
                    let other = tokio::spawn(async move {
                        while let Some(v) = ping_rx.recv().await {
                            pong_tx.send(v).await.unwrap();
                        }
                    });
                    for i in 0..ROUNDS as u32 {
                        ping_tx.send(i).await.unwrap();
                        assert_eq!(pong_rx.recv().await, Some(i));
                    }
                    drop(ping_tx);
                    other.await.unwrap();
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
    }

    pub async fn yield_many() {
        let hs: Vec<_> = (0..YIELDERS)
            .map(|_| {
                tokio::spawn(async {
                    for _ in 0..YIELDS {
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for h in hs {
            h.await.unwrap();
        }
    }

    pub async fn timers() -> Vec<f64> {
        let hs: Vec<_> = (0..TIMERS)
            .map(|i| {
                tokio::spawn(async move {
                    let want = Duration::from_millis(1 + (i * 7919 % 100) as u64);
                    let t = Instant::now();
                    tokio::time::sleep(want).await;
                    (t.elapsed() - want).as_secs_f64() * 1000.0
                })
            })
            .collect();
        let mut late = Vec::with_capacity(TIMERS);
        for h in hs {
            late.push(h.await.unwrap());
        }
        late
    }

    pub async fn steady_timers() -> Vec<f64> {
        let hs: Vec<_> = (0..STEADY_TASKS)
            .map(|i| {
                tokio::spawn(async move {
                    let mut late = Vec::with_capacity(STEADY_SLEEPS);
                    for k in 0..STEADY_SLEEPS {
                        let want = Duration::from_millis(1 + ((i * 31 + k * 7) % 20) as u64);
                        let t = Instant::now();
                        tokio::time::sleep(want).await;
                        late.push((t.elapsed() - want).as_secs_f64() * 1000.0);
                    }
                    late
                })
            })
            .collect();
        let mut all = Vec::new();
        for h in hs {
            all.extend(h.await.unwrap());
        }
        all
    }

    pub async fn echo() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..CONNS {
                let (mut c, _) = l.accept().await.unwrap();
                c.set_nodelay(true).unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 64];
                    loop {
                        match c.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => c.write_all(&buf[..n]).await.unwrap(),
                        }
                    }
                });
            }
        });
        let clients: Vec<_> = (0..CONNS)
            .map(|_| {
                tokio::spawn(async move {
                    let mut c = TcpStream::connect(addr).await.unwrap();
                    c.set_nodelay(true).unwrap();
                    let msg = [7u8; 64];
                    let mut back = [0u8; 64];
                    for _ in 0..ECHOS {
                        c.write_all(&msg).await.unwrap();
                        c.read_exact(&mut back).await.unwrap();
                    }
                })
            })
            .collect();
        for c in clients {
            c.await.unwrap();
        }
        server.await.unwrap();
    }

    pub async fn bulk() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let tx = tokio::spawn(async move {
            let (mut c, _) = l.accept().await.unwrap();
            let chunk = vec![1u8; 256 << 10];
            let mut sent = 0;
            while sent < BULK {
                c.write_all(&chunk).await.unwrap();
                sent += chunk.len();
            }
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut buf = vec![0u8; 256 << 10];
        let mut got = 0;
        while got < BULK {
            got += c.read(&mut buf).await.unwrap();
        }
        tx.await.unwrap();
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let workers: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(8);
    let runs: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(7);
    let srt = s::rt(workers);
    let trt = t::rt(workers);
    println!("{workers} workers each, median of {runs} runs (timers: lateness over 3 runs); ratio = tokio time / strand time");
    println!("{:<40} {:>12} {:>12} {:>9}", "workload", "strand", "tokio", "ratio");

    macro_rules! bench {
        ($name:expr, $unit:expr, $per:expr, $s:expr, $t:expr) => {{
            let (mut a, mut b) = (Vec::new(), Vec::new());
            for _ in 0..runs {
                let x = Instant::now();
                srt.block_on($s);
                a.push(x.elapsed().as_secs_f64());
                let x = Instant::now();
                trt.block_on($t);
                b.push(x.elapsed().as_secs_f64());
            }
            let (ma, mb) = (median(a), median(b));
            let f = |secs: f64| $per(secs);
            println!("{:<40} {:>12} {:>12} {:>8.2}x", $name, f(ma), f(mb), mb / ma);
            let _ = $unit;
        }};
    }
    let ms = |secs: f64| format!("{:.1} ms", secs * 1000.0);
    bench!(format!("spawn and join {SPAWN} tasks"), "", ms, s::spawn_many(), t::spawn_many());
    bench!(format!("chain of {CHAIN} spawns"), "", ms, s::chained_spawn(), t::chained_spawn());
    bench!(format!("ping-pong, {PAIRS} pairs x {ROUNDS} round trips"), "", ms, s::ping_pong(), t::ping_pong());
    bench!(format!("{YIELDERS} tasks x {YIELDS} yields"), "", ms, s::yield_many(), t::yield_many());
    bench!(format!("TCP echo, {CONNS} conns x {ECHOS} x 64 B"), "", ms, s::echo(), t::echo());
    let gbs = |secs: f64| format!("{:.2} GB/s", BULK as f64 / secs / 1e9);
    bench!("TCP bulk, 1 GiB over one connection", "", gbs, s::bulk(), t::bulk());

    // Timers: how late each of 100,000 sleeps of 1 to 100 ms woke.
    let (mut sl, mut tl) = (Vec::new(), Vec::new());
    for _ in 0..3 {
        sl.extend(srt.block_on(s::timers()));
        tl.extend(trt.block_on(t::timers()));
    }
    let (s50, s99) = lateness_stats(sl);
    let (t50, t99) = lateness_stats(tl);
    println!(
        "{:<40} {:>12} {:>12}",
        format!("{TIMERS} sleeps at once, late p50"),
        format!("{s50:.2} ms"),
        format!("{t50:.2} ms")
    );
    println!(
        "{:<40} {:>12} {:>12}",
        format!("{TIMERS} sleeps at once, late p99"),
        format!("{s99:.2} ms"),
        format!("{t99:.2} ms")
    );
    let (mut sl, mut tl) = (Vec::new(), Vec::new());
    for _ in 0..3 {
        sl.extend(srt.block_on(s::steady_timers()));
        tl.extend(trt.block_on(t::steady_timers()));
    }
    let (s50, s99) = lateness_stats(sl);
    let (t50, t99) = lateness_stats(tl);
    let name = format!("{STEADY_TASKS} tasks x {STEADY_SLEEPS} sleeps of 1-20 ms");
    println!("{:<40} {:>12} {:>12}", format!("{name}, p50"), format!("{s50:.2} ms"), format!("{t50:.2} ms"));
    println!("{:<40} {:>12} {:>12}", format!("{name}, p99"), format!("{s99:.2} ms"), format!("{t99:.2} ms"));
}
