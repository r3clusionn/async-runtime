//! Splits "spawn and join 100,000 tasks" into the spawning loop and the joining loop, for both
//! runtimes, to see where the time goes.

use std::time::Instant;

const N: usize = 100_000;

fn main() {
    let workers: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(8);
    let srt = strand::Builder::new().workers(workers).build().unwrap();
    let trt = tokio::runtime::Builder::new_multi_thread().worker_threads(workers).enable_all().build().unwrap();
    for round in 0..5 {
        let (sp, jo) = srt.block_on(async {
            let t = Instant::now();
            let hs: Vec<_> = (0..N).map(|i| strand::spawn(async move { i })).collect();
            let spawned = t.elapsed();
            let t = Instant::now();
            for h in hs {
                std::hint::black_box(h.await.unwrap());
            }
            (spawned, t.elapsed())
        });
        let (tsp, tjo) = trt.block_on(async {
            let t = Instant::now();
            let hs: Vec<_> = (0..N).map(|i| tokio::spawn(async move { i })).collect();
            let spawned = t.elapsed();
            let t = Instant::now();
            for h in hs {
                std::hint::black_box(h.await.unwrap());
            }
            (spawned, t.elapsed())
        });
        // The same, but spawned from inside a task (worker thread) instead of block_on.
        let (isp, ijo) = srt.block_on(async {
            strand::spawn(async {
                let t = Instant::now();
                let hs: Vec<_> = (0..N).map(|i| strand::spawn(async move { i })).collect();
                let spawned = t.elapsed();
                let t = Instant::now();
                for h in hs {
                    std::hint::black_box(h.await.unwrap());
                }
                (spawned, t.elapsed())
            })
            .await
            .unwrap()
        });
        let (tisp, tijo) = trt.block_on(async {
            tokio::spawn(async {
                let t = Instant::now();
                let hs: Vec<_> = (0..N).map(|i| tokio::spawn(async move { i })).collect();
                let spawned = t.elapsed();
                let t = Instant::now();
                for h in hs {
                    std::hint::black_box(h.await.unwrap());
                }
                (spawned, t.elapsed())
            })
            .await
            .unwrap()
        });
        let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
        println!(
            "round {round}: from block_on: strand spawn {:.1} join {:.1}, tokio spawn {:.1} join {:.1} | from a task: strand {:.1} + {:.1}, tokio {:.1} + {:.1}",
            ms(sp),
            ms(jo),
            ms(tsp),
            ms(tjo),
            ms(isp),
            ms(ijo),
            ms(tisp),
            ms(tijo)
        );
    }
}
