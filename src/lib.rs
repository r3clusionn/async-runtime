//! strand: a small multi-threaded async runtime.
//!
//! - [`task`]: one allocation per task, an atomic state machine, `JoinHandle` and abort.
//! - [`runtime`]: worker threads with local queues, a LIFO slot, work stealing, and parking in
//!   the I/O driver; `block_on`, `spawn`, `spawn_blocking`.
//! - [`coop`]: a per-poll budget so a task that is always ready cannot starve the others.
//! - [`time`]: a hierarchical timing wheel behind `sleep`, `timeout` and `interval`.
//! - [`net`]: TCP and UDP on a readiness reactor (epoll on Linux, AFD polling on Windows).
//! - [`sync`]: `oneshot` and `mpsc` channels.
//!
//! ```no_run
//! let rt = strand::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let h = strand::spawn(async { 1 + 1 });
//!     assert_eq!(h.await.unwrap(), 2);
//! });
//! ```

pub mod coop;
mod driver;
mod io;
pub mod net;
mod queue;
pub mod runtime;
pub mod sync;
pub mod task;
pub mod time;
mod waker;

pub use runtime::{spawn, spawn_blocking, yield_now, Builder, Handle, Runtime};
pub use task::{JoinError, JoinHandle};
pub use waker::AtomicWaker;
