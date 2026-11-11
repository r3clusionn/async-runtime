//! A worker's run queue: a fixed ring that its owner pushes to and pops from without locks,
//! and that other workers steal half of with a compare-and-swap.
//!
//! This is the algorithm tokio's scheduler uses. `head` packs two 32-bit positions: `real`,
//! the next task to pop, and `steal`, the start of a range a thief is still copying out. When
//! no theft is in progress they are equal. A thief first advances `real` past the half it
//! takes (so the owner no longer pops those), copies the tasks into its own queue, then moves
//! `steal` up to `real` to hand the slots back. The owner never writes into slots between
//! `steal` and `tail`, so a slot being copied is never overwritten. Only one theft runs at a
//! time; a second thief gives up and looks elsewhere.
//!
//! When the ring is full, the owner moves half of it plus the new task to the injector in one
//! batch. Positions wrap around `u32`, so only differences between them mean anything.

use std::mem::MaybeUninit;

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, AtomicU64};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, AtomicU64};

use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};

#[cfg(not(loom))]
const CAP: usize = 256;
#[cfg(loom)]
const CAP: usize = 4;
const MASK: usize = CAP - 1;

/// `std::cell::UnsafeCell` with loom's closure API, so the same code runs under loom.
#[cfg(not(loom))]
struct Slot<T>(std::cell::UnsafeCell<MaybeUninit<T>>);

#[cfg(not(loom))]
impl<T> Slot<T> {
    fn new() -> Self {
        Slot(std::cell::UnsafeCell::new(MaybeUninit::uninit()))
    }
    fn with<R>(&self, f: impl FnOnce(*const MaybeUninit<T>) -> R) -> R {
        f(self.0.get())
    }
    fn with_mut<R>(&self, f: impl FnOnce(*mut MaybeUninit<T>) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(loom)]
struct Slot<T>(loom::cell::UnsafeCell<MaybeUninit<T>>);

#[cfg(loom)]
impl<T> Slot<T> {
    fn new() -> Self {
        Slot(loom::cell::UnsafeCell::new(MaybeUninit::uninit()))
    }
    fn with<R>(&self, f: impl FnOnce(*const MaybeUninit<T>) -> R) -> R {
        self.0.with(f)
    }
    fn with_mut<R>(&self, f: impl FnOnce(*mut MaybeUninit<T>) -> R) -> R {
        self.0.with_mut(f)
    }
}

fn pack(steal: u32, real: u32) -> u64 {
    ((steal as u64) << 32) | real as u64
}

fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

pub(crate) struct LocalQueue<T> {
    head: AtomicU64,
    tail: AtomicU32,
    buf: Box<[Slot<T>]>,
}

// SAFETY: slots are handed between threads through the head and tail atomics, as described above.
unsafe impl<T: Send> Send for LocalQueue<T> {}
unsafe impl<T: Send> Sync for LocalQueue<T> {}

impl<T> LocalQueue<T> {
    pub(crate) fn new() -> LocalQueue<T> {
        LocalQueue { head: AtomicU64::new(0), tail: AtomicU32::new(0), buf: (0..CAP).map(|_| Slot::new()).collect() }
    }

    /// Tasks in the queue, as seen from any thread (approximate while it changes).
    pub(crate) fn len(&self) -> usize {
        let (_, real) = unpack(self.head.load(Acquire));
        self.tail.load(Acquire).wrapping_sub(real) as usize
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Owner only. Adds to the back; when full, hands half the queue and `task` to `overflow`.
    pub(crate) fn push_back(&self, mut task: T, overflow: &mut dyn FnMut(Vec<T>)) {
        loop {
            let (steal, real) = unpack(self.head.load(Acquire));
            // Only the owner writes `tail`, so a relaxed read sees its own last store.
            let tail = self.tail.load(Relaxed);
            if tail.wrapping_sub(steal) < CAP as u32 {
                // SAFETY: slots from `tail` up to `steal + CAP` are free and ours to write.
                self.buf[tail as usize & MASK].with_mut(|p| unsafe { (*p).write(task) });
                self.tail.store(tail.wrapping_add(1), Release);
                return;
            }
            if steal != real {
                // Full, and a thief is copying: it will free room soon; do not wait for it.
                overflow(vec![task]);
                return;
            }
            match self.move_half_out(task, real, tail) {
                Ok(batch) => {
                    overflow(batch);
                    return;
                }
                // A thief claimed some tasks meanwhile, so there may be room now.
                Err(t) => task = t,
            }
        }
    }

    fn move_half_out(&self, task: T, head: u32, tail: u32) -> Result<Vec<T>, T> {
        let n = (CAP / 2) as u32;
        debug_assert_eq!(tail.wrapping_sub(head), CAP as u32);
        if self
            .head
            .compare_exchange(pack(head, head), pack(head.wrapping_add(n), head.wrapping_add(n)), Release, Relaxed)
            .is_err()
        {
            return Err(task);
        }
        let mut batch = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            let pos = head.wrapping_add(i) as usize & MASK;
            // SAFETY: the claim above made these slots ours; each holds a task.
            batch.push(self.buf[pos].with(|p| unsafe { (*p).assume_init_read() }));
        }
        batch.push(task);
        Ok(batch)
    }

    /// Owner only. Takes from the front.
    pub(crate) fn pop(&self) -> Option<T> {
        let mut head = self.head.load(Acquire);
        let idx = loop {
            let (steal, real) = unpack(head);
            let tail = self.tail.load(Relaxed);
            if real == tail {
                return None;
            }
            let next_real = real.wrapping_add(1);
            // With no theft in progress, `steal` moves along with `real`.
            let next = if steal == real { pack(next_real, next_real) } else { pack(steal, next_real) };
            match self.head.compare_exchange(head, next, AcqRel, Acquire) {
                Ok(_) => break real as usize & MASK,
                Err(actual) => head = actual,
            }
        };
        // SAFETY: the slot at the old `real` was claimed by the exchange and holds a task.
        Some(self.buf[idx].with(|p| unsafe { (*p).assume_init_read() }))
    }

    /// Called by the owner of `dst`: moves half of this queue into `dst` and returns one task.
    pub(crate) fn steal_into(&self, dst: &LocalQueue<T>) -> Option<T> {
        let dst_tail = dst.tail.load(Relaxed);
        let (dst_steal, _) = unpack(dst.head.load(Acquire));
        // Do not steal into a queue that is already half full.
        if dst_tail.wrapping_sub(dst_steal) > (CAP / 2) as u32 {
            return None;
        }
        let n = self.steal_half(dst, dst_tail);
        if n == 0 {
            return None;
        }
        // Return the last task copied directly; publish the others in `dst`.
        let n = n - 1;
        let ret_pos = dst_tail.wrapping_add(n) as usize & MASK;
        // SAFETY: just written by `steal_half`, not yet published.
        let ret = dst.buf[ret_pos].with(|p| unsafe { (*p).assume_init_read() });
        if n > 0 {
            dst.tail.store(dst_tail.wrapping_add(n), Release);
        }
        Some(ret)
    }

    fn steal_half(&self, dst: &LocalQueue<T>, dst_tail: u32) -> u32 {
        let mut prev = self.head.load(Acquire);
        let (n, first) = loop {
            let (steal, real) = unpack(prev);
            let tail = self.tail.load(Acquire);
            if steal != real {
                return 0; // another thief is at work
            }
            let avail = tail.wrapping_sub(real);
            let n = avail - avail / 2;
            if n == 0 {
                return 0;
            }
            // Claim: `real` moves past the stolen range, `steal` stays at its start.
            let next = pack(steal, real.wrapping_add(n));
            match self.head.compare_exchange(prev, next, AcqRel, Acquire) {
                Ok(_) => break (n, real),
                Err(actual) => prev = actual,
            }
        };
        for i in 0..n {
            let src = first.wrapping_add(i) as usize & MASK;
            let d = dst_tail.wrapping_add(i) as usize & MASK;
            // SAFETY: the claimed source slots hold tasks the owner will not touch until `steal`
            // moves; the destination slots are free (checked by the caller, who owns `dst`).
            let task = self.buf[src].with(|p| unsafe { (*p).assume_init_read() });
            dst.buf[d].with_mut(|p| unsafe { (*p).write(task) });
        }
        // Hand the slots back: `steal` catches up with `real` (which the owner may have moved).
        let mut prev = pack(first, first.wrapping_add(n));
        loop {
            let (_, real) = unpack(prev);
            match self.head.compare_exchange(prev, pack(real, real), AcqRel, Acquire) {
                Ok(_) => return n,
                Err(actual) => prev = actual,
            }
        }
    }
}

impl<T> Drop for LocalQueue<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    #[test]
    fn fifo_and_overflow() {
        let q = LocalQueue::new();
        let mut spilled = Vec::new();
        for i in 0..CAP + 10 {
            q.push_back(i, &mut |b| spilled.extend(b));
        }
        // The first overflow moved the oldest half plus the new task out.
        assert_eq!(spilled.len(), CAP / 2 + 1);
        assert_eq!(spilled[..3], [0, 1, 2]);
        let mut rest = Vec::new();
        while let Some(v) = q.pop() {
            rest.push(v);
        }
        let mut all: Vec<usize> = spilled.into_iter().chain(rest).collect();
        all.sort();
        assert_eq!(all, (0..CAP + 10).collect::<Vec<_>>());
    }

    #[test]
    fn steal_takes_half() {
        let a = LocalQueue::new();
        let b = LocalQueue::new();
        for i in 0..10 {
            a.push_back(i, &mut |_| unreachable!());
        }
        assert_eq!(a.steal_into(&b), Some(4), "half (5) moved, the last returned");
        assert_eq!((a.len(), b.len()), (5, 4));
        assert_eq!(a.pop(), Some(5));
        assert_eq!(b.pop(), Some(0));
    }

    /// The owner pushes and pops while three thieves steal: every item is seen exactly once.
    /// Runs under Miri too (smaller counts there).
    #[test]
    fn concurrent_owner_and_thieves() {
        let n = if cfg!(miri) { 300 } else { 200_000 };
        let q: Arc<LocalQueue<usize>> = Arc::new(LocalQueue::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thieves: Vec<_> = (0..3)
            .map(|_| {
                let (q, seen, done) = (q.clone(), seen.clone(), done.clone());
                std::thread::spawn(move || {
                    let mine = LocalQueue::new();
                    let mut got = Vec::new();
                    loop {
                        if let Some(v) = q.steal_into(&mine) {
                            got.push(v);
                        }
                        while let Some(v) = mine.pop() {
                            got.push(v);
                        }
                        if done.load(std::sync::atomic::Ordering::SeqCst) && q.is_empty() {
                            break;
                        }
                    }
                    seen.lock().unwrap().extend(got);
                })
            })
            .collect();
        let mut spilled = Vec::new();
        let mut popped = Vec::new();
        for i in 0..n {
            q.push_back(i, &mut |b| spilled.extend(b));
            if i % 3 == 0 {
                if let Some(v) = q.pop() {
                    popped.push(v);
                }
            }
        }
        while let Some(v) = q.pop() {
            popped.push(v);
        }
        done.store(true, std::sync::atomic::Ordering::SeqCst);
        for t in thieves {
            t.join().unwrap();
        }
        let mut all = seen.lock().unwrap().clone();
        all.extend(spilled);
        all.extend(popped);
        assert_eq!(all.len(), n, "nothing lost or duplicated");
        let set: HashSet<usize> = all.into_iter().collect();
        assert_eq!(set.len(), n);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    /// Owner pushes and pops through an overflow while a thief steals; every item once.
    #[test]
    fn owner_and_one_thief() {
        loom::model(|| {
            let q: Arc<LocalQueue<usize>> = Arc::new(LocalQueue::new());
            let q2 = q.clone();
            let thief = thread::spawn(move || {
                let mine = LocalQueue::new();
                let mut got = Vec::new();
                if let Some(v) = q2.steal_into(&mine) {
                    got.push(v);
                }
                while let Some(v) = mine.pop() {
                    got.push(v);
                }
                got
            });
            let mut got = Vec::new();
            for i in 0..6 {
                q.push_back(i, &mut |b| got.extend(b));
            }
            while let Some(v) = q.pop() {
                got.push(v);
            }
            got.extend(thief.join().unwrap());
            got.sort();
            assert_eq!(got, (0..6).collect::<Vec<_>>());
        });
    }

    /// Two thieves at once: one of them backs off, and nothing is taken twice.
    #[test]
    fn two_thieves() {
        loom::model(|| {
            let q: Arc<LocalQueue<usize>> = Arc::new(LocalQueue::new());
            for i in 0..4 {
                q.push_back(i, &mut |_| unreachable!());
            }
            let ts: Vec<_> = (0..2)
                .map(|_| {
                    let q = q.clone();
                    thread::spawn(move || {
                        let mine = LocalQueue::new();
                        let mut got = Vec::new();
                        if let Some(v) = q.steal_into(&mine) {
                            got.push(v);
                        }
                        while let Some(v) = mine.pop() {
                            got.push(v);
                        }
                        got
                    })
                })
                .collect();
            let mut got = Vec::new();
            while let Some(v) = q.pop() {
                got.push(v);
            }
            for t in ts {
                got.extend(t.join().unwrap());
            }
            got.sort();
            assert_eq!(got, vec![0, 1, 2, 3]);
        });
    }
}
