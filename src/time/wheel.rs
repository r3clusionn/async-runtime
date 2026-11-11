//! A hierarchical timing wheel with millisecond ticks.
//!
//! Six levels of 64 slots: level 0 slots are 1 ms wide, level 1 slots 64 ms, level 2 4.1 s, and
//! so on up to level 5 (slots of 12.4 days, about 2,200 years in all). A timer goes in the
//! lowest level whose span still separates its deadline from the current time, so insertion and
//! removal are O(1). When a higher-level slot comes due, its timers move down a level (closer
//! to their exact millisecond) instead of firing; each timer moves at most five times.
//!
//! Each level keeps a 64-bit mask of occupied slots, so finding the next deadline is a few bit
//! operations per level rather than a walk over empty slots.

use std::sync::Arc;

const LEVELS: usize = 6;
const SLOTS: usize = 64;
const BITS: u32 = 6;
/// The furthest the wheel looks ahead (64^6 ms, 795 days). A later timer is stored at the far
/// end and moves on from there each time that slot comes due, until its deadline is in range.
pub const MAX_SPAN: u64 = (1 << (BITS * LEVELS as u32)) - 1;

/// What the wheel needs from a timer: its deadline, and somewhere to note where it is stored.
pub trait Entry {
    fn deadline(&self) -> u64;
    fn set_slot(&self, slot: Option<(u8, u8)>);
    fn slot(&self) -> Option<(u8, u8)>;
}

struct Level<E> {
    slots: Vec<Vec<Arc<E>>>,
    occupied: u64,
}

impl<E> Level<E> {
    fn new() -> Level<E> {
        Level { slots: (0..SLOTS).map(|_| Vec::new()).collect(), occupied: 0 }
    }
}

/// When the next occupied slot comes due.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Expiration {
    level: usize,
    slot: usize,
    deadline: u64,
}

pub struct Wheel<E> {
    /// Milliseconds already processed: every timer due at or before this has fired.
    elapsed: u64,
    levels: Vec<Level<E>>,
    len: usize,
}

fn level_for(elapsed: u64, when: u64) -> usize {
    // The highest bit in which the deadline differs from now picks the level.
    let masked = (elapsed ^ when) | (SLOTS as u64 - 1);
    let significant = 63 - masked.leading_zeros() as usize;
    (significant / BITS as usize).min(LEVELS - 1)
}

fn slot_for(when: u64, level: usize) -> usize {
    ((when >> (BITS as usize * level)) as usize) & (SLOTS - 1)
}

impl<E: Entry> Wheel<E> {
    pub fn new(now: u64) -> Wheel<E> {
        Wheel { elapsed: now, levels: (0..LEVELS).map(|_| Level::new()).collect(), len: 0 }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds a timer. Returns it back if its deadline has already passed.
    pub fn insert(&mut self, e: Arc<E>) -> Result<(), Arc<E>> {
        let when = e.deadline().min(self.elapsed + MAX_SPAN);
        if when <= self.elapsed {
            return Err(e);
        }
        let level = level_for(self.elapsed, when);
        let slot = slot_for(when, level);
        e.set_slot(Some((level as u8, slot as u8)));
        let l = &mut self.levels[level];
        l.slots[slot].push(e);
        l.occupied |= 1 << slot;
        self.len += 1;
        Ok(())
    }

    /// Takes a timer out before it fires. Returns false if it was not in the wheel.
    pub fn remove(&mut self, e: &Arc<E>) -> bool {
        let Some((level, slot)) = e.slot() else { return false };
        let l = &mut self.levels[level as usize];
        let v = &mut l.slots[slot as usize];
        let Some(pos) = v.iter().position(|x| Arc::ptr_eq(x, e)) else { return false };
        v.swap_remove(pos);
        if v.is_empty() {
            l.occupied &= !(1 << slot);
        }
        e.set_slot(None);
        self.len -= 1;
        true
    }

    fn next_expiration(&self) -> Option<Expiration> {
        for (level, l) in self.levels.iter().enumerate() {
            if l.occupied == 0 {
                continue;
            }
            let slot_range = 1u64 << (BITS as usize * level);
            let level_range = slot_range << BITS;
            let now_slot = (self.elapsed / slot_range) as u32 & (SLOTS as u32 - 1);
            // Rotate so the slot for "now" is bit 0, then the lowest set bit is the next slot.
            let rotated = l.occupied.rotate_right(now_slot);
            let slot = (rotated.trailing_zeros() + now_slot) as usize % SLOTS;
            let level_start = self.elapsed & !(level_range - 1);
            let mut deadline = level_start + slot as u64 * slot_range;
            // Only possible at the top level, whose slots form a ring: a slot "before" now is a
            // whole turn ahead. (Below the top, a timer due in the current slot of a level is
            // stored in a lower level, so every occupied slot lies ahead.)
            if deadline <= self.elapsed {
                deadline += level_range;
            }
            return Some(Expiration { level, slot, deadline });
        }
        None
    }

    /// The earliest time a timer may fire, if any are waiting. For timers in a higher level,
    /// this is when their slot comes due (they then move down), which is never later than
    /// their own deadline.
    pub fn next_deadline(&self) -> Option<u64> {
        self.next_expiration().map(|e| e.deadline.max(self.elapsed))
    }

    /// Advances to `now` and returns every timer whose deadline is at or before it.
    pub fn advance(&mut self, now: u64, fired: &mut Vec<Arc<E>>) {
        while let Some(exp) = self.next_expiration() {
            if exp.deadline > now {
                break;
            }
            let entries = std::mem::take(&mut self.levels[exp.level].slots[exp.slot]);
            self.levels[exp.level].occupied &= !(1 << exp.slot);
            self.len -= entries.len();
            // Time moves to the start of this slot; its timers go down a level or fire.
            self.elapsed = self.elapsed.max(exp.deadline);
            for e in entries {
                e.set_slot(None);
                if let Err(e) = self.insert(e) {
                    fired.push(e);
                }
            }
        }
        self.elapsed = self.elapsed.max(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU16, AtomicU64, Ordering::Relaxed};

    struct T {
        when: AtomicU64,
        slot: AtomicU16,
    }

    impl Entry for T {
        fn deadline(&self) -> u64 {
            self.when.load(Relaxed)
        }
        fn set_slot(&self, s: Option<(u8, u8)>) {
            self.slot.store(s.map_or(u16::MAX, |(l, s)| (l as u16) << 8 | s as u16), Relaxed);
        }
        fn slot(&self) -> Option<(u8, u8)> {
            let v = self.slot.load(Relaxed);
            (v != u16::MAX).then_some(((v >> 8) as u8, v as u8))
        }
    }

    fn t(when: u64) -> Arc<T> {
        Arc::new(T { when: AtomicU64::new(when), slot: AtomicU16::new(u16::MAX) })
    }

    /// Steps one millisecond at a time and records when each timer fires.
    fn fire_times(start: u64, deadlines: &[u64], until: u64) -> Vec<(u64, u64)> {
        let mut w = Wheel::new(start);
        for &d in deadlines {
            w.insert(t(d)).ok().expect("in the future");
        }
        let mut out = Vec::new();
        let mut fired = Vec::new();
        let mut now = start;
        while now < until {
            now += 1;
            w.advance(now, &mut fired);
            for e in fired.drain(..) {
                out.push((e.deadline(), now));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn every_timer_fires_at_its_millisecond() {
        let deadlines = [1, 2, 63, 64, 65, 127, 128, 4095, 4096, 4097, 5000, 70_000, 262_143, 262_144, 300_001];
        let got = fire_times(0, &deadlines, 300_100);
        let want: Vec<(u64, u64)> = deadlines.iter().map(|&d| (d, d)).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn works_from_any_start_time() {
        for start in [1, 63, 64, 4095, 1_000_003, (1 << 30) - 7] {
            let deadlines: Vec<u64> = [1, 5, 64, 100, 4096, 9000, 66_000].iter().map(|d| start + d).collect();
            let got = fire_times(start, &deadlines, start + 70_000);
            let want: Vec<(u64, u64)> = deadlines.iter().map(|&d| (d, d)).collect();
            assert_eq!(got, want, "start {start}");
        }
    }

    #[test]
    fn jumps_fire_everything_due_and_nothing_else() {
        let mut w = Wheel::new(0);
        let all: Vec<Arc<T>> = [10, 100, 1000, 10_000, 100_000, 1_000_000].iter().map(|&d| t(d)).collect();
        for e in &all {
            w.insert(e.clone()).ok().unwrap();
        }
        let mut fired = Vec::new();
        w.advance(5_000, &mut fired);
        let mut got: Vec<u64> = fired.iter().map(|e| e.deadline()).collect();
        got.sort();
        assert_eq!(got, [10, 100, 1000]);
        assert_eq!(w.len(), 3);
        assert_eq!(w.next_deadline().map(|d| d <= 10_000), Some(true));
        fired.clear();
        w.advance(2_000_000, &mut fired);
        assert_eq!(fired.len(), 3);
        assert!(w.is_empty());
        assert_eq!(w.next_deadline(), None);
    }

    #[test]
    fn next_deadline_never_late() {
        // Whatever the wheel says, advancing to it must not skip a timer.
        let mut seed = 12345u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200 {
            let start = rnd() % 1_000_000;
            let mut w = Wheel::new(start);
            let mut pending: Vec<u64> = (0..50).map(|_| start + 1 + rnd() % [70, 5000, 300_000, 20_000_000][round % 4]).collect();
            for &d in &pending {
                w.insert(t(d)).ok().unwrap();
            }
            pending.sort();
            let mut fired = Vec::new();
            while let Some(next) = w.next_deadline() {
                assert!(next <= pending[0], "next {next} after the earliest pending {}", pending[0]);
                w.advance(next, &mut fired);
                for e in fired.drain(..) {
                    assert_eq!(e.deadline(), next.max(e.deadline()), "fired early");
                    assert!(e.deadline() <= next);
                    let i = pending.iter().position(|&d| d == e.deadline()).unwrap();
                    pending.remove(i);
                }
            }
            assert!(pending.is_empty());
        }
    }

    #[test]
    fn removal() {
        let mut w = Wheel::new(0);
        let a = t(50);
        let b = t(5000);
        let c = t(50);
        for e in [&a, &b, &c] {
            w.insert(e.clone()).ok().unwrap();
        }
        assert!(w.remove(&a));
        assert!(!w.remove(&a), "twice");
        assert!(w.remove(&b));
        let mut fired = Vec::new();
        w.advance(10_000, &mut fired);
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &c));
    }

    #[test]
    fn past_and_far_deadlines() {
        let mut w = Wheel::new(1000);
        assert!(w.insert(t(1000)).is_err(), "now is already past");
        assert!(w.insert(t(3)).is_err());
        let far = t(u64::MAX);
        w.insert(far.clone()).ok().unwrap();
        let near = t(1000 + MAX_SPAN);
        w.insert(near.clone()).ok().unwrap();
        let mut fired = Vec::new();
        w.advance(1000 + MAX_SPAN, &mut fired);
        // The one at the edge of the span fires; the one beyond it waits, never early.
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &near));
        assert_eq!(w.len(), 1);
        fired.clear();
        w.advance(1000 + 3 * MAX_SPAN, &mut fired);
        assert!(fired.is_empty());
    }
}
