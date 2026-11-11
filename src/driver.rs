//! The I/O driver and the timers, turned by whichever worker is idle.

use std::io;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::io::{Events, Selector};
use crate::time::Timers;

pub(crate) struct Driver {
    core: Mutex<Core>,
    selector: Selector,
    timers: Timers,
}

/// What only the worker turning the driver touches.
pub(crate) struct Core {
    events: Events,
}

impl Driver {
    pub(crate) fn new() -> io::Result<Driver> {
        Ok(Driver {
            core: Mutex::new(Core { events: Events::with_capacity(1024) }),
            selector: Selector::new()?,
            timers: Timers::new(),
        })
    }

    pub(crate) fn try_lock(&self) -> Option<MutexGuard<'_, Core>> {
        self.core.try_lock().ok()
    }

    /// Waits up to `timeout` (forever with `None`) for I/O events or a wake, then delivers the
    /// events and fires due timers.
    pub(crate) fn turn(&self, core: &mut Core, timeout: Option<Duration>) {
        if self.selector.select(&mut core.events, timeout).is_ok() {
            self.selector.dispatch(&core.events);
        }
        self.timers.process();
    }

    /// How long a worker parking in the driver may wait: until the next timer.
    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        self.timers.next_timeout()
    }

    pub(crate) fn wake(&self) {
        self.selector.wake();
    }

    pub(crate) fn selector(&self) -> &Selector {
        &self.selector
    }

    pub(crate) fn timers(&self) -> &Timers {
        &self.timers
    }
}
