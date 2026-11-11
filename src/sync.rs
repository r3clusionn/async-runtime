//! Channels between tasks.

/// A channel for one value.
pub mod oneshot {
    use std::cell::UnsafeCell;
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicU8;
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use crate::waker::AtomicWaker;

    const VALUE: u8 = 1;
    const TX_DONE: u8 = 2;
    const RX_GONE: u8 = 4;

    struct Inner<T> {
        state: AtomicU8,
        value: UnsafeCell<Option<T>>,
        waker: AtomicWaker,
    }

    // SAFETY: the value is written by the sender before it sets VALUE and read by whichever side
    // sees VALUE (the receiver, or the sender after the receiver left), never both.
    unsafe impl<T: Send> Send for Inner<T> {}
    unsafe impl<T: Send> Sync for Inner<T> {}

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let inner = Arc::new(Inner { state: AtomicU8::new(0), value: UnsafeCell::new(None), waker: AtomicWaker::new() });
        (Sender { inner: Some(inner.clone()) }, Receiver { inner, done: false })
    }

    pub struct Sender<T> {
        inner: Option<Arc<Inner<T>>>,
    }

    impl<T> Sender<T> {
        /// Sends the value; gives it back if the receiver is gone.
        pub fn send(mut self, value: T) -> Result<(), T> {
            let inner = self.inner.take().unwrap();
            // SAFETY: only the sender writes, and only before VALUE is set.
            unsafe { *inner.value.get() = Some(value) };
            let prev = inner.state.fetch_or(VALUE | TX_DONE, AcqRel);
            if prev & RX_GONE != 0 {
                // SAFETY: the receiver left before seeing VALUE, so it will not read.
                return Err(unsafe { (*inner.value.get()).take() }.unwrap());
            }
            inner.waker.wake();
            Ok(())
        }

        pub fn is_closed(&self) -> bool {
            self.inner.as_ref().is_none_or(|i| i.state.load(Acquire) & RX_GONE != 0)
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            if let Some(inner) = self.inner.take() {
                inner.state.fetch_or(TX_DONE, AcqRel);
                inner.waker.wake();
            }
        }
    }

    pub struct Receiver<T> {
        inner: Arc<Inner<T>>,
        done: bool,
    }

    /// The sender was dropped without sending.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RecvError;

    impl fmt::Display for RecvError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the sender was dropped without sending")
        }
    }

    impl std::error::Error for RecvError {}

    impl<T> Receiver<T> {
        fn take(&mut self, s: u8) -> Option<Result<T, RecvError>> {
            if s & VALUE != 0 {
                self.done = true;
                // SAFETY: VALUE was set with release ordering after the write.
                return Some(Ok(unsafe { (*self.inner.value.get()).take() }.expect("value taken twice")));
            }
            if s & TX_DONE != 0 {
                self.done = true;
                return Some(Err(RecvError));
            }
            None
        }
    }

    impl<T> Future for Receiver<T> {
        type Output = Result<T, RecvError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            assert!(!self.done, "oneshot receiver polled after it returned");
            if crate::coop::poll_proceed(cx).is_pending() {
                return Poll::Pending;
            }
            let s = self.inner.state.load(Acquire);
            if let Some(r) = self.take(s) {
                crate::coop::spend();
                return Poll::Ready(r);
            }
            self.inner.waker.register(cx.waker());
            let s = self.inner.state.load(Acquire);
            match self.take(s) {
                Some(r) => {
                    crate::coop::spend();
                    Poll::Ready(r)
                }
                None => Poll::Pending,
            }
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let prev = self.inner.state.fetch_or(RX_GONE, AcqRel);
            if prev & VALUE != 0 && !self.done {
                // SAFETY: the sender is done with the value.
                drop(unsafe { (*self.inner.value.get()).take() });
            }
        }
    }
}

/// Channels for many values from many senders to one receiver.
pub mod mpsc {
    use std::collections::VecDeque;
    use std::fmt;
    use std::sync::{Arc, Mutex};
    use std::task::{Poll, Waker};

    use crate::waker::AtomicWaker;

    struct State<T> {
        items: VecDeque<T>,
        senders: usize,
        rx_alive: bool,
        /// Senders of a bounded channel waiting for room.
        blocked: VecDeque<Waker>,
    }

    struct Chan<T> {
        state: Mutex<State<T>>,
        rx_waker: AtomicWaker,
        cap: Option<usize>,
    }

    fn new<T>(cap: Option<usize>) -> Arc<Chan<T>> {
        Arc::new(Chan {
            state: Mutex::new(State { items: VecDeque::new(), senders: 1, rx_alive: true, blocked: VecDeque::new() }),
            rx_waker: AtomicWaker::new(),
            cap,
        })
    }

    /// A channel that holds at most `capacity` values; `send` waits for room.
    pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        assert!(capacity > 0, "a bounded channel needs room for one value");
        let c = new(Some(capacity));
        (Sender { chan: c.clone() }, Receiver { chan: c })
    }

    /// A channel without a limit; `send` never waits.
    pub fn unbounded<T>() -> (UnboundedSender<T>, Receiver<T>) {
        let c = new(None);
        (UnboundedSender { chan: c.clone() }, Receiver { chan: c })
    }

    /// The receiver is gone; the value comes back.
    pub struct SendError<T>(pub T);

    impl<T> fmt::Debug for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("SendError(..)")
        }
    }

    impl<T> fmt::Display for SendError<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("the receiver was dropped")
        }
    }

    impl<T> Chan<T> {
        fn try_push(&self, value: T) -> Result<Option<T>, SendError<T>> {
            let mut s = self.state.lock().unwrap();
            if !s.rx_alive {
                return Err(SendError(value));
            }
            if self.cap.is_some_and(|c| s.items.len() >= c) {
                return Ok(Some(value));
            }
            s.items.push_back(value);
            drop(s);
            self.rx_waker.wake();
            Ok(None)
        }

        fn add_sender(&self) {
            self.state.lock().unwrap().senders += 1;
        }

        fn drop_sender(&self) {
            let last = {
                let mut s = self.state.lock().unwrap();
                s.senders -= 1;
                s.senders == 0
            };
            if last {
                self.rx_waker.wake();
            }
        }
    }

    pub struct Sender<T> {
        chan: Arc<Chan<T>>,
    }

    impl<T> Sender<T> {
        /// Sends, waiting while the channel is full.
        pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
            let mut value = Some(value);
            std::future::poll_fn(|cx| {
                let v = value.take().unwrap();
                match self.chan.try_push(v) {
                    Err(e) => Poll::Ready(Err(e)),
                    Ok(None) => Poll::Ready(Ok(())),
                    Ok(Some(v)) => {
                        let mut s = self.chan.state.lock().unwrap();
                        // Room may have appeared since the push failed; check under the lock.
                        if s.items.len() < self.chan.cap.unwrap() || !s.rx_alive {
                            drop(s);
                            value = Some(v);
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        s.blocked.push_back(cx.waker().clone());
                        value = Some(v);
                        Poll::Pending
                    }
                }
            })
            .await
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.chan.add_sender();
            Sender { chan: self.chan.clone() }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.chan.drop_sender();
        }
    }

    pub struct UnboundedSender<T> {
        chan: Arc<Chan<T>>,
    }

    impl<T> UnboundedSender<T> {
        pub fn send(&self, value: T) -> Result<(), SendError<T>> {
            self.chan.try_push(value).map(|_| ())
        }
    }

    impl<T> Clone for UnboundedSender<T> {
        fn clone(&self) -> Self {
            self.chan.add_sender();
            UnboundedSender { chan: self.chan.clone() }
        }
    }

    impl<T> Drop for UnboundedSender<T> {
        fn drop(&mut self) {
            self.chan.drop_sender();
        }
    }

    pub struct Receiver<T> {
        chan: Arc<Chan<T>>,
    }

    impl<T> Receiver<T> {
        /// The next value, or `None` once every sender is gone and the channel is empty.
        pub async fn recv(&mut self) -> Option<T> {
            std::future::poll_fn(|cx| {
                if crate::coop::poll_proceed(cx).is_pending() {
                    return Poll::Pending;
                }
                let mut s = self.chan.state.lock().unwrap();
                if let Some(v) = s.items.pop_front() {
                    let unblock = s.blocked.pop_front();
                    drop(s);
                    if let Some(w) = unblock {
                        w.wake();
                    }
                    crate::coop::spend();
                    return Poll::Ready(Some(v));
                }
                if s.senders == 0 {
                    return Poll::Ready(None);
                }
                // Registered under the lock, so a send after it finds the waker.
                self.chan.rx_waker.register(cx.waker());
                Poll::Pending
            })
            .await
        }

        /// A value if one is waiting, without waiting.
        pub fn try_recv(&mut self) -> Option<T> {
            self.chan.state.lock().unwrap().items.pop_front()
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let (items, blocked) = {
                let mut s = self.chan.state.lock().unwrap();
                s.rx_alive = false;
                (std::mem::take(&mut s.items), std::mem::take(&mut s.blocked))
            };
            drop(items);
            for w in blocked {
                w.wake();
            }
        }
    }
}
