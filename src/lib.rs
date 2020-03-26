//! # Flume
//!
//! A blazingly fast multi-producer, single-consumer channel.
//!
//! *"Do not communicate by sharing memory; instead, share memory by communicating."*
//!
//! ## Examples
//!
//! ```
//! let (tx, rx) = flume::unbounded();
//!
//! tx.send(42).unwrap();
//! assert_eq!(rx.recv().unwrap(), 42);
//! ```

// #[cfg(feature = "select")]
// pub mod select;
// #[cfg(feature = "async")]
// pub mod r#async;

// TODO reimplement

// Reexports
// #[cfg(feature = "select")]
// pub use select::Selector;

use std::{
    collections::VecDeque,
    sync::{self, Arc, Condvar, Mutex, WaitTimeoutResult, atomic::{AtomicUsize, AtomicBool, Ordering}},
    time::{Duration, Instant},
    cell::{UnsafeCell, RefCell},
    marker::PhantomData,
    thread,
};
#[cfg(windows)]
use std::sync::{Mutex as InnerMutex, MutexGuard};
#[cfg(not(windows))]
use spin::{Mutex as InnerMutex, MutexGuard};

// #[cfg(feature = "async")]
// use std::task::Waker;
// #[cfg(feature = "select")]
// use crate::select::Token;
// #[cfg(feature = "async")]
// use crate::r#async::RecvFuture;
// TODO

/// An error that may be emitted when attempting to send a value into a channel on a sender.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

/// An error that may be emitted when attempting to wait for a value on a receiver.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RecvError {
    Disconnected,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    Full(T),
    Disconnected(T),
}

/// An error that may be emitted when attempting to fetch a value on a receiver.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

/// An error that may be emitted when attempting to wait for a value on a receiver with a timeout.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RecvTimeoutError {
    Timeout,
    Disconnected,
}

#[derive(Default, Debug)]
struct Signal<T = ()> {
    lock: Mutex<T>,
    trigger: Condvar,
    waiters: AtomicUsize,
}

impl<T> Signal<T> {
    fn lock(&self) -> sync::MutexGuard<T> {
        self.lock.lock().unwrap()
    }

    fn wait<G>(&self, sync_guard: G) {
        let guard = self.lock.lock().unwrap();
        self.waiters.fetch_add(1, Ordering::Relaxed);
        drop(sync_guard);
        let _guard = self.trigger.wait(guard).unwrap();
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }

    fn wait_timeout<G>(&self, dur: Duration, sync_guard: G) -> WaitTimeoutResult {
        let guard = self.lock.lock().unwrap();
        self.waiters.fetch_add(1, Ordering::Relaxed);
        drop(sync_guard);
        let (_guard, timeout) = self.trigger.wait_timeout(guard, dur).unwrap();
        self.waiters.fetch_sub(1, Ordering::Relaxed);
        timeout
    }

    fn wait_while<G>(&self, sync_guard: G, cond: impl FnMut(&mut T) -> bool) {
        let guard = self.lock.lock().unwrap();
        self.waiters.fetch_add(1, Ordering::Relaxed);
        drop(sync_guard);
        let _guard = self.trigger.wait_while(guard, cond).unwrap();
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }

    fn notify_one<G>(&self, sync_guard: G) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            drop(sync_guard);
            let _guard = self.lock.lock().unwrap();
            self.trigger.notify_one();
        }
    }

    fn notify_one_with<G>(&self, f: impl FnOnce(&mut T), sync_guard: G) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            drop(sync_guard);
            let mut guard = self.lock.lock().unwrap();
            f(&mut *guard);
            self.trigger.notify_one();
        }
    }

    fn notify_all<G>(&self, sync_guard: G) {
        if self.waiters.load(Ordering::Relaxed) > 0 {
            drop(sync_guard);
            let _guard = self.lock.lock().unwrap();
            self.trigger.notify_all();
        }
    }
}

#[inline]
#[cfg(not(windows))]
fn wait_lock<T>(lock: &InnerMutex<T>) -> MutexGuard<T> {
    let mut i = 0;
    loop {
        for _ in 0..5 {
            if let Some(guard) = lock.try_lock() {
                return guard;
            }
            thread::yield_now();
        }
        thread::sleep(Duration::from_nanos(i * 50));
        i += 1;
    }
}

#[derive(Debug)]
struct BoundedQueues<T> {
    senders: VecDeque<Arc<Signal<Option<T>>>>,
    queue: VecDeque<T>,
    receivers: VecDeque<Arc<Signal<Option<T>>>>,
}

enum Flavor<T> {
    Bounded {
        cap: usize,
        senders: spin::Mutex<VecDeque<Arc<Signal<Option<T>>>>>,
    },
    Unbounded,
}

struct Channel<T> {
    queue: spin::Mutex<VecDeque<T>>,
    receivers: spin::Mutex<VecDeque<Arc<Signal<Option<T>>>>>,
    flavor: Flavor<T>,
}

struct Shared<T> {
    chan: Channel<T>,
    disconnected: AtomicBool,

    senders: AtomicUsize,
    /// Used for notifying the receiver about incoming messages.
    send_signal: Signal,
}

impl<T> Shared<T> {
    fn new(cap: Option<usize>) -> Self {
        Self {
            chan: Channel {
                queue: Default::default(),
                receivers: Default::default(),
                flavor: if let Some(cap) = cap { 
                    Flavor::Bounded { 
                        cap,
                        senders: Default::default(),
                    }
                } else {
                    Flavor::Unbounded
                },
            },
            disconnected: AtomicBool::new(false),

            senders: AtomicUsize::new(1),
            send_signal: Signal::default(),
        }
    }

    /// Inform the receiver that all senders have been dropped
    #[inline]
    fn all_senders_disconnected(&self) {
        self.disconnected.store(true, Ordering::Relaxed);
        self.send_signal.notify_all(());
    }

    #[inline]
    fn receiver_disconnected(&self) {
        self.disconnected.store(true, Ordering::Relaxed);
        for receiver in self.chan.receivers.lock().iter() {
            receiver.notify_all(());
        }
        if let Flavor::Bounded { senders, .. } = &self.chan.flavor {
            for sender in senders.lock().iter() {
                sender.notify_all(());
            }
        }
    }
}

/// A transmitting end of a channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
    unblock_signal: Option<Arc<Signal<Option<T>>>>,
    /// Used to prevent Sync being implemented for this type - we never actually use it!
    /// TODO: impl<T> !Sync for Receiver<T> {} when negative traits are stable
    _phantom_cell: UnsafeCell<()>,
}

impl<T> Sender<T> {
    /// Send a value into the channel, returning an error if the channel receiver has
    /// been dropped. If the channel is bounded and is full, this method will block.
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if self.shared.disconnected.load(Ordering::Relaxed) {
            return Err(TrySendError::Disconnected(msg));
        }

        let chan = &self.shared.chan;

        if let (Some(recv), receivers) = {
            let mut receivers = wait_lock(&chan.receivers);
            (receivers.pop_front(), receivers)
        } {
            debug_assert!(wait_lock(&chan.queue).len() == 0);
            recv.notify_one_with(|m| *m = Some(msg), receivers);
            return Ok(());
        }

        let mut queue = wait_lock(&chan.queue);

        match &chan.flavor {
            Flavor::Bounded { cap, .. } => {
                if queue.len() < *cap {
                    queue.push_back(msg);
                    Ok(())
                } else {
                    Err(TrySendError::Full(msg))
                }
            },
            Flavor::Unbounded => {
                queue.push_back(msg);
                self.shared.send_signal.notify_one(queue);
                Ok(())
            },
        }
    }

    /// Attempt to send a value into the channel. If the channel is bounded and full, or the
    /// receiver has been dropped, an error is returned. If the channel associated with this
    /// sender is unbounded, this method has the same behaviour as [`Sender::send`].
    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let chan = &self.shared.chan;

        if self.shared.disconnected.load(Ordering::Relaxed) {
            return Err(SendError(msg));
        }

        if let (Some(recv), receivers) = {
            let mut receivers = wait_lock(&chan.receivers);
            (receivers.pop_front(), receivers)
        } {
            debug_assert!(wait_lock(&chan.queue).len() == 0);
            recv.notify_one_with(|m| *m = Some(msg), receivers);
            return Ok(());
        }

        let mut queue = wait_lock(&chan.queue);
        match &chan.flavor {
            Flavor::Bounded { cap, senders } => {
                if queue.len() < *cap {
                    queue.push_back(msg);
                    return Ok(());
                }

                let mut senders = wait_lock(&senders);
                let unblock_signal = self.unblock_signal.as_ref().unwrap();
                *unblock_signal.lock() = Some(msg);
                senders.push_back(unblock_signal.clone());

                unblock_signal.wait_while(senders, |msg| {
                    msg.is_some() && !self.shared.disconnected.load(Ordering::Relaxed)
                });

                if let Some(msg) = unblock_signal.lock().take() {
                    Err(SendError(msg))
                } else {
                    Ok(())
                }
            }
            Flavor::Unbounded => {
                queue.push_back(msg);
                return Ok(())
            }
        }
    }
}

impl<T> Clone for Sender<T> {
    /// Clone this sender. [`Sender`] acts as a handle to a channel, and the channel will only be
    /// cleaned up when all senders and the receiver have been dropped.
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Release);
        Self {
            shared: self.shared.clone(),
            unblock_signal: Some(Arc::new(Signal::default())),
            _phantom_cell: UnsafeCell::new(()),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev_senders = self.shared.senders.fetch_sub(1, Ordering::AcqRel);
        let cur_senders = prev_senders - 1;
        if cur_senders == 0 {
            self.shared.all_senders_disconnected();
        }
    }
}

/// The receiving end of a channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
    unblock_signal: Arc<Signal<Option<T>>>,
    /// Buffer for messages
    buffer: RefCell<VecDeque<T>>,
    /// Used to prevent Sync being implemented for this type - we never actually use it!
    /// TODO: impl<T> !Sync for Receiver<T> {} when negative traits are stable
    _phantom_cell: UnsafeCell<()>,
}

fn pull_pending<T>(
    cap: usize,
    queue: &mut VecDeque<T>,
    senders: &mut VecDeque<Arc<Signal<Option<T>>>>,
) {
    while queue.len() <= cap {
        if let Some(signal) = senders.pop_front() {
            let mut msg = None;
            signal.notify_one_with(|m| msg = m.take(), ());
            if let Some(msg) = msg {
                queue.push_back(msg);
            }
        } else {
            break;
        }
    }
}

impl<T> Receiver<T> {
    fn wait_for_message(
        &self,
        mut receivers: MutexGuard<VecDeque<Arc<Signal<Option<T>>>>>,
    ) -> T {
        receivers.push_back(self.unblock_signal.clone());
        self.unblock_signal.wait_while(receivers, |msg| {
            msg.is_none() && !self.shared.disconnected.load(Ordering::Relaxed)
        });
        self.unblock_signal.lock().take().unwrap()
    }

    /// Attempt to fetch an incoming value from the channel associated with this receiver,
    /// returning an error if the channel is empty or all channel senders have been dropped.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let chan = &self.shared.chan;

        let mut queue = wait_lock(&chan.queue);
        if let Flavor::Bounded { cap, senders } =  &chan.flavor {
            pull_pending(*cap, &mut queue, &mut wait_lock(&senders));
        }

        if let Some(msg) = wait_lock(&chan.queue).pop_front() {
            Ok(msg)
        } else if self.shared.disconnected.load(Ordering::Relaxed) {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Wait for an incoming value from the channel associated with this receiver, returning an
    /// error if all channel senders have been dropped.
    pub fn recv(&self) -> Result<T, RecvError> where T: std::fmt::Debug { // TODO Debug
        let chan = &self.shared.chan;
        let mut queue = wait_lock(&chan.queue);

        if let Flavor::Bounded { cap, senders } =  &chan.flavor {
            pull_pending(*cap, &mut queue, &mut wait_lock(&senders));
        }

        if let Some(msg) = queue.pop_front() {
            Ok(msg)
        } else if self.shared.disconnected.load(Ordering::Relaxed) {
            Err(RecvError::Disconnected)
        } else {
            Ok(self.wait_for_message(wait_lock(&chan.receivers)))
        }
    }

    /// Wait for an incoming value from the channel associated with this receiver, returning an
    /// error if all channel senders have been dropped or the timeout has expired.
    pub fn recv_timeout(&self, dur: Duration) -> Result<T, RecvTimeoutError> {
        self.recv_deadline(Instant::now().checked_add(dur).unwrap())
    }

    /// Wait for an incoming value from the channel associated with this receiver, returning an
    /// error if all channel senders have been dropped or the deadline has passed.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        let chan = &self.shared.chan;
        match &chan.flavor {
            Flavor::Bounded { cap, senders } => loop {
                let mut queue = wait_lock(&chan.queue);
                pull_pending(*cap, &mut queue, &mut wait_lock(&senders));
                if let Some(msg) = queue.pop_front() {
                    break Ok(msg);
                } else if self.shared.disconnected.load(Ordering::Relaxed) {
                    break Err(RecvTimeoutError::Disconnected);
                } else {
                    let now = Instant::now();
                    if self
                        .shared
                        .send_signal
                        .wait_timeout(deadline.duration_since(now), queue)
                        .timed_out()
                    {
                        break Err(RecvTimeoutError::Timeout);
                    }
                }
            },
            Flavor::Unbounded => loop {
                let mut queue = wait_lock(&chan.queue);
                if let Some(msg) = queue.pop_front() {
                    break Ok(msg);
                } else if self.shared.disconnected.load(Ordering::Relaxed) {
                    break Err(RecvTimeoutError::Disconnected);
                } else {
                    let now = Instant::now();
                    if self
                        .shared
                        .send_signal
                        .wait_timeout(deadline.duration_since(now), queue)
                        .timed_out()
                    {
                        break Err(RecvTimeoutError::Timeout);
                    }
                }
            },
        }
    }

    // Takes `&mut self` to avoid >1 task waiting on this channel
    // TODO: Is this necessary?
    /// Create a future that may be used to wait asynchronously for an incoming value on the
    /// channel associated with this receiver.
    #[cfg(feature = "async")]
    // pub fn recv_async(&mut self) -> RecvFuture<T> {
    //     RecvFuture::new(self)
    // }
    // TODO reimplement

    /// A blocking iterator over the values received on the channel that finishes iteration when
    /// all receivers of the channel have been dropped.
    pub fn iter(&self) -> Iter<T> {
        Iter { receiver: &self }
    }

    /// A non-blocking iterator over the values received on the channel that finishes iteration
    /// when all receivers of the channel have been dropped or the channel is empty.
    pub fn try_iter(&self) -> TryIter<T> {
        TryIter { receiver: &self }
    }

    /// Take all items currently sitting in the channel and produce an iterator over them. Unlike
    /// `try_iter`, the iterator will not attempt to fetch any more values from the channel once
    /// the function has been called.
    pub fn drain(&self) -> Drain<T> {
        let chan = &self.shared.chan;
        let mut queue = wait_lock(&chan.queue);
        let queue = match &chan.flavor {
            Flavor::Bounded { cap, senders } => {
                pull_pending(*cap, &mut queue, &mut wait_lock(&senders));
                std::mem::take(&mut *queue)
            },
            Flavor::Unbounded => std::mem::take(&mut *queue),
        };

        Drain { queue, _phantom: PhantomData }
    }
}

// TODO debug
impl<T: std::fmt::Debug> IntoIterator for Receiver<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { receiver: self }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_disconnected();
    }
}

/// An iterator over the items received from a channel.
pub struct Iter<'a, T> {
    receiver: &'a Receiver<T>,
}

// TODO debug
impl<'a, T: std::fmt::Debug> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

/// An non-blocking iterator over the items received from a channel.
pub struct TryIter<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Iterator for TryIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.try_recv().ok()
    }
}

/// An fixed-sized iterator over the items drained from a channel.
#[derive(Debug)]
pub struct Drain<'a, T> {
    queue: VecDeque<T>,
    /// A phantom field used to constrain the lifetime of this iterator. We do this because the
    /// implementation may change and we don't want to unintentionally constrain it. Removing this
    /// lifetime later is a possibility.
    _phantom: PhantomData<&'a ()>,
}

impl<'a, T> Iterator for Drain<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.queue.pop_front()
    }
}

impl<'a, T> ExactSizeIterator for Drain<'a, T> {
    fn len(&self) -> usize {
        self.queue.len()
    }
}

/// An owned iterator over the items received from a channel.
pub struct IntoIter<T> {
    receiver: Receiver<T>,
}

// tODO debug
impl<T: std::fmt::Debug> Iterator for IntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.receiver.recv().ok()
    }
}

/// Create a channel with no maximum capacity.
///
/// Create an unbounded channel with a [`Sender`] and [`Receiver`] connected to each end
/// respectively. Values sent in one end of the channel will be received on the other end. The
/// channel is thread-safe, and both sender and receiver may be sent to threads as necessary. In
/// addition, [`Sender`] may be cloned.
///
/// # Examples
/// ```
/// let (tx, rx) = flume::unbounded();
///
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv().unwrap(), 42);
/// ```
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared::new(None));
    (
        Sender {
            shared: shared.clone(),
            unblock_signal: None,
            _phantom_cell: UnsafeCell::new(())
        },
        Receiver {
            shared,
            unblock_signal: Arc::new(Signal::default()),
            buffer: RefCell::new(VecDeque::new()),
            _phantom_cell: UnsafeCell::new(())
        },
    )
}

/// Create a channel with a maximum capacity.
///
/// Create a bounded channel with a [`Sender`] and [`Receiver`] connected to each end
/// respectively. Values sent in one end of the channel will be received on the other end. The
/// channel is thread-safe, and both sender and receiver may be sent to threads as necessary. In
/// addition, [`Sender`] may be cloned.
///
/// Unlike an [`unbounded`] channel, if there is no space left for new messages, calls to
/// [`Sender::send`] will block (unblocking once a receiver has made space). If blocking behaviour
/// is not desired, [`Sender::try_send`] may be used.
///
/// Like `std::sync::mpsc`, `flume` supports 'rendezvous' channels. A bounded queue with a maximum
/// capacity of zero will block senders until a receiver is available to take the value.
///
/// # Examples
/// ```
/// let (tx, rx) = flume::bounded(32);
///
/// for i in 1..33 {
///     tx.send(i).unwrap();
/// }
/// assert!(tx.try_send(33).is_err());
///
/// assert_eq!(rx.try_iter().sum::<u32>(), (1..33).sum());
/// ```
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared::new(Some(cap)));
    (
        Sender {
            shared: shared.clone(),
            unblock_signal: Some(Arc::new(Signal::default())),
            _phantom_cell: UnsafeCell::new(())
        },
        Receiver {
            shared,
            unblock_signal: Arc::new(Signal::default()),
            buffer: RefCell::new(VecDeque::new()),
            _phantom_cell: UnsafeCell::new(())
        },
    )
}
