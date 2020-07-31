//! Futures and other types that allow asynchronous interaction with channels.

use crate::{signal::Signal, *};
use futures::{future::FusedFuture, stream::FusedStream, Sink, Stream};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct AsyncSignal(Waker);

impl Signal for AsyncSignal {
    fn fire(&self) {
        self.0.wake_by_ref()
    }
}

// TODO: Wtf happens with timeout races? Futures can still receive items when not being actively polled...
// Is this okay? I guess it must be? How do other async channel crates handle it?

enum Item<T> {
    Slot(Arc<Slot<T, AsyncSignal>>),
    ToSend(T),
}

impl<T: Unpin> Sender<T> {
    /// Asynchronously send a value into the channel, returning an error if the channel receiver has
    /// been dropped. If the channel is bounded and is full, this method will yield to the async runtime.
    pub fn send_async(&self, item: T) -> SendFut<T> {
        SendFut {
            shared: &self.shared,
            slot: Some(Item::ToSend(item)),
        }
    }

    /// Use this channel as an asynchronous item sink.
    pub fn sink(&self, item: T) -> ChannelSink<T> {
        ChannelSink(SendFut {
            shared: &self.shared,
            slot: Some(Item::ToSend(item)),
        })
    }
}

/// The future returned by [struct.Sender.html#method.send_async]. It should be polled in order to
/// send the item.
pub struct SendFut<'a, T: Unpin> {
    shared: &'a Shared<T>,
    // Only none after dropping
    slot: Option<Item<T>>,
}

impl<'a, T: Unpin> Drop for SendFut<'a, T> {
    fn drop(&mut self) {
        if let Some(Item::Slot(slot)) = self.slot.take() {
            let slot: Arc<Slot<T, dyn Signal>> = slot;
            wait_lock(&self.shared.chan)
                .sending
                .as_mut()
                .unwrap()
                .1
                .retain(|s| !Arc::ptr_eq(s, &slot));
        }
    }
}

impl<'a, T: Unpin> Future for SendFut<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(Item::Slot(slot)) = self.slot.as_ref() {
            // The item has been queued
            if slot.is_empty() {
                // The item has been received
                Poll::Ready(Ok(()))
            } else if self.shared.is_disconnected() {
                match self.slot.take().unwrap() {
                    // The item hasn't been sent yet and the receiver is disconnected, so return err
                    Item::ToSend(item) => Poll::Ready(Err(SendError(item))),
                    // The item has been queued but not necessarily received yet
                    Item::Slot(slot) => match slot.try_take() {
                        // The item hasn't been received yet
                        Some(item) => Poll::Ready(Err(SendError(item))),
                        // The item has already been received
                        None => Poll::Ready(Ok(())),
                    },
                }
            } else {
                // Slot is full and isn't disconnected - wait for it to be received
                Poll::Pending
            }
        } else {
            // The item has not been queued yet, so begin the send
            self.shared
                .send(
                    // item
                    match self.slot.take().unwrap() {
                        Item::ToSend(item) => item,
                        Item::Slot(_) => return Poll::Ready(Ok(())),
                    },
                    // should_wait
                    true,
                    // make_signal
                    || AsyncSignal(cx.waker().clone()),
                    // do_wait
                    |slot| {
                        self.slot = Some(Item::Slot(slot));
                        Poll::Pending
                    },
                )
                .map(|r| {
                    r.map_err(|err| match err {
                        TrySendTimeoutError::Disconnected(msg) => SendError(msg),
                        _ => unreachable!(),
                    })
                })
        }
    }
}

impl<'a, T: Unpin> FusedFuture for SendFut<'a, T> {
    fn is_terminated(&self) -> bool {
        self.shared.is_disconnected()
    }
}

/// The sink returned by [struct.Sender.html#method.sink]. It should be polled in order to
/// send the item.
pub struct ChannelSink<'a, T: Unpin>(SendFut<'a, T>);

impl<'a, T: Unpin> Sink<T> for ChannelSink<'a, T> {
    type Error = SendError<T>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.0 = SendFut {
            shared: &self.0.shared,
            slot: Some(Item::ToSend(item)),
        };

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Receiver<T> {
    /// Asynchronously wait for an incoming value from the channel associated with this receiver,
    /// returning an error if all channel senders have been dropped.
    ///
    /// If the returned future is polled and returns `Pending`, then it should be polled until
    /// completion, or else one message may be dropped. This is true for all subsequent polls.
    pub fn recv_async(&self) -> RecvFut<T> {
        RecvFut::new(&self.shared)
    }

    /// Use this channel as an asynchronous stream of items.
    pub fn stream(&self) -> ChannelStream<T> {
        ChannelStream(RecvFut::new(&self.shared))
    }
}

/// The future returned by [struct.Receiver.html#method.recv_async]. It will do nothing unless polled.
///
/// If this future is polled and returns `Pending`, then it should be polled until completion, or else
/// one message may be dropped. This is true for all subsequent polls.
pub struct RecvFut<'a, T> {
    shared: &'a Shared<T>,
    slot: Option<Arc<Slot<T, AsyncSignal>>>,
}

impl<'a, T> RecvFut<'a, T> {
    fn new(shared: &'a Shared<T>) -> Self {
        Self { shared, slot: None }
    }
}

impl<'a, T> Drop for RecvFut<'a, T> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let slot: Arc<Slot<T, dyn Signal>> = slot;
            wait_lock(&self.shared.chan)
                .waiting
                .retain(|s| !Arc::ptr_eq(s, &slot));
        }
    }
}

impl<'a, T> Future for RecvFut<'a, T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(slot) = self.slot.as_ref() {
            match slot.try_take() {
                Some(item) => Poll::Ready(Ok(item)),
                None => {
                    if self.shared.is_disconnected() {
                        Poll::Ready(Err(RecvError::Disconnected))
                    } else {
                        Poll::Pending
                    }
                }
            }
        } else {
            self.shared
                .recv(
                    // should_wait
                    true,
                    // make_signal
                    || AsyncSignal(cx.waker().clone()),
                    // do_wait
                    |slot| {
                        self.slot = Some(slot);
                        Poll::Pending
                    },
                )
                .map(|r| {
                    r.map_err(|err| match err {
                        TryRecvTimeoutError::Disconnected => RecvError::Disconnected,
                        _ => unreachable!(),
                    })
                })
        }
    }
}

impl<'a, T> FusedFuture for RecvFut<'a, T> {
    fn is_terminated(&self) -> bool {
        self.shared.is_disconnected()
    }
}

pub struct ChannelStream<'a, T>(RecvFut<'a, T>);

impl<'a, T> Stream for ChannelStream<'a, T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(item) => {
                // Replace the recv future for every item we receive
                self.0 = RecvFut::new(self.0.shared);
                Poll::Ready(item.ok())
            }
        }
    }
}

impl<'a, T> FusedStream for ChannelStream<'a, T> {
    fn is_terminated(&self) -> bool {
        self.0.shared.is_disconnected()
    }
}
