//! Futures and other types that allow asynchronous interaction with channels.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use crate::*;

/// A future  used to receive a value from the channel.
pub struct RecvFuture<'a, T> {
    recv: &'a mut Receiver<T>,
}

impl<'a, T> RecvFuture<'a, T> {
    pub(crate) fn new(recv: &mut Receiver<T>) -> RecvFuture<T> {
        RecvFuture { recv }
    }
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // On success, set the waker to none to avoid it being woken again in case that is wrong
        // TODO: `poll_recv` instead to prevent even spinning?
        #[cfg(feature = "receiver_buffer")]
        let mut buf = self.recv.buffer.borrow_mut();

        #[cfg(feature = "receiver_buffer")]
        {
            if let Some(msg) = buf.pop_front() {
                return Poll::Ready(Ok(msg));
            }
        }

        let mut inner = match self.recv.shared.inner.try_lock() {
            Some(guard) => guard,
            None => {
                cx.waker().wake_by_ref();
                return Poll::Pending; // TODO spin some first?
            },
        };

        let res = match inner.try_recv(&mut buf) {
            Ok(msg) => {
                self.recv.shared.wake_senders(inner);
                Ok(msg)
            },
            Err(e) => Err((inner, e))
        };

        let poll = match res {
            Ok(msg) => {
                self.recv.shared.with_inner(|mut inner| {
                    // Detach the waker
                    inner.recv_waker = None;
                    // Inform the sender that we no longer need waking
                    inner.listen_mode = 1;
                });
                Poll::Ready(Ok(msg))
            },
            Err((_, TryRecvError::Disconnected)) => Poll::Ready(Err(RecvError::Disconnected)),
            Err((mut inner, TryRecvError::Empty)) => {
                // Inform the sender that we need waking
                inner.recv_waker = Some(cx.waker().clone());
                inner.listen_mode = 2;
                Poll::Pending
            },
        };

        poll
    }
}
