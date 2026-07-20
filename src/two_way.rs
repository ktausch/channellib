//! symmatric two-way communication between threads
//!
//! This module is centered around the [`two_way::channel()`] function, which
//! creates a channel similar to that created by [`mpsc::channel()`] with two main differences:
//! 1. there is exactly one producer/consumer on each side of the channel
//! 2. the two sides of the channel are symmetric
//!
//! The two sides of the channel are populated by [`Communicator`] objects, which are
//! each essentially a [`mpsc::Sender`] and [`mpsc::Receiver`] bundled into one.
//!
//! # Examples
//!
//! Messages are built up into an internal queue, so messages
//! can be sent even if there are some in the queue:
//!
//! ```
//! use channellib::two_way;
//! let (first, second) = two_way::channel();
//! first.send(1).unwrap();
//! second.send(2).unwrap();
//! first.send(3).unwrap();
//! second.send(4).unwrap();
//! assert_eq!(first.recv().unwrap(), 2);
//! assert_eq!(first.recv().unwrap(), 4);
//! assert_eq!(second.recv().unwrap(), 1);
//! assert_eq!(second.recv().unwrap(), 3);
//! ```
//!
//! The two directions of the [`two_way::channel()`] can serve different data:
//!
//! ```
//! use channellib::two_way;
//! use std::thread;
//!
//! let (first, second) = two_way::channel();
//! let second_thread = thread::spawn(move || {
//!     while let Ok(received) = second.recv() {
//!         second.send(format!("roger {}", received)).unwrap();
//!     }
//! });
//! first.send(0).unwrap();
//! first.send(1).unwrap();
//! first.send(2).unwrap();
//! assert_eq!(&first.recv().unwrap(), "roger 0");
//! assert_eq!(&first.recv().unwrap(), "roger 1");
//! assert_eq!(&first.recv().unwrap(), "roger 2");
//! drop(first);
//! second_thread.join().unwrap();
//! ```
//!
//! The [`Communicator::drain()`] method can be used to ensure that no messages are missed:
//!
//! ```
//! use channellib::two_way;
//! use std::thread;
//!
//! // the two generic parameters are the data types sent first->second and second->first
//! let (first, second) = two_way::channel::<i32, usize>();
//! let second_thread = thread::spawn(move || {
//!     // blocks until first is dropped or drained
//!     let received = second.drain().collect::<Vec<_>>();
//!     assert_eq!(received.as_array().unwrap(), &[1, 2, 3]);
//! });
//! first.send(1).unwrap();
//! first.send(2).unwrap();
//! first.send(3).unwrap();
//! drop(first);
//! second_thread.join().unwrap();
//! ```
//!
//! [`two_way::channel()`]: channel()
//! [`mpsc::channel()`]: std::sync::mpsc::channel()
//! [`mpsc::Sender`]: std::sync::mpsc::Sender
//! [`mpsc::Receiver`]: std::sync::mpsc::Receiver
//! [`Communicator`]: Communicator
//! [`Communicator::drain()`]: Communicator::drain()
pub use std::sync::mpsc::{RecvError, SendError, TryRecvError};
use std::{
    iter,
    sync::mpsc::{self, Receiver, Sender},
};

/// One half of a [`two_way::channel`]. Has both sending and receiving capabilities
/// that are both connected to another `Communicator` instance.
///
/// [`two_way::channel`]: channel
#[must_use]
pub struct Communicator<T, U> {
    /// the sender the communicator uses to send data over the channel
    sender: Sender<T>,
    /// the receiver the communicator uses to receive data from the channel
    receiver: Receiver<U>,
}

impl<T, U> Communicator<T, U> {
    /// Sends the given data over the channel.
    ///
    /// # Errors
    ///
    /// Returns a [`SendError`] if the [`Communicator`] on
    /// the other side of the channel has been dropped.
    ///
    /// [`SendError`]: SendError
    /// [`Communicator`]: Communicator
    pub fn send(&self, data: T) -> Result<(), SendError<T>> {
        self.sender.send(data)
    }

    /// Blocks until another payload is received over the channel.
    ///
    /// # Errors
    ///
    /// If the other [`Communicator`] is dropped or consumed (see
    /// [`drain()`] for details), a [`RecvError`] is returned.
    ///
    /// [`drain()`]: Communicator::drain()
    /// [`RecvError`]: RecvError
    /// [`Communicator`]: Communicator
    pub fn recv(&self) -> Result<U, RecvError> {
        self.receiver.recv()
    }

    /// Attempts to receive a payload from the channel.
    ///
    /// # Errors
    ///
    /// If the other [`Communicator`] has not sent anything new but is still not dropped or
    /// consumed (see [`drain()`] for details), a [`TryRecvError::Empty`] is returned. If the
    /// other `Communicator` has been dropped, a [`TryRecvError::Disconnected`] is returned.
    ///
    /// [`Communicator`]: Communicator
    /// [`drain()`]: Communicator::drain()
    /// [`TryRecvError::Empty`]: TryRecvError::Empty
    /// [`TryRecvError::Disconnected`]: TryRecvError::Disconnected
    pub fn try_recv(&self) -> Result<U, TryRecvError> {
        self.receiver.try_recv()
    }

    /// Consumes self and listens through the channel until the other [`Communicator`] is dropped
    /// or consumed. When orchestrating two-way communication, in the absence of panics, ending
    /// each communicator's scope by calling `drain()` (and collecting the result) guarantees
    /// that no messages will be lost. **Note that this iterator's [`next()`] method will block if
    /// the queue is fully read but the other communicator hasn't been dropped or consumed!**
    ///
    /// ## Details on communication state after `drain` is called
    ///
    /// If both [`Communicator`] instances comprising a channel call `drain()`,
    /// there is no infinite loop because the call to `drain()` puts the consumed
    /// communicator into a special, listen-only state where it can never send messages.
    ///
    /// [`Communicator`]: Communicator
    /// [`next()`]: std::iter::Iterator::next()
    pub fn drain(self) -> impl Iterator<Item = U> {
        let Self { sender, receiver } = self;
        drop(sender);
        iter::from_fn(move || receiver.recv().ok())
    }
}

/// Creates a new two-way channel with a [`Communicator`] on each side.
/// The two different directions can accomodate two different types
/// of data (hence the two generic parameters `T` and `U`); but,
/// they are otherwise perfectly symmetric.
///
/// ```
/// use std::thread;
/// use channellib::two_way::{self, RecvError, SendError, TryRecvError};
///
/// let (first, second) = two_way::channel();
///
/// let separate_thread = thread::spawn(move || {
///     assert_eq!(first.recv(), Ok("456"));
///     first.send(123);
/// });
/// // the first-to-second message queue is empty because first
/// // doesn't send anything until it receives something
/// assert_eq!(second.try_recv(), Err(TryRecvError::Empty));
/// second.send("456");
/// assert_eq!(second.recv(), Ok(123));
/// // first drops after sending just one payload, so recv() returns an error
/// assert_eq!(second.recv(), Err(RecvError));
/// separate_thread.join().unwrap();
/// // when message fails to send, the returned Err returns back ownership of data
/// assert_eq!(second.send("789"), Err(SendError("789")));
/// ```
///
/// [`Communicator`]: Communicator
pub fn channel<T, U>() -> (Communicator<T, U>, Communicator<U, T>) {
    let (t_sender, t_receiver) = mpsc::channel();
    let (u_sender, u_receiver) = mpsc::channel();
    let first = Communicator {
        sender: t_sender,
        receiver: u_receiver,
    };
    let second = Communicator {
        sender: u_sender,
        receiver: t_receiver,
    };
    (first, second)
}

#[cfg(test)]
mod tests {
    use super as two_way;
    use std::thread;

    /// Tests the routine send and receive abilities of two-way channel communicators, including
    /// that messages can be sent while the message queue (in either direction) is non-empty.
    #[test]
    fn send_and_receive() {
        let (first, second) = two_way::channel();
        first.send(100).unwrap();
        second.send("abc").unwrap();
        assert_eq!(second.recv().unwrap(), 100);
        second.send("def").unwrap();
        assert_eq!(first.try_recv().unwrap(), "abc");
        assert_eq!(first.recv().unwrap(), "def");
    }

    /// Tests that sending from one communicator after the
    /// other one has dropped results in a SendError.
    #[test]
    fn send_when_other_dropped() {
        let (first, second) = two_way::channel::<i32, i32>();
        drop(second);
        assert_eq!(first.send(100).unwrap_err(), two_way::SendError(100));
    }

    /// Tests that receiving (via either `recv` or `try_recv`) after
    /// the other communicator has been dropped returns an error.
    #[test]
    fn receive_when_other_dropped() {
        let (first, second) = two_way::channel::<i32, i32>();
        second.send(1).unwrap();
        drop(second);
        assert_eq!(first.recv().unwrap(), 1);
        assert_eq!(first.recv().unwrap_err(), two_way::RecvError);
        assert_eq!(
            first.try_recv().unwrap_err(),
            two_way::TryRecvError::Disconnected
        );
    }

    /// Tests that using `try_recv()` when the message queue is empty leads to an error.
    #[test]
    fn try_receive_empty() {
        let (first, _second) = two_way::channel::<i32, i32>();
        assert_eq!(first.try_recv().unwrap_err(), two_way::TryRecvError::Empty);
    }

    // Tests that `drain()` can be used to listen until the message queue is empty.
    #[test]
    fn drain_works() {
        let (first, second) = two_way::channel::<i32, i32>();
        for value in 0..5 {
            first.send(value).unwrap();
        }
        drop(first);
        let received = second.drain().collect::<Vec<_>>();
        assert_eq!(received.as_array().unwrap(), &[0, 1, 2, 3, 4]);
    }

    /// Tests that messages sent after `drain()` will still be collected by drain.
    #[test]
    fn messages_sent_after_drain() {
        let (first, second) = two_way::channel::<i32, i32>();
        for value in 0..3 {
            first.send(value).unwrap();
        }
        let mut results = Vec::with_capacity(6);
        let mut iter = second.drain();
        results.extend((&mut iter).take(3));
        for value in 3..6 {
            first.send(value).unwrap();
        }
        drop(first);
        results.extend(iter);
        assert_eq!(results.as_array().unwrap(), &[0, 1, 2, 3, 4, 5]);
    }

    /// Tests that putting both sides of a `two_way::channel` into
    /// the `drain` state doesn't cause an infinite loop.
    #[test]
    fn both_drain_not_infinite_loop() {
        let (first, second) = two_way::channel::<i32, i32>();
        let first_thread = thread::spawn(move || {
            for value in 1..=3 {
                first.send(value).unwrap();
            }
            let results = first.drain().collect::<Vec<_>>();
            assert_eq!(results.as_array().unwrap(), &[10, 9, 8]);
        });
        let second_thread = thread::spawn(move || {
            for value in (8..=10).rev() {
                second.send(value).unwrap();
            }
            let results = second.drain().collect::<Vec<_>>();
            assert_eq!(results.as_array().unwrap(), &[1, 2, 3]);
        });
        first_thread.join().unwrap();
        second_thread.join().unwrap();
    }
}
