//! acknowledged communication between [`Speaker`] and [`Listener`]
//!
//! This module is centered around the [`acknowledge::channel()`] function (and related, more
//! customizable, [`acknowledge::custom_channel()`] function), which creates a channel similar to
//! that created by [`mpsc::channel()`] with two main differences:
//! 1. there is exactly one producer (on one side of the channel) and one consumer
//!    (on the other side of the channel)
//! 2. the producer side (called a [`Speaker`] in this module) can read acknowledgements sent
//!    by the consumer side (called a [`Listener`]) to verify that the message has been received
//!
//! The [`Speaker`] and [`Listener`] structs both use [`two_way::Communicator`] structs to send
//! and receive messages.
//!
//! ```
//! use channellib::acknowledge;
//!
//! let (speaker, listener) = acknowledge::channel();
//! speaker.send(1).unwrap();
//! assert_eq!(speaker.read_acknowledgement().unwrap(), None);
//! assert_eq!(listener.recv().unwrap(), 1);
//! // there's an acknowledgement now!
//! speaker.read_acknowledgement().unwrap().unwrap();
//! ```
//!
//! A main use-case of the acknowledge channel is to ensure that sent messages are actually
//! received (with [`mpsc::channel()`], the [`Sender`] could send something and not see an error
//! because the [`Receiver`] hasn't been dropped; but, the [`Receiver`] _could_ be dropped between
//! [`Sender::send()`] and [`Receiver::recv()`], which would lead to the message not being
//! attended to.
//!
//! ```
//! use channellib::acknowledge;
//! use std::{collections::VecDeque, thread, time::Duration};
//!
//! // in reality, you'd pull from a database or something
//! fn get_ids() -> impl Iterator<Item = u32> { 0.. }
//!
//! // stubs for two steps of processing that have dedicated threads
//! fn step_1(id: u32) { thread::sleep(Duration::from_micros(10)); }
//! fn step_2(id: u32) {}
//!
//! fn set_aside_for_later_step_2_processing(queue: &[u32]) {}
//!
//! let (speaker, listener) = acknowledge::channel();
//! let step_1_thread = thread::spawn(move || {
//!     let mut queue = VecDeque::new();
//!     for id in get_ids().take(200) {
//!         while let Ok(Some(_)) = speaker.read_acknowledgement() {
//!             queue.pop_front();
//!         }
//!         step_1(id);
//!         queue.push_back(id);
//!         let Ok(()) = speaker.send(id) else { break; };
//!     }
//!     speaker.drain().for_each(|_| { queue.pop_front(); });
//!     set_aside_for_later_step_2_processing(queue.make_contiguous());
//!     queue.len()
//! });
//! let step_2_thread = thread::spawn(move || {
//!     for (id, _) in listener.drain().take(100) {
//!         step_2(id);
//!     }
//! });
//! let num_unprocessed = step_1_thread.join().unwrap();
//! step_2_thread.join().unwrap();
//! assert!(num_unprocessed <= 100);
//! ```
//!
//! [`acknowledge::channel()`]: channel()
//! [`acknowledge::custom_channel()`]: custom_channel()
//! [`two_way::Communicator`]: Communicator
//! [`mpsc::channel()`]: std::sync::mpsc::channel()
//! [`Speaker`]: Speaker
//! [`Listener`]: Listener
//! [`Sender`]: std::sync::mpsc::Sender
//! [`Receiver`]: std::sync::mpsc::Receiver
//! [`Sender::send()`]: std::sync::mpsc::Sender::send()
//! [`Receiver::recv()`]: std::sync::mpsc::Receiver::recv()
use crate::two_way::{self, Communicator};

use std::{
    cell::RefCell,
    error::Error,
    fmt::{Debug, Display},
    iter,
    sync::mpsc,
};

/// Error returned if [`Speaker::send()`] is called after the corresponding [`Listener`] is dropped.
///
/// [`Speaker::send()`]: Speaker::send()
/// [`Listener`]: Listener
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> Debug for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("data was sent after listener was dropped")
    }
}

impl<T> Error for SendError<T> {}

/// Producer/sender of objects through an [`acknowledge::channel()`].
///
/// [`acknowledge::channel()`]: channel()
#[must_use]
pub struct Speaker<T, U> {
    /// speaker's side of the underlying two way channel
    communicator: Communicator<T, U>,
}

/// An error that occurs while the [`Speaker`] is attempting to check acknowledgements sent by the
/// [`Listener`]. The only reason this could happen is if the [`Listener`] has been dropped.
///
/// [`Speaker`]: Speaker
/// [`Listener`]: Listener
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReadAcknowledgementError {
    ListenerDropped,
}

impl Display for ReadAcknowledgementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "listener was dropped before another acknowledgement was written",
        )
    }
}

impl Error for ReadAcknowledgementError {}

impl<T, U> Speaker<T, U> {
    /// Sends the given data to the listener, returning a [`ReadAcknowledgementError`]
    /// if the [`Listener`] has been dropped.
    ///
    /// [`ReadAcknowledgementError`]
    /// [`Listener`]: Listener
    pub fn send(&self, data: T) -> Result<(), SendError<T>> {
        self.communicator
            .send(data)
            .map_err(|mpsc::SendError(data)| SendError(data))
    }

    /// Attempts to read an acknowledgement from the [`Listener`]
    ///
    /// NOTE: **This method is non-blocking.** See [`blocking_read_acknowledgement()`]
    ///       if you'd like to block until an acknowledgement is received or
    ///       the [`Listener`] has been dropped.
    ///
    /// Returns:
    ///
    /// - `Ok(Some(payload))` if [`Listener`] has sent an unread acknowledgement
    /// - `Ok(None)` if [`Listener`] hasn't been dropped but there are no unread acknowledgements
    /// - `Err(read_acknowledgement_error)` if [`Listener`] has been dropped
    ///
    /// [`Listener`]: Listener
    /// [`blocking_read_acknowledgement()`]: Speaker::blocking_read_acknowledgement()
    pub fn read_acknowledgement(
        &self,
    ) -> Result<Option<U>, ReadAcknowledgementError> {
        match self.communicator.try_recv() {
            Ok(payload) => Ok(Some(payload)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(ReadAcknowledgementError::ListenerDropped)
            }
        }
    }

    /// Reads an acknowledgement from the [`Listener`], returning the next
    /// acknowledgement if the [`Listener`] sends at least one more or a
    /// [`ReadAcknowledgementError`] if the [`Listener`] has been dropped.
    ///
    /// NOTE: This method is blocking. See [`read_acknowledgement()`] if you'd like to
    ///       not wait for something to change on the listener side before returning.
    ///
    /// [`Listener`]: Listener
    /// [`read_acknowledgement()`]: Speaker::read_acknowledgement()
    /// [`ReadAcknowledgementError`]: ReadAcknowledgementError
    pub fn blocking_read_acknowledgement(
        &self,
    ) -> Result<U, ReadAcknowledgementError> {
        self.communicator
            .recv()
            .map_err(|_| ReadAcknowledgementError::ListenerDropped)
    }

    /// Reads all pending acknowledgements from the [`Listener`]. The variant of [`Result`] that
    /// is returned depends on status of [`Listener`] when the last acknowledgement is read. If
    /// the [`Listener`] has been dropped, [`Err`] is returned; otherwise [`Ok`] is returned.
    ///
    /// NOTE: This method is non-blocking because otherwise, it would only
    ///       return after the [`Listener`] is dropped, which is not desired in
    ///       most use-cases where the [`Speaker`] is borrowed. If you wish to
    ///       cease speaking and only listen for acknowledgements until the listener
    ///       is dropped, consume self with [`drain()`].
    ///
    /// [`Result`]: std::result::Result
    /// [`Ok`]: std::result::Result::Ok
    /// [`Err`]: std::result::Result::Err
    /// [`Listener`]: Listener
    /// [`Speaker`]: Speaker
    /// [`drain()`]: Speaker::drain
    pub fn read_acknowledgements(&self) -> Result<Vec<U>, Vec<U>> {
        let mut acknowledgements = Vec::new();
        loop {
            match self.read_acknowledgement() {
                Ok(Some(acknowledgement)) => {
                    acknowledgements.push(acknowledgement);
                }
                Ok(None) => {
                    break Ok(acknowledgements);
                }
                Err(ReadAcknowledgementError::ListenerDropped) => {
                    break Err(acknowledgements);
                }
            }
        }
    }

    /// Consumes the speaker and returns an iterator over acknowledgements sent by the listener
    /// until it drops. Note that if [`Speaker::drain()`] and [`Listener::drain()`] are both
    /// called, it *will not* cause an infinite loop.
    pub fn drain<'a>(self) -> impl 'a + Iterator<Item = U>
    where
        Self: 'a,
    {
        self.communicator.drain()
    }
}

/// Consumer/receiver of objects through an [`acknowledge::channel()`].
///
/// [`acknowledge::channel()`]: channel()
#[must_use]
pub struct Listener<T, U, F> {
    /// listener's side of the underlying two-way channel
    communicator: Communicator<U, T>,
    /// the function that creates acknowledgement types from references to data types
    function: RefCell<F>,
}

/// An error that occurs when a [`Listener`] fails to receive a message
/// on blocking reads (i.e. with the [`recv()`] method)
///
/// [`Listener`]: Listener
/// [`recv()`]: Listener::recv()
#[derive(PartialEq, Eq)]
pub enum RecvError<T, U> {
    /// the [`Speaker`] was dropped between sending the loaded
    /// payload and sending the acknowledgement
    ///
    /// [`Speaker`]: Speaker
    AcknowledgementError {
        /// the payload that couldn't be acknowledged
        payload: T,
        /// the acknowledgement that couldn't be sent successfully
        failed_acknowledgement: U,
    },
    /// the [`Speaker`] was dropped before sending another payload
    ///
    /// [`Speaker`]: Speaker
    SpeakerDropped,
}

impl<T, U> Debug for RecvError<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgementError { .. } => {
                f.write_str("RecvError::AcknowledgementError")
            }
            Self::SpeakerDropped => f.write_str("RecvError::SpeakerDropped"),
        }
    }
}

impl<T, U> Display for RecvError<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgementError { .. } => {
                f.write_str("data was received, but speaker dropped before acknowledgement could be sent")
            },
            Self::SpeakerDropped => {
                f.write_str("speaker dropped before sending anymore payloads")
            },
        }
    }
}

impl<T, U> Error for RecvError<T, U> {}

/// An error that occurs when a [`Listener`] fails to receive a message
/// on non-blocking reads (i.e. with the [`try_recv()`] method)
///
/// [`Listener`]: Listener
/// [`try_recv()`]: Listener::try_recv()
#[derive(PartialEq, Eq)]
pub enum TryRecvError<T, U> {
    /// the [`Speaker`] was dropped between sending the loaded payload
    /// and sending the acknowledgement
    ///
    /// [`Speaker`]: Speaker
    AcknowledgementError {
        /// the payload that couldn't be acknowledged
        payload: T,
        /// the acknowledgement that couldn't be sent successfully
        failed_acknowledgement: U,
    },
    /// the [`Speaker`] was dropped before try_recv tried to get a payload
    ///
    /// [`Speaker`]: Speaker
    SpeakerDropped,
    /// the [`Speaker`] hasn't been dropped but hasn't sent another payload yet
    ///
    /// [`Speaker`]: Speaker
    NoPayloadsAvailable,
}

impl<T, U> Debug for TryRecvError<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgementError { .. } => {
                f.write_str("TryRecvError::AcknowledgementError")
            }
            Self::SpeakerDropped => f.write_str("TryRecvError::SpeakerDropped"),
            Self::NoPayloadsAvailable => {
                f.write_str("TryRecvError::NoPayloadsAvailable")
            }
        }
    }
}

impl<T, U> Display for TryRecvError<T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AcknowledgementError { .. } => {
                f.write_str("data was received, but speaker dropped before acknowledgement could be sent")
            },
            Self::SpeakerDropped => {
                f.write_str("speaker dropped before sending anymore payloads")
            },
            Self::NoPayloadsAvailable => {
                f.write_str("no unread payloads from still connected speaker")
            }
        }
    }
}

impl<T, U> Error for TryRecvError<T, U> {}

impl<T, U, F> Listener<T, U, F>
where
    F: FnMut(&T) -> U,
{
    /// Attempts to acknowledge the given payload received from the speaker. If the acknowledgement
    /// fails to send, it is because the [`Speaker`] has been dropped.
    ///
    /// [`Speaker`]: Speaker
    fn acknowledge(&self, payload: &T) -> Result<(), two_way::SendError<U>> {
        let acknowledgement = (self.function.borrow_mut())(&payload);
        self.communicator.send(acknowledgement)
    }

    /// Reads data sent by the speaker, returning the payload on success or a [`RecvError`] if the
    /// [`Speaker`] has been dropped (the variant of [`RecvError`] depends on whether there are
    /// messages in the queue).
    ///
    /// NOTE: This method blocks until the speaker sends data or the speaker is dropped.
    ///       If you wish to read without blocking the thread, use [`Listener::try_recv()`]
    ///
    /// [`Speaker`]: Speaker
    /// [`RecvError`]: RecvError
    /// [`Listener::try_recv()`]: Listener::try_recv()
    pub fn recv(&self) -> Result<T, RecvError<T, U>> {
        match self.communicator.recv() {
            Ok(payload) => match self.acknowledge(&payload) {
                Ok(()) => Ok(payload),
                Err(send_error) => Err(RecvError::AcknowledgementError {
                    payload,
                    failed_acknowledgement: send_error.0,
                }),
            },
            Err(mpsc::RecvError) => Err(RecvError::SpeakerDropped),
        }
    }

    /// Reads data sent by the speaker in a non-blocking fashion, returning the payload on success
    /// or a [`TryRecvError`] if the message queue is empty or the [`Speaker`] has been dropped.
    ///
    /// NOTE: If you wish to block until a payload is received, use [`Listener::recv()`]
    ///
    /// [`TryRecvError`]: TryRecvError
    /// [`Speaker`]: Speaker
    /// [`Listener::recv()`]: Listener::recv()
    pub fn try_recv(&self) -> Result<T, TryRecvError<T, U>> {
        match self.communicator.try_recv() {
            Ok(payload) => match self.acknowledge(&payload) {
                Ok(()) => Ok(payload),
                Err(send_error) => Err(TryRecvError::AcknowledgementError {
                    payload,
                    failed_acknowledgement: send_error.0,
                }),
            },
            Err(mpsc::TryRecvError::Disconnected) => {
                Err(TryRecvError::SpeakerDropped)
            }
            Err(mpsc::TryRecvError::Empty) => {
                Err(TryRecvError::NoPayloadsAvailable)
            }
        }
    }

    /// Consumes self to iterate over all payloads sent by the [`Speaker`]. Each call to [`next()`]
    /// on the output will perform a blocking read from the message queue and then return a tuple
    /// with the received message of type `T` and a [`Result`] object whose [`Ok`] type is empty
    /// and whose [`Err`] type is an acknowledgement that failed to send (because the [`Speaker`]
    /// has been dropped).
    ///
    /// Note that if [`Speaker::drain()`] and [`Listener::drain()`] are both
    /// called, it *will not* cause an infinite loop.
    ///
    /// [`Speaker`]: Speaker
    /// [`next()`]: std::iter::Iterator::next()
    /// [`Result`]: std::result::Result
    /// [`Ok`]: std::result::Result::Ok
    /// [`Err`]: std::result::Result::Err
    /// [`Speaker::drain()`]: Speaker::drain()
    /// [`Listener::drain()`]: Listener::drain()
    pub fn drain<'a>(self) -> impl 'a + Iterator<Item = (T, Result<(), U>)>
    where
        Self: 'a,
    {
        iter::from_fn(move || match self.recv() {
            Ok(payload) => Some((payload, Ok(()))),
            Err(RecvError::AcknowledgementError {
                payload,
                failed_acknowledgement,
            }) => Some((payload, Err(failed_acknowledgement))),
            Err(RecvError::SpeakerDropped) => None,
        })
    }
}

/// Creates an acknowledge channel with a custom acknowledgement function.
///
/// The acknowledgement function is the function that determines with what payload the [`Listener`]
/// should respond to a given payload from the [`Speaker`]. For example, if the [`Speaker`] is
/// sending `u8`s, you may want the [`Listener`] to acknowledge the message by repeating it:
///
/// ```
/// use channellib::acknowledge;
/// let (speaker, listener) = acknowledge::custom_channel(|&byte| byte);
/// speaker.send(1).unwrap();
/// assert_eq!(speaker.read_acknowledgement().unwrap(), None);
/// assert_eq!(listener.recv().unwrap(), 1);
/// assert_eq!(speaker.read_acknowledgement().unwrap().unwrap(), 1);
/// ```
///
/// You may also want to build a custom acknowledgement struct to return:
///
/// ```
/// use channellib::acknowledge;
/// use std::{thread, time::{Duration, Instant}};
///
/// struct Payload<T> {
///     sent: T,
///     when: Instant,
/// }
///
/// struct Acknowledgement<T> {
///     received: T,
///     latency: Duration,
/// }
/// let (speaker, listener) = acknowledge::custom_channel(|&Payload {sent, when}| {
///     Acknowledgement {received: sent, latency: when.elapsed()}
/// });
/// speaker.send(Payload {sent: 1, when: Instant::now()}).unwrap();
/// thread::sleep(Duration::from_micros(100));
/// assert_eq!(listener.recv().unwrap().sent, 1);
/// let Acknowledgement {received, latency} = speaker.blocking_read_acknowledgement().unwrap();
/// assert_eq!(received, 1);
/// assert!(latency >= Duration::from_micros(100));
/// ```
///
/// [`Speaker`]: Speaker
/// [`Listener`]: Listener
pub fn custom_channel<T, U, F>(
    function: F,
) -> (Speaker<T, U>, Listener<T, U, F>)
where
    F: FnMut(&T) -> U,
{
    let (speaker_communicator, listener_communicator) = two_way::channel();
    let speaker = Speaker {
        communicator: speaker_communicator,
    };
    let listener = Listener {
        communicator: listener_communicator,
        function: RefCell::new(function),
    };
    (speaker, listener)
}

/// Creates a basic acknowledge channel, consisting of a [`Speaker`] and a [`Listener`], where the
/// [`Listener`] sends acknowledgements in the form of `()`.
///
/// ```
/// use channellib::acknowledge;
/// let (speaker, listener) = acknowledge::channel();
/// speaker.send(1).unwrap();
/// assert_eq!(speaker.read_acknowledgement().unwrap(), None);
/// assert_eq!(listener.recv().unwrap(), 1);
/// assert_eq!(speaker.read_acknowledgement().unwrap(), Some(()));
/// ```
///
/// [`Speaker`]: Speaker
/// [`Listener`]: Listener
pub fn channel<T>() -> (Speaker<T, ()>, Listener<T, (), impl FnMut(&T)>) {
    custom_channel(|_: &T| ())
}

#[cfg(test)]
mod tests {
    use super as acknowledge;
    use std::{
        assert_matches,
        cell::RefCell,
        iter, thread,
        time::{Duration, Instant},
    };

    /// Tests the basic functionality of the acknowledge channel, including ID generation,
    /// acknowledgement computation, and [`Speaker::send()`], [`Speaker::read_acknowledgement()`],
    /// [`Speaker::read_acknowledgements()`], [`Listener::recv()`], and [`Listener::try_recv()`]
    #[test]
    fn acknowledge_channel_happy_path() {
        let (speaker, listener) = acknowledge::custom_channel(|&x| x + 1);
        assert_matches!(
            listener.try_recv(),
            Err(acknowledge::TryRecvError::NoPayloadsAvailable)
        );
        assert_eq!(speaker.read_acknowledgement(), Ok(None));
        speaker.send(10).unwrap();
        speaker.send(11).unwrap();
        speaker.send(12).unwrap();
        assert_matches!(listener.try_recv(), Ok(10));
        assert_matches!(listener.recv(), Ok(11));
        assert_eq!(speaker.read_acknowledgement(), Ok(Some(11)));
        assert_matches!(listener.recv(), Ok(12));
        let acknowledgements: [i32; 2] =
            speaker.read_acknowledgements().unwrap().try_into().unwrap();
        assert_eq!(acknowledgements, [12, 13]);
        assert_eq!(speaker.read_acknowledgement(), Ok(None));
        assert!(speaker.read_acknowledgements().unwrap().is_empty());
    }

    /// Tests that if the speaker is dropped when there are no unread payloads,
    /// both [`Listener::recv()`] and [`Listener::try_recv()`] return the `SpeakerDropped`
    /// variant of their respective error types.
    #[test]
    fn acknowledge_speaker_dropped_early() {
        let (speaker, listener) = acknowledge::channel::<usize>();
        drop(speaker);
        assert_matches!(
            listener.recv(),
            Err(acknowledge::RecvError::SpeakerDropped)
        );
        assert_matches!(
            listener.try_recv(),
            Err(acknowledge::TryRecvError::SpeakerDropped)
        );
    }

    /// Tests that if the listener is dropped, the [`Speaker::send()`] method
    /// returns a `SendError` with the payload that failed to send.
    #[test]
    fn acknowledge_listener_dropped_early() {
        let (speaker, listener) = acknowledge::channel();
        drop(listener);
        assert_matches!(speaker.send(1), Err(acknowledge::SendError(1)));
    }

    /// Tests that if the speaker attempts to read an acknowledgement when the listener
    /// is dropped, a [`ReadAcknowledgementError::ListenerDropped`] error is returned.
    #[test]
    fn listener_dropped_before_acknowledgement_read() {
        let (speaker, listener) = acknowledge::channel::<i32>();
        drop(listener);
        assert_matches!(
            speaker.read_acknowledgement(),
            Err(acknowledge::ReadAcknowledgementError::ListenerDropped)
        );
    }

    /// Tests that if the speaker dropped between when it sent an unread
    /// payload and an acknowledgement is sent by [`Listener::recv()`].
    #[test]
    fn acknowledgement_error_recv() {
        let (speaker, listener) = acknowledge::channel();
        assert_eq!(speaker.send(1), Ok(()));
        drop(speaker);
        assert_matches!(
            listener.recv(),
            Err(acknowledge::RecvError::AcknowledgementError {
                payload: 1,
                failed_acknowledgement: (),
            })
        );
    }

    /// Tests that if the speaker dropped between when it sent an unread
    /// payload and an acknowledgement is sent by [`Listener::try_recv()`].
    #[test]
    fn acknowledgement_error_try_recv() {
        let (speaker, listener) = acknowledge::channel();
        assert_eq!(speaker.send(1), Ok(()));
        drop(speaker);
        assert_matches!(
            listener.try_recv(),
            Err(acknowledge::TryRecvError::AcknowledgementError {
                payload: 1,
                failed_acknowledgement: ()
            })
        );
    }

    /// Tests that when the listener is dropped before `Speaker::read_acknowledgements()`
    /// is called, it will return an `Err(vec_of_acknowledgements)`
    #[test]
    fn listener_dropped_after_short_burst() {
        let (speaker, listener) = acknowledge::channel();
        assert_eq!(speaker.send(1), Ok(()));
        assert_eq!(speaker.send(2), Ok(()));
        assert_eq!(listener.recv(), Ok(1));
        drop(listener);
        let acknowledgements = speaker.read_acknowledgements().unwrap_err();
        let acknowledgements = acknowledgements.as_array().unwrap();
        assert_eq!(acknowledgements, &[()]);
    }

    /// Tests that [`Speaker::drain()`] creates an iterator that:
    /// 1. consumes the speaker
    /// 2. returns acknowledgements sent by the listener
    /// 3. each `next()` call waits until either the acknowledgement is
    ///    received or the listener is dropped
    /// 4. won't return `None` until the listener is dropped
    #[test]
    fn test_speaker_drain() {
        let (speaker, listener) = acknowledge::channel();
        assert_eq!(
            listener.try_recv(),
            Err(acknowledge::TryRecvError::NoPayloadsAvailable)
        );
        let speaker_thread = thread::spawn(move || {
            assert_eq!(speaker.send(1), Ok(()));
            assert_eq!(speaker.send(2), Ok(()));
            assert_eq!(speaker.send(3), Ok(()));
            let mut acknowledgements = speaker.drain();
            let mut none_time = None;
            let acknowledgements = iter::from_fn(|| {
                let start = Instant::now();
                let acknowledgement = acknowledgements.next();
                let duration = start.elapsed();
                match acknowledgement {
                    Some(_) => Some(duration),
                    None => {
                        none_time.get_or_insert(duration);
                        None
                    }
                }
            });
            let acknowledgements = acknowledgements.collect::<Vec<_>>();
            assert_eq!(acknowledgements.len(), 3);
            // no strict ability to know how long first call to next() took
            assert!(acknowledgements[1].as_micros() > 50);
            assert!(acknowledgements[2].as_micros() > 150);
            assert!(none_time.unwrap().as_micros() > 250);
        });
        let listener_thread = thread::spawn(move || {
            assert_eq!(listener.recv(), Ok(1));
            thread::sleep(Duration::from_micros(100));
            assert_eq!(listener.recv(), Ok(2));
            thread::sleep(Duration::from_micros(200));
            assert_eq!(listener.recv(), Ok(3));
            thread::sleep(Duration::from_micros(300));
        });
        speaker_thread.join().unwrap();
        listener_thread.join().unwrap();
    }

    /// Tests that [`Listener::drain()`] creates an iterator that:
    /// 1. consumes the listener
    /// 2. returns payloads sent by the speaker
    /// 3. each `next()` call waits until either a payload is received or the speaker is dropped
    /// 4. each payload returned will be acknowledged if possible
    /// 5. won't return `None` until the listener is dropped
    #[test]
    fn test_listener_drain() {
        let (speaker, listener) = acknowledge::channel();
        let speaker_thread = thread::spawn(move || {
            thread::sleep(Duration::from_micros(100));
            speaker.send(1).unwrap();
            thread::sleep(Duration::from_micros(100));
            speaker.send(2).unwrap();
            thread::sleep(Duration::from_micros(200));
            speaker.send(3).unwrap();
            thread::sleep(Duration::from_micros(300));
            assert_eq!(speaker.read_acknowledgements().unwrap().len(), 3);
        });
        let listener_thread = thread::spawn(move || {
            let mut payloads = listener.drain();
            let mut none_time = None;
            let payloads = iter::from_fn(|| {
                let start = Instant::now();
                let payload = payloads.next();
                let duration = start.elapsed();
                match payload {
                    Some(_) => Some(duration),
                    None => {
                        none_time.get_or_insert(duration);
                        None
                    }
                }
            });
            let payloads = payloads.collect::<Vec<_>>();
            assert!(payloads[1].as_micros() > 50);
            assert!(payloads[2].as_micros() > 150);
            assert!(none_time.unwrap().as_micros() > 250);
        });
        listener_thread.join().unwrap();
        speaker_thread.join().unwrap();
    }

    /// Tests that if the listener is still draining after the speaker is dropped, then
    /// acknowledgements that fail to send are marked by `Err`s being returned as the
    /// second element of the tuple.
    #[test]
    fn test_listener_drain_after_speaker_drops() {
        let (speaker, listener) = acknowledge::channel();
        speaker.send(1).unwrap();
        let mut results = Vec::with_capacity(3);
        let mut drain = listener.drain();
        results.push(drain.next().unwrap());
        speaker.send(2).unwrap();
        speaker.send(3).unwrap();
        drop(speaker);
        results.extend(drain);
        assert_eq!(
            results.as_array().unwrap(),
            &[(1, Ok(())), (2, Err(())), (3, Err(()))]
        )
    }

    /// Tests that putting both `Speaker` and `Listener` into a `drain()` state does not
    /// lead to an infinite loop, i.e. both iterators can be consumed in finite time.
    #[test]
    fn test_both_drain_not_infinite_loop() {
        let (speaker, listener) = acknowledge::custom_channel(|&x| x + 1);
        speaker.send(1).unwrap();
        let listener_thread = thread::spawn(move || {
            let payloads = listener.drain().collect::<Vec<_>>();
            // all acknowledgements are `Some` because even messages sent after `Listener::drain()`
            // is called are acknowledged.
            assert_eq!(
                payloads.as_array().unwrap(),
                &[(1, Ok(())), (2, Ok(()))]
            );
        });
        speaker.send(2).unwrap();
        let acknowledgements = speaker.drain().collect::<Vec<_>>();
        assert_eq!(acknowledgements.as_array().unwrap(), &[2, 3]);
        listener_thread.join().unwrap();
    }

    /// Tests that the `Listener` correctly handles a mutable acknowledge function.
    #[test]
    fn mutable_acknowledge_function() {
        let items_responded_to = RefCell::new(0usize);
        let (speaker, listener) = acknowledge::custom_channel(move |_| {
            *items_responded_to.borrow_mut() += 1;
            *items_responded_to.borrow()
        });
        let speaker_thread = thread::spawn(move || {
            speaker.send(3).unwrap();
            speaker.send(2).unwrap();
            speaker.send(1).unwrap();
            let acknowledgements = speaker.drain().collect::<Vec<_>>();
            assert_eq!(acknowledgements.as_array().unwrap(), &[1, 2, 3]);
        });
        let listener_thread = thread::spawn(move || {
            let messages = listener.drain().collect::<Vec<_>>();
            assert_eq!(
                messages.as_array().unwrap(),
                &[(3, Ok(())), (2, Ok(())), (1, Ok(()))]
            );
        });
        listener_thread.join().unwrap();
        speaker_thread.join().unwrap();
    }
}
