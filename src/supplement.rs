//! input enrichment and output handling at the sender and receiver level
//!
//! This module is centered around the [`supplement::channel()`] function, which
//! creates a channel similar to that created by [`mpsc::channel()`] with two main differences:
//! 1. the [`Sender`] is replaced with [`Producer`], which can enrich data with extra
//!    information before sending it over the channel in its [`send()`] method
//! 2. the [`Receiver`] is replaced with [`Consumer`], which, instead of returning received
//!    values from its [`recv()`] and [`try_recv()`] methods, pipes them into handling logic
//!    directly (although that handling logic could still return something; see
//!    [`EventAccepter::Output`])
//!
//! # Examples
//!
//! ## Using common collections
//!
//! Common collections can be passed into [`supplement::channel()`] to
//! accumulate them automatically.
//!
//! ```
//! use channellib::supplement;
//! use std::thread;
//! let (producer, consumer) = supplement::channel(Vec::new());
//! let consumer_thread = thread::spawn(move || consumer.blocking_close());
//! let producer_thread = thread::spawn(move || {
//!     producer.send(1).unwrap();
//!     producer.send(2).unwrap();
//!     producer.send(3).unwrap();
//! });
//! producer_thread.join().unwrap();
//! let results = consumer_thread.join().unwrap();
//! assert_eq!(results.as_array().unwrap(), &[1, 2, 3]);
//! ```
//!
//! See [`Consumer`] for info about the [`blocking_close()`]
//! method, its consequences, and its alternatives.
//!
//! ## Using the [`PriorityQueue`]
//!
//! This module exports the [`PriorityQueue`], which is a struct that allows for items to
//! prioritized as they are consumed from the channel. A common pattern is to pull in all
//! messages from the message buffer and then process one in a loop.
//!
//! ```
//! use channellib::supplement::{self, PriorityQueue};
//! use std::thread;
//! // most important are farthest from 10
//! let priority_function = |x: &isize| (x - 5).abs();
//! let (mut index, mut total) = (0, 0);
//! let (producer, mut consumer) = supplement::channel(PriorityQueue::new(priority_function));
//! thread::scope(|scope| {
//!     let (index_ref, total_ref) = (&mut index, &mut total);
//!     let producer_thread = scope.spawn(move || {
//!         producer.send(7).unwrap();
//!         producer.send(2).unwrap();
//!         producer.send(6).unwrap();
//!     });
//!     // usually producer_thread would send in a loop, but
//!     // finishing sending here makes the test a bit cleaner
//!     producer_thread.join().unwrap();
//!     let consumer_thread = scope.spawn(move || {
//!         loop {
//!             // recv_buffer_and_process() is only available when using PriorityQueue
//!             if let (None, false) = consumer.recv_buffer_and_process(|element| {
//!                 *total_ref += element * 10isize.pow((*index_ref) as u32);
//!                 *index_ref += 1usize;
//!             }) {
//!                 break;
//!             }
//!         }
//!     });
//!     consumer_thread.join().unwrap();
//! });
//! assert_eq!(index, 3);
//! assert_eq!(total, 672);
//! ```
//!
//! ## Using types from this module
//!
//! In addition to [`PriorityQueue`], some other convenient types for consuming
//! are provided in this module, such as [`HashEventSorter`] (which puts events
//! into buckets via a [`HashMap`]) and [`BTreeEventSorter`] (which does the same
//! via a [`BTreeMap`]). For example, here's an example of a [`supplement::channel()`]
//! that uses [`HashEventSorter`].
//!
//! ```
//! use channellib::supplement::{self, HashEventSorter};
//! let (producer, consumer) = supplement::channel(HashEventSorter::new(|data: &i8| data % 2));
//! producer.send(0).unwrap();
//! producer.send(1).unwrap();
//! producer.send(2).unwrap();
//! drop(producer);
//! let sorter = consumer.blocking_close();
//! assert_eq!(sorter.num_streams(), 2);
//! assert_eq!(sorter.num_events(), 3);
//! assert_eq!(sorter.get(&0).unwrap().as_array().unwrap(), &[0, 2]);
//! assert_eq!(sorter.get(&1).unwrap().as_array().unwrap(), &[1]);
//! ```
//!
//! ## More examples
//!
//! Many more examples can be found in the [`supplement::channel()`] documentation.
//!
//! [`supplement::channel()`]: channel()
//! [`mpsc::channel()`]: std::sync::mpsc::channel()
//! [`Sender`]: std::sync::mpsc::Sender
//! [`Producer`]: Producer
//! [`send()`]: Producer::send()
//! [`Receiver`]: std::sync::mpsc::Receiver
//! [`Consumer`]: Consumer
//! [`recv()`]: Consumer::recv()
//! [`try_recv()`]: Consumer::try_recv()
//! [`blocking_close()`]: Consumer::blocking_close()
//! [`EventAccepter::Output`]: EventAccepter::Output
//! [`HashEventSorter`]: HashEventSorter
//! [`HashMap`]: std::collections::HashMap
//! [`BTreeEventSorter`]: BTreeEventSorter
//! [`BTreeMap`]: std::collections::BTreeMap
//! [`PriorityQueue`]: PriorityQueue
//! [`BinaryHeap`]: std::collections::BinaryHeap
pub use std::sync::mpsc::{RecvError, SendError, TryRecvError};
use std::{
    collections::{
        BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, LinkedList, VecDeque,
        binary_heap,
    },
    hash::{BuildHasher, Hash, RandomState},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::mpsc::{self, Receiver, Sender},
};

/// Modification of data sent by caller before being sent over a [`supplement::channel()`]
///
/// Sometimes there are cases where the data that is natural
/// to send by a sending thread is better when additional context
/// is given. This trait specifies an instance that can do just that.
///
/// [`supplement::channel()`]: channel()
pub trait SendSupplementer {
    /// The type that is sent by the caller to [`Producer::send()`]
    ///
    /// [`Producer::send()`]: Producer::send()
    type From;

    /// The type that is actually sent through the underlying
    /// [`mpsc::channel()`] when [`Producer::send()`] is called.
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    /// [`Producer::send()`]: Producer::send()
    type To;

    /// The function by which data sent by the caller to
    /// [`Producer::send()`] is transformed into data sent
    /// over the underlying [`mpsc::channel()`].
    ///
    /// [`Producer::send()`]: Producer::send()
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    fn enrich(&self, data: Self::From) -> Self::To;

    /// A function by which the enriched data sent over the underlying
    /// [`mpsc::channel()`] can be re-formed into the original data sent
    /// by the caller to [`Producer::send()`].
    ///
    /// Unless called directly outside of this crate, this function
    /// is guaranteed to only run on values that were output by [`enrich()`]
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    /// [`Producer::send()`]: Producer::send()
    /// [`enrich()`]: SendSupplementer::enrich()
    fn unenrich(&self, data: Self::To) -> Self::From;

    fn into_supplementer(self) -> (Self, NullEventAccepter<Self::To>)
    where
        Self: Sized,
    {
        (self, NullEventAccepter::new())
    }
}

/// [`SendSupplementer`]-type that doesn't change its arguments
///
/// ```
/// use channellib::supplement::{self, SendSupplementer};
/// let send_supplementer = supplement::NullSendSupplementer::new();
/// assert_eq!(send_supplementer.enrich(1), 1);
/// assert_eq!(send_supplementer.unenrich(send_supplementer.enrich(2)), 2);
/// ```
///
/// [`SendSupplementer`]: SendSupplementer
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct NullSendSupplementer<T>(PhantomData<T>);

impl<T> NullSendSupplementer<T> {
    /// Creates a new [`NullSendSupplementer<T>`].
    ///
    /// [`NullSendSupplementer<T>`]: NullSendSupplementer
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> SendSupplementer for NullSendSupplementer<T> {
    type From = T;
    type To = T;
    fn enrich(&self, data: Self::From) -> Self::To {
        data
    }
    fn unenrich(&self, data: Self::To) -> Self::From {
        data
    }
}

/// [`SendSupplementer`]-type that runs a function on a reference
/// to the input type to determine what it should be bundled with
/// when sent across the [`supplement::channel()`].
///
/// ```
/// use channellib::supplement::{self, SendSupplementer};
/// let send_supplementer = supplement::PrependSendSupplementer::new(|x| x + 1);
/// assert_eq!(send_supplementer.enrich(1), (2, 1));
/// assert_eq!(send_supplementer.unenrich(send_supplementer.enrich(2)), 2);
/// ```
///
/// [`SendSupplementer`]: SendSupplementer
/// [`supplement::channel()`]: channel()
#[derive(Clone, Copy, Debug)]
pub struct PrependSendSupplementer<F, T, U>(F, PhantomData<T>, PhantomData<U>);

impl<F, T, U> PrependSendSupplementer<F, T, U>
where
    F: Fn(&T) -> U,
{
    pub const fn new(function: F) -> Self {
        Self(function, PhantomData, PhantomData)
    }
}

impl<F, T, U> SendSupplementer for PrependSendSupplementer<F, T, U>
where
    F: Fn(&T) -> U,
{
    type From = T;
    type To = (U, T);
    fn enrich(&self, data: Self::From) -> Self::To {
        ((self.0)(&data), data)
    }
    fn unenrich(&self, data: Self::To) -> Self::From {
        data.1
    }
}

/// A handler for the receiving side of a [`supplement::channel()`]
///
/// The receiving side of a [`supplement::channel()`] is a [`Consumer`].
/// This trait specifies a type that can customize how this is done.
/// In particular, it determines what happens to events when they
/// are received via [`Consumer::recv()`] or [`Consumer::try_recv()`].
///
/// Because of the fact that all [`EventAccepter`] types implement
/// the [`IntoSupplementer`] trait, they can be passed directly
/// to [`supplement::channel()`]. In this case, there is no send
/// supplementation, i.e. data sent by the caller is not changed
/// or enriched before being sent by a [`Producer`] to a [`Consumer`].
///
/// A few standard library types automatically implement
/// [`EventAccepter<Data = T>`], such as [`Vec<T>`], [`VecDeque<T>`],
/// [`HashSet<T>`], and [`BTreeSet<T>`].
///
/// [`supplement::channel()`]: channel()
/// [`Producer`]: Producer
/// [`Consumer`]: Consumer
/// [`Consumer::recv()`]: Consumer::recv()
/// [`Consumer::try_recv()`]: Consumer::try_recv()
/// [`EventAccepter`]: EventAccepter
/// [`IntoSupplementer`]: IntoSupplementer
/// [`Vec<T>`]: std::vec::Vec
/// [`VecDeque<T>`]: std::collections::VecDeque
/// [`HashSet<T>`]: std::collections::HashSet
/// [`BTreeSet<T>`]: std::collections::BTreeSet
pub trait EventAccepter {
    /// The type of data that this [`EventAccepter`] can accept
    /// over the [`supplement::channel()`]
    ///
    /// [`EventAccepter`]: EventAccepter
    /// [`supplement::channel()`]: channel()
    type Data;

    /// The type of data that is output when an event is handled. The
    /// generic lifetime parameter in this type is tied to the lifetime
    /// of the mutable reference to self in the [`accept()`] method.
    ///
    /// [`accept()`]: EventAccepter::accept()
    type Output<'a>
    where
        Self: 'a;

    /// Defines what the [`EventAccepter`] does with data sent over the
    /// [`supplement::channel()`]. The general assumption is that
    /// [`EventAccepter`] will store some state that will be modified as
    /// events are accepted. Then, the state can be queried between calls
    /// to [`Consumer::recv()`]/[`Consumer::try_recv()`].
    ///
    /// [`EventAccepter`]: EventAccepter
    /// [`supplement::channel()`]: channel()
    /// [`Consumer::recv()`]: Consumer::recv()
    /// [`Consumer::try_recv()`]: Consumer::try_recv()
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_>;
}

impl<T> EventAccepter for Vec<T> {
    type Data = T;
    type Output<'a>
        = &'a mut T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.push_mut(data)
    }
}

impl<T> EventAccepter for VecDeque<T> {
    type Data = T;
    type Output<'a>
        = &'a mut T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.push_back_mut(data)
    }
}

impl<T, H: BuildHasher> EventAccepter for HashSet<T, H>
where
    T: Eq + Hash,
{
    type Data = T;
    type Output<'a>
        = bool
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.insert(data)
    }
}

impl<T> EventAccepter for BTreeSet<T>
where
    T: Ord,
{
    type Data = T;
    type Output<'a>
        = bool
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.insert(data)
    }
}

impl<T> EventAccepter for LinkedList<T> {
    type Data = T;
    type Output<'a>
        = &'a mut T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.push_back_mut(data)
    }
}

/// An implementation of the [`EventAccepter`] trait that
/// does nothing but return the data unchanged.
///
/// [`EventAccepter`]: EventAccepter
pub struct NullEventAccepter<T>(PhantomData<T>);

impl<T> NullEventAccepter<T> {
    /// Creates a new [`NullEventAccepter`] for the given type.
    ///
    /// [`NullEventAccepter`]: NullEventAccepter
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for NullEventAccepter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventAccepter for NullEventAccepter<T> {
    type Data = T;
    type Output<'a>
        = T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        data
    }
}

/// The sending half of a [`supplement::channel()`]
///
/// This type utilizes an instance of a [`SendSupplementer`]-implementing
/// type to enrich data before sending it over the channel.
///
/// Note that [`Producer`] implements [`Clone`] if its [`SendSupplementer`]
/// does so that the [`Producer`] and [`Consumer`] of a [`supplement::channel()`]
/// can have a multi-producer, single-consumer relationship just like
/// [`Sender`] and [`Receiver`] with [`mpsc::channel()`].
///
/// This type can only be instantiated via the [`supplement::channel()`] method.
///
/// See [`supplement::channel()`] for detailed examples.
///
/// [`supplement::channel()`]: channel()
/// [`Clone`]: std::clone::Clone
/// [`Producer`]: Producer
/// [`Consumer`]: Consumer
/// [`supplement::channel()`]: channel()
/// [`Sender`]: std::sync::mpsc::Sender
/// [`Receiver`]: std::sync::mpsc::Receiver
/// [`mpsc::channel()`]: std::sync::mpsc::channel()
#[must_use]
#[derive(Clone)]
pub struct Producer<S: SendSupplementer> {
    /// the actual [`Sender`] used to send messages
    ///
    /// [`Sender`]: std::sync::mpsc::Sender
    sender: Sender<S::To>,
    /// supplementer of the data from the caller before
    /// being sent over the underlying [`mpsc::channel()`]
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    send_supplementer: S,
}

impl<S: SendSupplementer> Producer<S> {
    /// Supplements and sends the given data from the caller over the channel
    ///
    /// # Errors
    ///
    /// This method will return a [`SendError`] that hands back ownership of
    /// the sent data if the receiver has been dropped.
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, consumer) = supplement::channel(Vec::new());
    /// drop(consumer);
    /// assert_eq!(producer.send(1), Err(supplement::SendError(1)));
    /// ```
    ///
    /// [`SendError`]: SendError
    pub fn send(&self, data: S::From) -> Result<(), SendError<S::From>> {
        self.sender
            .send(self.send_supplementer.enrich(data))
            .map_err(|SendError(data)| {
                SendError(self.send_supplementer.unenrich(data))
            })
    }
}

/// The receiving half of a [`supplement::channel()`]
///
/// This type utilizes an instance of an [`EventAccepter`]-implementing
/// type to handle events that come over the channel.
///
/// Since [`Consumer<A>`] implements [`Deref<Target = A>`] and [`DerefMut`],
/// between receiving messages, you can access (via shared or mutable reference)
/// methods of the underlying [`EventAccepter`] class from the [`Consumer`].
/// To get ownership of the underlying [`EventAccepter`], you must close the
/// channel via one of the three possible methods, listed here in decreasing
/// order or message safety:
/// 1. [`blocking_close()`] - blocks until all messages have been sent and accepted
/// 2. [`close()`] - handles all pending messages in the queue and closes once its empty
/// 3. [`inner()`] - immediately closes, without handling any more messages
///
/// This type can only be instantiated via the [`supplement::channel()`] method.
///
/// See [`supplement::channel()`] for detailed examples.
///
/// [`supplement::channel()`]: channel()
/// [`EventAccepter`]: EventAccepter
/// [`Consumer`]: Consumer
/// [`Consumer<A>`]: Consumer
/// [`Deref<Target = A>`]: std::ops::Deref
/// [`DerefMut`]: std::ops::DerefMut
/// [`blocking_close()`]: Consumer::blocking_close()
/// [`close()`]: Consumer::close()
/// [`inner()`]: Consumer::inner()
#[must_use]
pub struct Consumer<A: EventAccepter> {
    /// the actual [`Receiver`] used to receive messages
    ///
    /// [`Receiver`]: std::sync::mpsc::Receiver
    receiver: Receiver<A::Data>,
    /// entity that handles events as they come in over the underlying
    /// [`mpsc::channel()`], usually adding it to its mutable state
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    event_accepter: A,
}

impl<A: EventAccepter> Consumer<A> {
    /// Blocks until a message can be read from the queue. If a
    /// message is read, it is automatically passed to the internal
    /// [`EventAccepter`] object's [`accept()`] method and the return
    /// value is propagated from there.
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, mut consumer) = supplement::channel(Vec::new());
    /// producer.send(1).unwrap();
    /// drop(producer);
    /// // even though producer was dropped, consumer.recv() can read from the queue
    /// assert_eq!(consumer.recv(), Ok(&mut 1));
    /// // since producer has been dropped and queue is empty, consumer.recv() returns error
    /// assert_eq!(consumer.recv(), Err(supplement::RecvError));
    /// ```
    ///
    /// # Errors
    ///
    /// If all [`Producer`] objects linked to this [`Consumer`]
    /// are dropped and there are no messages in the queue, this
    /// method will return a [`RecvError`]
    ///
    /// [`RecvError`]: std::sync::mpsc::RecvError
    /// [`EventAccepter`]: EventAccepter
    /// [`accept()`]: EventAccepter::accept()
    /// [`Producer`]: Producer
    /// [`Consumer`]: Consumer
    pub fn recv(&mut self) -> Result<A::Output<'_>, RecvError> {
        Ok(self.event_accepter.accept(self.receiver.recv()?))
    }

    /// Attempts to read a message can be read from the queue. If a
    /// message is read, it is automatically passed to the internal
    /// [`EventAccepter`] object's [`accept()`] method and the return
    /// value is propagated from there.
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, mut consumer) = supplement::channel(Vec::new());
    /// // no messages have been sent yet, so returns TryRecvError::Empty
    /// assert_eq!(consumer.try_recv(), Err(supplement::TryRecvError::Empty));
    /// producer.send(1).unwrap();
    /// drop(producer);
    /// // successfully handles message from the queue, even though producer is dropped
    /// assert_eq!(consumer.try_recv(), Ok(&mut 1));
    /// // since queue is empty and producer is dropped, return a TryRecvError::Disconnected
    /// assert_eq!(consumer.try_recv(), Err(supplement::TryRecvError::Disconnected));
    /// ```
    ///
    /// # Errors
    ///
    /// If all [`Producer`] objects linked to this [`Consumer`]
    /// are dropped and there are no messages in the queue, this
    /// method will return a [`TryRecvError::Disconnected`].
    ///
    /// If there is still at least one [`Producer`], but there
    /// are no messages in the queue, this method will return
    /// a [`TryRecvError::Empty`].
    ///
    /// [`TryRecvError::Disconnected`]: std::sync::mpsc::TryRecvError::Disconnected
    /// [`TryRecvError::Empty`]: std::sync::mpsc::TryRecvError::Empty
    /// [`EventAccepter`]: EventAccepter
    /// [`accept()`]: EventAccepter::accept()
    /// [`Producer`]: Producer
    /// [`Consumer`]: Consumer
    pub fn try_recv(&mut self) -> Result<A::Output<'_>, TryRecvError> {
        Ok(self.event_accepter.accept(self.receiver.try_recv()?))
    }

    /// Clears the full message queue by repeatedly calling [`try_recv()`] until
    /// it returns an error.
    ///
    /// This method returns whether the channel is still connected (i.e.
    /// `true` if there is still at least one [`Producer`] connected and
    /// `false` if all connected [`Producer`]s have been dropped).
    ///
    /// ```
    /// use channellib::supplement::{self, PriorityQueue, PriorityQueueItem};
    /// use std::thread;
    /// let (producer, mut consumer) =
    ///     supplement::channel(PriorityQueue::new(|x: &i32| (x - 10).abs()));
    /// let producer_thread = thread::spawn(move || {
    ///     producer.send(0).unwrap();
    ///     producer.send(10).unwrap();
    ///     producer.send(5).unwrap();
    /// });
    /// let consumer_thread = thread::spawn(move || {
    ///     loop {
    ///         let still_open = consumer.recv_buffer();
    ///         match (consumer.pop(), still_open) {
    ///             (Some(PriorityQueueItem{data, ..}), _) => println!("Processed {data}."),
    ///             (None, true) => continue,
    ///             (None, false) => break,
    ///         }
    ///     }
    /// });
    /// producer_thread.join().unwrap();
    /// consumer_thread.join().unwrap();
    /// ```
    ///
    /// The example above doesn't produce in the way of testing, so here is a
    /// longer, more spelled-out version.
    ///
    /// ```
    /// use channellib::supplement::{self, PriorityQueue, PriorityQueueItem};
    /// use std::{sync::mpsc, thread, time::Duration};
    /// let (producer, mut consumer) =
    ///     supplement::channel(PriorityQueue::new(|x: &i32| (x - 10).abs()));
    /// let (sync_sender, sync_receiver) = mpsc::channel();
    /// let producer_thread = thread::spawn(move || {
    ///     producer.send(0).unwrap();
    ///     producer.send(10).unwrap();
    ///     sync_receiver.recv().unwrap(); // just syncs up loop iterations
    ///     producer.send(5).unwrap();
    ///     drop(producer);
    ///     sync_receiver.recv().unwrap(); // consumer loop will finish 3 times
    ///     sync_receiver.recv().unwrap(); // consumer loop will finish 3 times
    /// });
    /// let consumer_thread = thread::spawn(move || {
    ///     let mut processed = Vec::new();
    ///     let mut recv_buffer_results = Vec::new();
    ///     thread::sleep(Duration::from_micros(50)); // ensure first two sends have happened
    ///     loop {
    ///         let still_open = consumer.recv_buffer();
    ///         match (consumer.pop(), still_open) {
    ///             (Some(PriorityQueueItem{data, ..}), _) => {
    ///                 recv_buffer_results.push(still_open);
    ///                 processed.push(data);
    ///             }
    ///             (None, true) => continue,
    ///             (None, false) => break (recv_buffer_results, processed),
    ///         }
    ///         sync_sender.send(()).unwrap();
    ///         thread::sleep(Duration::from_micros(50)); // ensure send() above is received
    ///     }
    /// });
    /// producer_thread.join().unwrap();
    /// let (recv_buffer_results, processed) = consumer_thread.join().unwrap();
    /// assert_eq!(recv_buffer_results.as_array().unwrap(), &[true, false, false]);
    /// assert_eq!(processed.as_array().unwrap(), &[0, 5, 10]);
    /// ```
    ///
    /// [`try_recv()`]: Consumer::try_recv()
    /// [`Producer`]: Producer
    pub fn recv_buffer(&mut self) -> bool {
        loop {
            match self.try_recv() {
                Ok(_) => {}
                Err(TryRecvError::Empty) => break true,
                Err(TryRecvError::Disconnected) => break false,
            }
        }
    }

    /// Reads messages from the queue, automatically passing them to the
    /// internal [`EventAccepter`] object's [`accept()`] method, until all
    /// of the [`Producer`] instances connected to this [`Consumer`]
    /// have been dropped. Afterward, owmnership of the underlying
    /// [`EventAccepter`] is returned to the caller.
    ///
    /// Note that since this method performs blocking reads, it will live at
    /// least as long as the longest-living [`Producer`] tied to this [`Consumer`].
    /// For a non-blocking alternative that merely reads from the queue until the
    /// queue is empty _or_ the last [`Producer`] is dropped, see [`close()`].
    ///
    /// Using this method ensures that [`Producer`] objects connected to this
    /// consumer will never receiver [`SendError`] when calling [`send()`].
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, consumer) = supplement::channel(Vec::new());
    /// producer.send(1).unwrap();
    /// producer.send(2).unwrap();
    /// drop(producer); // if we don't drop producer, blocking_close() would deadlock
    /// let result = consumer.blocking_close(); // consumes consumer to give vec of results
    /// assert_eq!(result.as_array().unwrap(), &[1, 2]);
    /// ```
    ///
    /// This method does not pass on any return value from the [`accept()`] method.
    ///
    /// [`EventAccepter`]: EventAccepter
    /// [`accept()`]: EventAccepter::accept()
    /// [`Producer`]: Producer
    /// [`Consumer`]: Consumer
    /// [`close()`]: Consumer::close()
    /// [`send()`]: Producer::send()
    pub fn blocking_close(mut self) -> A {
        while self.recv().is_ok() {}
        self.event_accepter
    }

    /// Reads messages from the queue until it is empty, automatically passing
    /// them to the internal [`EventAccepter`] object's [`accept()`] method.
    /// Afterward, ownership of the underlying [`EventAccepter`] is returned
    /// to the caller.
    ///
    /// This is a non-blocking form of [`blocking_close()`]. It will only block
    /// until the message queue is empty, even if there are [`Producer`] objects
    /// still tied to this [`Consumer`] (if any of them call [`send()`] after this
    /// function returns, they will get a [`SendError`] in response).
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, consumer) = supplement::channel(Vec::new());
    /// producer.send(1).unwrap();
    /// producer.send(2).unwrap();
    /// // unlike blocking_close(), no need to drop producer for close() to not deadlock
    /// let result = consumer.close(); // consumes consumer to give vec of results
    /// assert_eq!(result.as_array().unwrap(), &[1, 2]);
    /// assert_eq!(producer.send(3), Err(supplement::SendError(3)));
    /// ```
    ///
    /// This method does not pass on any return value from the [`accept()`] method.
    ///
    /// [`Producer`]: Producer
    /// [`Consumer`]: Consumer
    /// [`EventAccepter`]: EventAccepter
    /// [`accept()`]: EventAccepter::accept()
    /// [`blocking_close()`]: Consumer::blocking_close()
    /// [`send()`]: Producer::send()
    /// [`SendError`]: std::sync::mpsc::SendError
    pub fn close(mut self) -> A {
        while self.try_recv().is_ok() {}
        self.event_accepter
    }

    /// Immediately closes the receiver to the underlying channel,
    /// even if there are unhandled messages in the queue, and returns
    /// ownership of the underlying [`EventAccepter`] to the caller.
    ///
    /// This is the most dangerous (in terms of missing messages), but most
    /// immediate way of closing a [`supplement::channel()`]. See the
    /// [`close()`] and [`blocking_close()`] methods for safer alternatives.
    ///
    /// ```
    /// use channellib::supplement;
    /// let (producer, consumer) = supplement::channel(Vec::new());
    /// producer.send(1).unwrap(); // this message is sent but never delivered!
    /// assert!(consumer.inner().is_empty());
    /// assert_eq!(producer.send(2), Err(supplement::SendError(2)));
    /// ```
    ///
    /// [`supplement::channel()`]: channel()
    /// [`close()`]: Consumer::close()
    /// [`blocking_close()`]: Consumer::blocking_close()
    pub fn inner(self) -> A {
        self.event_accepter
    }
}

impl<A: EventAccepter> Deref for Consumer<A> {
    type Target = A;
    fn deref(&self) -> &Self::Target {
        &self.event_accepter
    }
}

impl<A: EventAccepter> DerefMut for Consumer<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.event_accepter
    }
}

/// The type of the parameter to [`supplement::channel()`]
///
/// In particular, anything that can be resolved into a combination
/// of a [`SendSupplementer`] and a [`EventAccepter`] should
/// implement [`IntoSupplementer`]. Below are a few types that
/// automatically satisfy [`IntoSupplementer`]:
/// - Any [`EventAccepter`] type: this will implicitly combine it with
///   a [`NullSendSupplementer`], which sends data through unchanged
/// - `(S, A)` where `S` is a [`SendSupplementer`] and `A` is an
///   [`EventAccepter`]. The two types have the extra cross-requirement
///   that `A::Data` must be the same as `S::To`, because they will be
///   communicating over the channel with that type.
///
/// [`supplement::channel()`]: channel()
/// [`SendSupplementer`]: SendSupplementer
/// [`EventAccepter`]: EventAccepter
/// [`IntoSupplementer`]: IntoSupplementer
pub trait IntoSupplementer {
    /// the data actually being sent over the underlying [`mpsc::channel()`]
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    type Data;
    /// the [`SendSupplementer`] type that will be returned by [`into_supplementer()`]
    ///
    /// [`SendSupplementer`]: SendSupplementer
    /// [`into_supplementer()`]: IntoSupplementer::into_supplementer()
    type SendSupplementer: SendSupplementer<To = Self::Data>;
    /// the [`EventAccepter`] type that will be returned by [`into_supplementer()`]
    ///
    /// [`EventAccepter`]: EventAccepter
    /// [`into_supplementer()`]: IntoSupplementer::into_supplementer()
    type EventAcccepter: EventAccepter<Data = Self::Data>;
    /// Consumers self and outputs a tuple of:
    /// 1. the [`SendSupplementer`] that will modify incoming data on
    ///    sending side of the [`supplement::channel()`]
    /// 2. the [`EventAccepter`] that will process data on receiving
    ///    side of the [`supplement::channel()`]
    ///
    /// [`SendSupplementer`]: SendSupplementer
    /// [`EventAccepter`]: EventAccepter
    /// [`supplement::channel()`]: channel()
    fn into_supplementer(
        self,
    ) -> (Self::SendSupplementer, Self::EventAcccepter);
}

impl<A: EventAccepter> IntoSupplementer for A {
    type Data = A::Data;
    type EventAcccepter = A;
    type SendSupplementer = NullSendSupplementer<A::Data>;
    fn into_supplementer(
        self,
    ) -> (Self::SendSupplementer, Self::EventAcccepter) {
        (NullSendSupplementer::new(), self)
    }
}

impl<S, A, T> IntoSupplementer for (S, A)
where
    S: SendSupplementer<To = T>,
    A: EventAccepter<Data = T>,
{
    type Data = T;
    type EventAcccepter = A;
    type SendSupplementer = S;
    fn into_supplementer(
        self,
    ) -> (Self::SendSupplementer, Self::EventAcccepter) {
        self
    }
}

/// Creates a channel that is supplemented with input modification and output handling.
///
/// # Calling patterns
///
/// In general, this function can be called with an argument of any type that implements
/// [`IntoSupplementer`]. In general, that is usually achieved through one of a few patterns.
///
/// ## With a collection type
///
/// The collection types [`Vec`], [`VecDeque`], [`LinkedList`], [`HashSet`], and [`BTreeSet`]
/// all implement the [`IntoSupplementer`] trait (as long as the trait requirements
/// are met, e.g. data must be [`Eq`] and [`Hash`] to use [`HashSet`] and [`Ord`]
/// to use [`BTreeSet`]). This means passing one directly to this function will create
/// a channel that will auto-fill these collections. For example with a [`Vec`]
///
/// ```
/// use channellib::supplement;
/// let (producer, mut consumer) = supplement::channel(Vec::with_capacity(3));
/// assert!(consumer.is_empty()); // Consumer implements Deref<Target=Vec<i32>>
/// producer.send(1).unwrap();
/// consumer.recv().unwrap();
/// assert_eq!(consumer.as_array().unwrap(), &[1]);
/// producer.send(2).unwrap();
/// consumer.recv().unwrap();
/// let inner = consumer.inner(); // consumes the consumer to release ownership of Vec
/// assert_eq!(inner.as_array().unwrap(), &[1, 2]);
/// ```
///
/// ## With a custom [`EventAccepter`]
///
/// Any type that implements the [`EventAccepter`] trait can be used the same way as the
/// collections above (as in that case, there is no input modification and data is passed
/// through unchanged from caller to [`send()`] to the [`EventAccepter`]). For example, if
/// you had some processing you want to do with each event and don't want to accumulate
/// events on the heap, you could make an [`EventAccepter`] to handle those events
///
/// ```
/// use channellib::supplement;
/// use std::sync::{Arc, Mutex};
/// static output: Mutex<String> = Mutex::new(String::new());
/// struct MyEventAccepter;
/// impl supplement::EventAccepter for MyEventAccepter {
///     type Data = i32;
///     type Output<'a> = () where Self: 'a;
///     fn accept(&mut self, data: Self::Data) {
///         // we could just println!("{data}"), but then we wouldn't
///         // be able to test the output in this doctest.
///         output.lock().unwrap().push_str(&format!("{data}\n"));
///     }
/// }
/// let (producer, consumer) = supplement::channel(MyEventAccepter);
/// producer.send(1).unwrap();
/// producer.send(2).unwrap();
/// producer.send(3).unwrap();
/// drop(producer);
/// consumer.blocking_close();
/// assert_eq!(output.lock().unwrap().as_str(), "1\n2\n3\n");
/// ```
///
/// ## With only a [`SendSupplementer`]
///
/// You can utilize only the send supplementation aspects of this module by
/// calling [`into_supplementer()`] on a [`SendSupplementer`]. This will
/// turn it into an [`IntoSupplementer`]-implementing type that can be passed
/// to this function.
///
/// ```
/// use channellib::supplement::{self, SendSupplementer};
/// struct MySendSupplementer;
/// impl SendSupplementer for MySendSupplementer {
///     type From = i32;
///     type To = i32;
///     fn enrich(&self, data: Self::From) -> Self::To {
///         2 * data
///     }
///     fn unenrich(&self, data: Self::To) -> Self::From {
///         data / 2
///     }
/// }
/// let (producer, mut consumer) = supplement::channel(MySendSupplementer.into_supplementer());
/// producer.send(1).unwrap();
/// producer.send(2).unwrap();
/// producer.send(3).unwrap();
/// assert_eq!(consumer.recv().unwrap(), 2);
/// assert_eq!(consumer.recv().unwrap(), 4);
/// assert_eq!(consumer.recv().unwrap(), 6);
/// assert_eq!(consumer.try_recv(), Err(supplement::TryRecvError::Empty));
/// drop(consumer);
/// assert_eq!(producer.send(4), Err(supplement::SendError(4)));
/// ```
///
/// ## With a custom [`IntoSupplementer`]
///
/// Note that every tuple of the form `(S, A)` where `S` is a [`SendSupplementer`]
/// and `A` is a [`EventAccepter`] and `S::To` is the same as `A::Data` also implements
/// [`IntoSupplementer`]! So, you can define your own supplementers and accepters!
///
/// # Multi-producer, single-consumer
///
/// As with a basic [`mpsc::channel()`] instance (which is used under the hood here),
/// this function creates a multi-producer, single-consumer channel, i.e. the [`Producer`]
/// type is [`Clone`] (assuming the [`SendSupplementer`] is [`Clone`]) but the [`Consumer`]
/// type is not and the [`Consumer`] will only consider the channel disconnected when
/// _all_ [`Producer`] instances have been dropped.
///
/// ```
/// use channellib::supplement;
/// let (producer, mut consumer) = supplement::channel(Vec::new());
/// let producer_clone = producer.clone().send(1).unwrap();
/// drop(producer_clone);
/// assert!(consumer.is_empty()); // Consumer implements Deref<Target=A> where A is the EventAccepter
/// consumer.recv().unwrap();
/// assert_eq!(consumer.as_array().unwrap(), &[1]);
/// assert_eq!(consumer.try_recv(), Err(supplement::TryRecvError::Empty));
/// producer.send(2).unwrap();
/// consumer.try_recv().unwrap();
/// assert_eq!(consumer.as_array().unwrap(), &[1, 2]);
/// drop(producer);
/// assert_eq!(consumer.try_recv(), Err(supplement::TryRecvError::Disconnected));
/// assert_eq!(consumer.recv(), Err(supplement::RecvError));
/// ```
///
/// [`IntoSupplementer`]: IntoSupplementer
/// [`into_supplementer()`]: SendSupplementer::into_supplementer()
/// [`mpsc::channel()`]: std::sync::mpsc::channel()
/// [`SendSupplementer`]: SendSupplementer
/// [`EventAccepter`]: EventAccepter
/// [`Clone`]: std::clone::Clone
/// [`Producer`]: Producer
/// [`Consumer`]: Consumer
/// [`Vec`]: std::vec::Vec
/// [`VecDeque`]: std::collections::VecDeque
/// [`LinkedList`]: std::collections::LinkedList
/// [`HashSet`]: std::collections::HashSet
/// [`BTreeSet`]: std::collections::BTreeSet
/// [`Eq`]: std::cmp::Eq
/// [`Hash`]: std::hash::Hash
/// [`send()`]: Producer::send()
pub fn channel<T, I, S, A>(supplementer: I) -> (Producer<S>, Consumer<A>)
where
    I: IntoSupplementer<Data = T, SendSupplementer = S, EventAcccepter = A>,
    S: SendSupplementer<To = T>,
    A: EventAccepter<Data = T>,
{
    let (sender, receiver) = mpsc::channel();
    let (send_supplementer, event_accepter) = supplementer.into_supplementer();
    let producer = Producer {
        sender,
        send_supplementer,
    };
    let consumer = Consumer {
        receiver,
        event_accepter,
    };
    (producer, consumer)
}

/// [`EventAccepter`] type that sorts events into buckets based on a computed ID.
/// Since [`HashMap`] is used under the hood, lookups, insertions and deletions are O(1).
///
/// Contrast with [`BTreeEventSorter`] for tradeoffs.
///
/// Invariant: no empty vecs are stored in the events map
///
/// [`EventAccepter`]: EventAccepter
/// [`HashMap`]: std::collections::HashMap
/// [`BTreeEventSorter`]: BTreeEventSorter
#[must_use]
pub struct HashEventSorter<T, I, F, S: BuildHasher> {
    events: HashMap<I, Vec<T>, S>,
    id_function: F,
}

impl<T, I, F> HashEventSorter<T, I, F, RandomState>
where
    I: Eq + Hash,
    F: Fn(&T) -> I,
{
    /// Creates a new [`HashEventSorter`] with the given function to computer IDs.
    ///
    /// [`HashEventSorter`]: HashEventSorter
    pub fn new(id_function: F) -> Self {
        Self {
            events: HashMap::new(),
            id_function,
        }
    }
}

impl<T, I, F, S> HashEventSorter<T, I, F, S>
where
    I: Eq + Hash,
    F: Fn(&T) -> I,
    S: BuildHasher,
{
    pub const fn with_hasher(id_function: F, hash_builder: S) -> Self {
        Self {
            events: HashMap::with_hasher(hash_builder),
            id_function,
        }
    }

    /// Claims ownership of the underlying [`HashMap`] by consuming `self`
    ///
    /// [`HashMap`]: std::collections::HashMap
    pub fn inner(self) -> HashMap<I, Vec<T>, S> {
        self.events
    }
}

impl<T, I, F, S> HashEventSorter<T, I, F, S>
where
    I: Eq + Hash,
    S: BuildHasher,
{
    /// The number of different IDs currently stored by the sorter
    pub fn num_streams(&self) -> usize {
        self.events.len()
    }

    /// The number of events currently stored by the sorter across all IDs
    pub fn num_events(&self) -> usize {
        self.events.values().map(Vec::len).sum()
    }
}

impl<T, I, F, S> From<HashEventSorter<T, I, F, S>> for HashMap<I, Vec<T>, S>
where
    S: BuildHasher,
{
    fn from(value: HashEventSorter<T, I, F, S>) -> Self {
        value.events
    }
}

impl<T, I, F, S> Deref for HashEventSorter<T, I, F, S>
where
    S: BuildHasher,
{
    type Target = HashMap<I, Vec<T>, S>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

impl<T, I, F, S> DerefMut for HashEventSorter<T, I, F, S>
where
    S: BuildHasher,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

impl<T, I, F, S> EventAccepter for HashEventSorter<T, I, F, S>
where
    F: Fn(&T) -> I,
    I: Eq + Hash,
    S: BuildHasher,
{
    type Data = T;
    type Output<'a>
        = &'a mut T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        let id = (self.id_function)(&data);
        self.events
            .entry(id)
            .or_insert_with(|| Vec::with_capacity(1))
            .push_mut(data)
    }
}

/// [`EventAccepter`] type that sorts events into buckets based on a computed ID.
/// Since it uses a [`BTreeMap`] under the hood, this keeps events sorted by ID.
///
/// Contrast with [`HashEventSorter`] for tradeoffs.
///
/// Invariant: no empty vecs are stored in the events map
///
/// [`EventAccepter`]: EventAccepter
/// [`BTreeMap`]: std::collections::BTreeMap
/// [`HashEventSorter`]: HashEventSorter
#[must_use]
pub struct BTreeEventSorter<T, I, F> {
    events: BTreeMap<I, Vec<T>>,
    id_function: F,
}

impl<T, I, F> BTreeEventSorter<T, I, F>
where
    I: Ord,
    F: Fn(&T) -> I,
{
    /// Creates a new [`BTreeEventSorter`] with the given function to computer IDs.
    ///
    /// [`BTreeEventSorter`]: BTreeEventSorter
    pub const fn new(id_function: F) -> Self {
        Self {
            events: BTreeMap::new(),
            id_function,
        }
    }
}

impl<T, I, F> BTreeEventSorter<T, I, F>
where
    I: Ord,
{
    /// The number of different IDs currently stored by the sorter
    pub fn num_streams(&self) -> usize {
        self.events.len()
    }

    /// The number of events currently stored by the sorter across all IDs
    pub fn num_events(&self) -> usize {
        self.events.values().map(Vec::len).sum()
    }
}

impl<T, I, F> From<BTreeEventSorter<T, I, F>> for BTreeMap<I, Vec<T>> {
    fn from(value: BTreeEventSorter<T, I, F>) -> Self {
        value.events
    }
}

impl<T, I, F> Deref for BTreeEventSorter<T, I, F> {
    type Target = BTreeMap<I, Vec<T>>;
    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

impl<T, I, F> DerefMut for BTreeEventSorter<T, I, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

impl<T, I, F> EventAccepter for BTreeEventSorter<T, I, F>
where
    F: Fn(&T) -> I,
    I: Ord,
{
    type Data = T;
    type Output<'a>
        = &'a mut T
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        let id = (self.id_function)(&data);
        self.events
            .entry(id)
            .or_insert_with(|| Vec::with_capacity(1))
            .push_mut(data)
    }
}

/// An item in the priority queue, allowing for ordering
/// based on something other than the underlying value.
#[derive(Clone, Copy, Debug)]
pub struct PriorityQueueItem<T, P> {
    /// the actual data being stored
    pub data: T,
    /// the computed priority (higher -> more important)
    pub priority: P,
}

impl<T, P> PartialEq for PriorityQueueItem<T, P>
where
    P: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T, P> Eq for PriorityQueueItem<T, P> where P: Eq {}

impl<T, P> PartialOrd for PriorityQueueItem<T, P>
where
    P: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.priority.partial_cmp(&other.priority)
    }
}

impl<T, P> Ord for PriorityQueueItem<T, P>
where
    P: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

/// A priority queue implemented via a [`BinaryHeap`].
///
/// The main benefit of this over the standalone [`BinaryHeap`] class is that
/// it can use a priority function to determine ordering without requiring
/// the main data being placed to implement [`Ord`]. Note that when utilizing
/// the [`EventAccepter::accept()`] trait method on a [`PriorityQueue`] the
/// returned value is a mutable reference to the highest priority
/// [`PriorityQueueItem`] in the queue, returned via a [`PeekMut`] object.
///
/// ```
/// use channellib::supplement::{self, PriorityQueue, PriorityQueueItem};
/// use std::collections::{BinaryHeap, binary_heap::PeekMut};
/// let (producer, mut consumer) = supplement::channel(PriorityQueue::new(|x: &i64| (x - 10).abs()));
/// producer.send(0).unwrap();
/// producer.send(5).unwrap();
/// drop(producer);
/// assert_eq!(consumer.recv().unwrap().data, 0);
/// assert_eq!(consumer.len(), 1);
/// let top_after_second_element = consumer.recv().unwrap();
/// assert_eq!(PeekMut::pop(top_after_second_element), PriorityQueueItem{ data: 0, priority: 10 });
/// let remaining: BinaryHeap<_> = consumer.blocking_close().into();
/// let remaining = remaining.into_iter().collect::<Vec<_>>();
/// assert_eq!(remaining.as_array().unwrap(), &[PriorityQueueItem{ data: 5, priority: 5 }]);
/// ```
///
/// [`PriorityQueue`]: PriorityQueue
/// [`PriorityQueueItem`]: PriorityQueueItem
/// [`PeekMut`]: std::collections::binary_heap::PeekMut
/// [`BinaryHeap`]: std::collections::BinaryHeap
/// [`Ord`]: std::cmp::Ord
/// [`EventAccepter::accept()`]: EventAccepter::accept()
pub struct PriorityQueue<T, P, F> {
    /// the underlying [`BinaryHeap`] containing the data
    ///
    /// [`BinaryHeap`]: std::collections::BinaryHeap
    queue: BinaryHeap<PriorityQueueItem<T, P>>,
    /// the function that will be used to determine the
    /// priority of a given item of type `T` (from a `&T`)
    priority: F,
}

impl<T, P, F> PriorityQueue<T, P, F>
where
    F: Fn(&T) -> P,
    P: Ord,
{
    /// Creates a new [`PriorityQueue`] with the given priority function,
    /// which will be used to determine where values get placed in the
    /// queue.
    ///
    /// [`PriorityQueue`]: PriorityQueue
    pub const fn new(priority: F) -> Self {
        Self {
            queue: BinaryHeap::new(),
            priority,
        }
    }

    /// Computes the priority of the data and places a new
    /// item containing it in the queue.
    pub fn prioritize(&mut self, data: T) {
        let priority = (self.priority)(&data);
        self.queue.push(PriorityQueueItem { data, priority });
    }
}

impl<T, P, F> From<PriorityQueue<T, P, F>>
    for BinaryHeap<PriorityQueueItem<T, P>>
{
    fn from(value: PriorityQueue<T, P, F>) -> Self {
        value.queue
    }
}

impl<T, P, F> Deref for PriorityQueue<T, P, F> {
    type Target = BinaryHeap<PriorityQueueItem<T, P>>;
    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

impl<T, P, F> DerefMut for PriorityQueue<T, P, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.queue
    }
}

impl<T, P, F> EventAccepter for PriorityQueue<T, P, F>
where
    F: Fn(&T) -> P,
    P: Ord,
{
    type Data = T;
    type Output<'a>
        = binary_heap::PeekMut<'a, PriorityQueueItem<T, P>>
    where
        Self: 'a;
    fn accept(&mut self, data: Self::Data) -> Self::Output<'_> {
        self.prioritize(data);
        self.queue
            .peek_mut()
            .expect("queue shouldn't be empty because we just added to it")
    }
}

impl<T, P, F> Consumer<PriorityQueue<T, P, F>>
where
    F: Fn(&T) -> P,
    P: Ord,
{
    /// Reads all messages from the underlying [`mpsc::channel()`] buffer
    /// and then processes the highest priority one. This function returns
    /// `(processed, is_open)` where `processed` is the highest priority
    /// item after having the `process` function applied to it and `is_open`
    /// is a `bool` that is `true` if this [`Consumer`] still has one or
    /// more connected [`Producer`]s. and `false` if all of the connected
    /// [`Producer`]s have been dropped.
    ///
    /// Note that `process` is not guaranteed to be called. In particular, if
    /// the priority queue is empty even after all messages from the buffer
    /// are read, then it will not be called and the first element of the
    /// returned tuple will be `None`.
    ///
    /// This is a special method that exists on the [`Consumer`] struct
    /// when its [`EventAccepter`] implementation is a [`PriorityQueue`].
    ///
    /// ```rust
    /// use channellib::supplement::{self, PriorityQueue};
    /// let priority_queue = PriorityQueue::new(|x: &usize| *x);
    /// let (producer, mut consumer) = supplement::channel(priority_queue);
    /// producer.send(1).unwrap();
    /// producer.send(2).unwrap();
    /// drop(producer);
    /// let (mut total, mut index) = (0, 0);
    /// loop {
    ///     if let (None, false) = consumer.recv_buffer_and_process(|element| {
    ///         total += element * 10usize.pow(index as u32);
    ///         index += 1;
    ///     }) {
    ///         break;
    ///     }
    /// }
    /// assert_eq!(total, 12);
    /// assert_eq!(index, 2);
    /// ```
    ///
    /// [`mpsc::channel()`]: std::sync::mpsc::channel()
    /// [`Consumer`]: Consumer
    /// [`EventAccepter`]: EventAccepter
    /// [`PriorityQueue`]: PriorityQueue
    pub fn recv_buffer_and_process<U, G>(
        &mut self,
        process: G,
    ) -> (Option<U>, bool)
    where
        G: FnOnce(T) -> U,
    {
        let is_open = self.recv_buffer();
        let processed = self
            .pop()
            .map(|PriorityQueueItem { data, .. }| process(data));
        (processed, is_open)
    }
}

#[cfg(test)]
mod tests {

    use super as supplement;
    use std::{
        collections::{BTreeMap, BTreeSet, HashSet, LinkedList, VecDeque},
        ops::Deref,
        thread,
    };

    /// Confirms that `Vec` objects can be passed to `supplement::channel()` and
    /// the `Consumer`'s `accept()` method will accumulate the sent items in a `Vec`.
    /// Also tests order of messages.
    #[test]
    fn receive_vec() {
        let (producer, consumer) = supplement::channel(Vec::with_capacity(3));
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(3).unwrap();
        drop(producer);
        let results = consumer.blocking_close();
        assert_eq!(results.as_array().unwrap(), &[1, 2, 3]);
    }

    /// Confirms that `VecDeque` objects can be passed to `supplement::channel()` and
    /// the `Consumer`'s `accept()` method will accumulate the sent items in a `VecDeque`.
    /// Also tests order of messages.
    #[test]
    fn receive_vec_deque() {
        let (producer, consumer) = supplement::channel(VecDeque::new());
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(3).unwrap();
        drop(producer);
        let results = consumer.blocking_close().into_iter().collect::<Vec<_>>();
        assert_eq!(results.as_array().unwrap(), &[1, 2, 3]);
    }

    /// Confirms that `HashSet` objects can be passed to `supplement::channel()` and
    /// the `Consumer`'s `accept()` method will accumulate the sent items in a `HashSet`
    #[test]
    fn receive_hash_set() {
        let (producer, consumer) = supplement::channel(HashSet::new());
        producer.send(1).unwrap();
        producer.send(1).unwrap();
        producer.send(1).unwrap();
        drop(producer);
        let results = consumer.blocking_close().into_iter().collect::<Vec<_>>();
        assert_eq!(results.as_array().unwrap(), &[1]);
    }

    /// Confirms that `BTreeSet` objects can be passed to `supplement::channel()` and
    /// the `Consumer`'s `accept()` method will accumulate the sent items in a `BTreeSet`
    #[test]
    fn receive_btree_set() {
        let (producer, consumer) = supplement::channel(BTreeSet::new());
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(1).unwrap();
        drop(producer);
        let results = consumer.blocking_close().into_iter().collect::<Vec<_>>();
        assert_eq!(results.as_array().unwrap(), &[1, 2]);
    }

    /// Confirms that `LinkedList` objects can be passed to `supplement::channel()` and
    /// the `Consumer`'s `accept()` method will accumulate the sent items in a `BTreeSet`.
    #[test]
    fn receive_linked_list() {
        let (producer, consumer) = supplement::channel(LinkedList::new());
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(1).unwrap();
        drop(producer);
        let results = consumer.blocking_close().into_iter().collect::<Vec<_>>();
        assert_eq!(results.as_array().unwrap(), &[1, 2, 1]);
    }

    /// Confirms that `Producer`s can be cloned (as long as the `SendSupplementer`
    /// created by `supplement::channel()` can be cloned) and used to send
    /// messages to the same `Consumer`
    #[test]
    fn multi_producer_single_consumer() {
        let (producer, consumer) = supplement::channel(Vec::with_capacity(4));
        let producer_clone = producer.clone();
        thread::scope(move |scope| {
            scope.spawn(move || {
                producer.send(1).unwrap();
                producer.send(2).unwrap();
            });
            scope.spawn(move || {
                producer_clone.send(3).unwrap();
                producer_clone.send(4).unwrap();
            });
        });
        let (mut seen_one, mut seen_three) = (false, false);
        for result in consumer.blocking_close() {
            match result {
                1 => seen_one = true,
                2 => assert!(seen_one),
                3 => seen_three = true,
                4 => assert!(seen_three),
                _ => unreachable!(),
            }
        }
    }

    /// Ensures that custom implementations of SendSupplementer can be passed into
    /// `supplement::channel()` as long as the `SendSupplementer::into_supplementer()`
    /// method is called on them (which requires the `SendSupplementer` trait is in scope)
    #[test]
    fn custom_send_supplementer() {
        use supplement::SendSupplementer; // needed to use SendSupplementer::into_supplementer()
        struct MySendSupplementer;
        impl SendSupplementer for MySendSupplementer {
            type From = u8;
            type To = String;
            fn enrich(&self, data: Self::From) -> Self::To {
                data.to_string()
            }
            fn unenrich(&self, data: Self::To) -> Self::From {
                data.parse().unwrap()
            }
        }
        let (producer, mut consumer) =
            supplement::channel(MySendSupplementer.into_supplementer());
        producer.send(0).unwrap();
        producer.send(1).unwrap();
        drop(producer);
        assert_eq!(consumer.recv().unwrap().as_str(), "0");
        assert_eq!(consumer.recv().unwrap().as_str(), "1");
        assert_eq!(consumer.recv(), Err(supplement::RecvError));
    }

    /// Ensures that custom implementations of `EventAccepter` can be passed
    /// directly into `supplement::channel()` and that they work as expected.
    #[test]
    fn custom_event_accepter() {
        struct MyEventAccepter(String);
        impl Deref for MyEventAccepter {
            type Target = String;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl supplement::EventAccepter for MyEventAccepter {
            type Data = u8;
            type Output<'a>
                = &'a str
            where
                Self: 'a;
            fn accept<'a>(&'a mut self, data: Self::Data) -> Self::Output<'a> {
                self.0.push_str(data.to_string().as_str());
                &self.0
            }
        }
        let (producer, mut consumer) =
            supplement::channel(MyEventAccepter(String::new()));
        producer.send(10).unwrap();
        producer.send(255).unwrap();
        drop(producer);
        assert_eq!(consumer.as_str(), "");
        assert_eq!(consumer.recv(), Ok("10"));
        assert_eq!(consumer.recv(), Ok("10255"));
        assert_eq!(consumer.recv(), Err(supplement::RecvError));
    }

    /// Ensures that custom implementers of the IntoSupplementer trait can be passed
    /// directly into `supplement::channel()` and the send supplementation and event
    /// acceptance work as expected.
    #[test]
    fn custom_into_supplementer() {
        use supplement::SendSupplementer;
        struct MySendSupplementer;
        impl SendSupplementer for MySendSupplementer {
            type From = u8;
            type To = (u8, u8);
            fn enrich(&self, data: Self::From) -> Self::To {
                (data, data)
            }
            fn unenrich(&self, data: Self::To) -> Self::From {
                data.0
            }
        }
        struct MyEventAccepter(u8);
        impl supplement::EventAccepter for MyEventAccepter {
            type Data = (u8, u8);
            type Output<'a>
                = u8
            where
                Self: 'a;
            fn accept<'a>(&'a mut self, data: Self::Data) -> Self::Output<'a> {
                self.0 += data.0 + data.1;
                self.0
            }
        }
        struct MyIntoSupplementer;
        impl supplement::IntoSupplementer for MyIntoSupplementer {
            type Data = (u8, u8);
            type EventAcccepter = MyEventAccepter;
            type SendSupplementer = MySendSupplementer;
            fn into_supplementer(
                self,
            ) -> (Self::SendSupplementer, Self::EventAcccepter) {
                (MySendSupplementer, MyEventAccepter(0))
            }
        }
        let (producer, mut consumer) = supplement::channel(MyIntoSupplementer);
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(3).unwrap();
        drop(producer);
        assert_eq!(consumer.recv(), Ok(2));
        assert_eq!(consumer.recv(), Ok(6));
        assert_eq!(consumer.recv(), Ok(12));
        assert_eq!(consumer.recv(), Err(supplement::RecvError));
    }

    /// Ensures that tuples of custom implementers of the SendSupplementer and
    /// EventAccepter traits can be passed directly into `supplement::channel()`
    /// and the send supplementation and event acceptance work as expected.
    #[test]
    fn custom_send_supplementer_and_custom_event_acceptance_tuple() {
        use supplement::SendSupplementer;
        struct MySendSupplementer;
        impl SendSupplementer for MySendSupplementer {
            type From = u8;
            type To = (u8, u8);
            fn enrich(&self, data: Self::From) -> Self::To {
                (data, data)
            }
            fn unenrich(&self, data: Self::To) -> Self::From {
                data.0
            }
        }
        struct MyEventAccepter(u8);
        impl supplement::EventAccepter for MyEventAccepter {
            type Data = (u8, u8);
            type Output<'a>
                = u8
            where
                Self: 'a;
            fn accept<'a>(&'a mut self, data: Self::Data) -> Self::Output<'a> {
                self.0 += data.0 + data.1;
                self.0
            }
        }
        let (producer, mut consumer) =
            supplement::channel((MySendSupplementer, MyEventAccepter(0)));
        producer.send(1).unwrap();
        producer.send(2).unwrap();
        producer.send(3).unwrap();
        drop(producer);
        assert_eq!(consumer.recv(), Ok(2));
        assert_eq!(consumer.recv(), Ok(6));
        assert_eq!(consumer.recv(), Ok(12));
        assert_eq!(consumer.recv(), Err(supplement::RecvError));
    }

    /// Tests the `HashEventSorter` by passing &str over the
    /// channel and bucketing them by first character.
    #[test]
    fn hash_event_sorter() {
        let sorter = supplement::HashEventSorter::new(|s: &&str| {
            let mut chars = s.chars();
            chars.next().unwrap()
        });
        let (producer, consumer) = supplement::channel(sorter);
        producer.send("A black cat").unwrap();
        producer.send("The white dog").unwrap();
        producer.send("At last I have found my home").unwrap();
        drop(producer);
        let sorter = consumer.blocking_close();
        assert_eq!(sorter.num_streams(), 2);
        assert_eq!(sorter.num_events(), 3);
        assert_eq!(
            sorter.get(&'A').unwrap().as_array().unwrap(),
            &["A black cat", "At last I have found my home"]
        );
        assert_eq!(
            sorter.get(&'T').unwrap().as_array().unwrap(),
            &["The white dog"]
        );
    }

    /// Tests the `HashEventSorter` by passing &str over the
    /// channel and bucketing them by first character.
    #[test]
    fn btree_event_sorter() {
        let sorter = supplement::BTreeEventSorter::new(|s: &&str| {
            let mut chars = s.chars();
            chars.next().unwrap()
        });
        let (producer, consumer) = supplement::channel(sorter);
        producer.send("A black cat").unwrap();
        producer.send("The white dog").unwrap();
        producer.send("At last I have found my home").unwrap();
        drop(producer);
        let sorter = consumer.blocking_close();
        assert_eq!(sorter.num_streams(), 2);
        assert_eq!(sorter.num_events(), 3);
        let btree: BTreeMap<char, Vec<&str>> = sorter.into();
        let mut iter = btree.into_iter();
        let (first_starting_letter, starting_with_first_letter) =
            iter.next().unwrap();
        assert_eq!(first_starting_letter, 'A');
        assert_eq!(
            starting_with_first_letter.as_array().unwrap(),
            &["A black cat", "At last I have found my home"]
        );
        let (second_starting_letter, starting_with_second_letter) =
            iter.next().unwrap();
        assert_eq!(second_starting_letter, 'T');
        assert_eq!(
            starting_with_second_letter.as_array().unwrap(),
            &["The white dog"]
        );
        assert!(iter.next().is_none());
    }

    /// Tests that the `PriorityQueue` implementation of `EventAccepter` works as expected,
    /// yielding a binary heap that pulls the highest priority data on `pop()`
    #[test]
    fn priority_queue() {
        let (producer, mut consumer) =
            supplement::channel(supplement::PriorityQueue::new(|s: &&str| {
                s.len()
            }));
        producer.send("12").unwrap();
        producer.send("345").unwrap();
        producer.send("6").unwrap();
        drop(producer);
        assert_eq!(consumer.recv_buffer(), false);
        assert_eq!(consumer.pop().unwrap().data, "345");
        assert_eq!(consumer.pop().unwrap().data, "12");
        assert_eq!(consumer.pop().unwrap().data, "6");
        assert!(consumer.pop().is_none());
    }
}
