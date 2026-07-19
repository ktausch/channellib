//! A crate with tools for making enriched channels for communication between threads.
//!
//! There are two main types of channels supported by this crate:
//! - [`two_way::channel()`] provides a channel where both sides can both send and receive messages
//!   using a [`Communicator`] object (see [`two_way`])
//! - [`acknowledge::channel()`] provides a channel where the sender (called a [`Speaker`]) can
//!   send messages to the receiver (called a [`Listener`]) and then receive acknowledgements to
//!   confirm to them that the message was received and, implicitly, acted upon
//!
//! [`two_way`]: two_way
//! [`two_way::channel()`]: two_way::channel()
//! [`Communicator`]: two_way::Communicator
//! [`acknowledge`]: acknowledge
//! [`acknowledge::channel()`]: acknowledge::channel()
//! [`Speaker`]: acknowledge::Speaker
//! [`Listener`]: acknowledge::Listener
pub mod acknowledge;
pub mod two_way;
