//! A crate with tools for making enriched channels for communication between threads.
//!
//! There are three main types of channels supported by this crate:
//! - [`two_way::channel()`] provides a channel where both sides can both send and receive messages
//!   using a [`Communicator`] object (see [`two_way`]).
//! - [`acknowledge::channel()`] provides a channel where the sender (called a [`Speaker`]) can
//!   send messages to the receiver (called a [`Listener`]) and then receive acknowledgements to
//!   confirm to them that the message was received and, implicitly, acted upon.
//! - [`supplement::channel()`] provides a multi-provider, single-consumer channel with
//!   customizable input enrichment (via the [`SendSupplementer`] trait) and output
//!   handling (via the [`EventAccepter`] trait). The sending side(s) of the stream are
//!   orchestrated via the [`Producer`] struct and the receiving side is handled by the
//!   [`Consumer`] struct.
//!
//! [`two_way`]: two_way
//! [`two_way::channel()`]: two_way::channel()
//! [`Communicator`]: two_way::Communicator
//! [`acknowledge`]: acknowledge
//! [`acknowledge::channel()`]: acknowledge::channel()
//! [`Speaker`]: acknowledge::Speaker
//! [`Listener`]: acknowledge::Listener
//! [`supplement::channel()`]: supplement::channel()
//! [`SendSupplementer`]: supplement::SendSupplementer
//! [`Producer`]: supplement::Producer
//! [`EventAccepter`]: supplement::EventAccepter
//! [`Consumer`]: supplement::Consumer
pub mod acknowledge;
pub mod supplement;
pub mod two_way;
