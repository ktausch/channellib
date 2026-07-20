A crate with tools to enrich communication between threads powered by `mpsc::channel`. To view the up-to-date documentation, clone this repo and run

```bash
cargo doc
```

# Channels

The main centerpieces of `channellib` are the `two_way::channel()` and the `acknowledge::channel()`.

## `two_way::channel()`

The `two_way::channel()` is a channel designed for the two sides to be symmetric. Both sides can send and receive messages. the only assymetry in the setup is that the types of the data sent in the two directions can be different. Both sides are orchestrated by `Communicator` instances.

```rust
use channellib::two_way;

let (first, second) = two_way::channel();
assert_eq!(first.try_recv().unwrap_err(), two_way::TryRecvError::Empty);
first.send(123).unwrap();
second.send("abc").unwrap();
assert_eq!(first.recv().unwrap(), "abc");
drop(first);
assert_eq!(second.try_recv().unwrap(), 123);
```

## `acknowledge::channel()`

The `acknowledge::channel()` is a channel with a sending side and a receiving side, similar to `std::sync::mpsc::channel()`, but where the listener can send acknowledgements back to the receiver. There are two main variants: the `acknowledge::channel()` and `acknowledge::custom_channel()`. Both variants are manipulated via the `Speaker` and `Listener` structs.

The `acknowledge::channel()` is useful for the situation where the `Speaker`'s thread needs to know whether the `Listener` has processed the message.

```rust
use channellib::acknowledge;

let (speaker, listener) = acknowledge::channel();
speaker.send(1);
// not acknowledged yet
assert_eq!(speaker.read_acknowledgement().unwrap(), None);
assert_eq!(listener.recv().unwrap(), 1);
// acknowledged when listener.recv() is called; note that
// acknowledgement is () because we are using basic acknowledge::channel
assert_eq!(speaker.read_acknowledgement().unwrap(), Some(()));
```

The `acknowledge::custom_channel()` is used when the acknowledgement should be richer than just `()`. It accepts an argument that is a function that determines how the acknowledgement should be formed.

```rust
use std::cell::RefCell;
use channellib::acknowledge;

let num_received = RefCell::new(0);
let (speaker, listener) = acknowledge::custom_channel(|&sent| {
    *num_received.borrow_mut() += 1;
    sent + *num_received.borrow()
});
for _ in 0..3 {
    speaker.send(10).unwrap();
}
for _ in 0..3 {
    assert_eq!(listener.recv().unwrap(), 10);
}
let acknowledgements = speaker.read_acknowledgements().collect::<Vec<_>>();
assert_eq!(acknowledgements.as_array().unwrap(), &[11, 12, 13]);
```

See the documentation in `src/two_way.rs` and `src/acknowledge.rs` for details.
