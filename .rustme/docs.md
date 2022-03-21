[![crate version](https://img.shields.io/crates/v/circulate.svg)](https://crates.io/crates/circulate)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/circulate/Tests/main)](https://github.com/khonsulabs/circulate/actions?query=workflow:Tests)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/circulate/main/circulate/)

Circulate is a lightweight
[PubSub](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern)
framework that supports both async and non-async code. This project is written
for [BonsaiDb](https://github.com/khonsulabs/bonsaidb). While BonsaiDb's async
relies upon tokio, this crate is runtime agnostic thanks to the excellent crate
[flume](https://github.com/zesterer/flume).

## Async Example

```rust
# #[derive(serde::Serialize, serde::Deserialize, Debug)]
# struct AnySerializableType;
# async fn example() -> Result<(), anyhow::Error> {
# use circulate::Relay;
let relay = Relay::default();
let subscriber = relay.create_subscriber();

subscriber.subscribe_to(&"some topic")?;

relay.publish(&"some topic", &AnySerializableType)?;

let message = subscriber.receiver().recv_async().await?;
println!(
    "Received message on topic {}: {:?}",
    message.topic::<String>()?, 
    message.payload::<AnySerializableType>()?
);
# Ok(())
# }
```

## Sync Example

```rust
# #[derive(serde::Serialize, serde::Deserialize, Debug)]
# struct AnySerializableType;
# fn example() -> Result<(), anyhow::Error> {
# use circulate::Relay;
let relay = Relay::default();
let subscriber = relay.create_subscriber();

subscriber.subscribe_to(&"some topic")?;

relay.publish(&"some topic", &AnySerializableType)?;

let message = subscriber.receiver().recv()?;
println!(
    "Received message on topic {}: {:?}",
    message.topic::<String>()?, 
    message.payload::<AnySerializableType>()?
);
# Ok(())
# }
```
