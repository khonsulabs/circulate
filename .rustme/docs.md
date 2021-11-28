[![crate version](https://img.shields.io/crates/v/circulate.svg)](https://crates.io/crates/circulate)
[![Live Build Status](https://img.shields.io/github/workflow/status/khonsulabs/circulate/Tests/main)](https://github.com/khonsulabs/circulate/actions?query=workflow:Tests)
[![Documentation for `main` branch](https://img.shields.io/badge/docs-main-informational)](https://khonsulabs.github.io/circulate/main/circulate/)

Circulate is a lightweight async [PubSub](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern) framework. It currently requires tokio.

This project is written for [BonsaiDb](https://github.com/khonsulabs/bonsaidb). However, it's a general-purpose PubSub implementation that can be utilized in any tokio-based Rust codebase.

```rust
let relay = Relay::default();
let subscriber = relay.create_subscriber().await;

subscriber.subscribe_to("some topic").await;

relay.publish("some topic", &AnySerializableType).await?;

let message = subscriber.receiver().recv_async().await?;
println!(
    "Received message on topic {}: {:?}",
    message.topic, 
    message.payload::<AnySerializableType>()?
);
```
