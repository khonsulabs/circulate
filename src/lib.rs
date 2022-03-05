#![doc = include_str!("../.rustme/docs.md")]
#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(clippy::option_if_let_else)]

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use arc_bytes::OwnedBytes;
pub use flume;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// A `PubSub` message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// The topic of the message.
    pub topic: String,
    /// The payload of the message.
    pub payload: OwnedBytes,
}

impl Message {
    /// Creates a new message.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub fn new<S: Into<String>, P: Serialize>(topic: S, payload: &P) -> Result<Self, pot::Error> {
        Ok(Self::raw(topic, pot::to_vec(payload)?))
    }

    /// Creates a new message with a raw payload.
    pub fn raw<S: Into<String>, B: Into<OwnedBytes>>(topic: S, payload: B) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
        }
    }

    /// Deserialize the payload as `P` using `pot`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to deserialize with `pot`.
    pub fn payload<'a, P: Deserialize<'a>>(&'a self) -> Result<P, pot::Error> {
        pot::from_slice(&self.payload).map_err(pot::Error::from)
    }
}

type TopicId = u64;
type SubscriberId = u64;

#[derive(Default, Debug, Clone)]
/// Manages subscriptions and notifications for `PubSub`.
pub struct Relay {
    data: Arc<Data>,
}

#[derive(Debug, Default)]
struct Data {
    subscribers: RwLock<HashMap<SubscriberId, SubscriberInfo>>,
    topics: RwLock<HashMap<String, TopicId>>,
    subscriptions: RwLock<HashMap<TopicId, HashSet<SubscriberId>>>,
    last_topic_id: AtomicU64,
    last_subscriber_id: AtomicU64,
}

impl Relay {
    /// Create a new [`Subscriber`] for this relay.
    pub fn create_subscriber(&self) -> Subscriber {
        let mut subscribers = self.data.subscribers.write();
        let id = self.data.last_subscriber_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = flume::unbounded();
        subscribers.insert(
            id,
            SubscriberInfo {
                sender,
                topics: HashSet::default(),
            },
        );
        Subscriber {
            data: Arc::new(SubscriberData {
                id,
                receiver,
                relay: self.clone(),
            }),
        }
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub fn publish<S: Into<String> + Send, P: Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pot::Error> {
        let message = Message::new(topic, payload)?;
        self.publish_message(message);
        Ok(())
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    pub fn publish_raw<S: Into<String> + Send, P: Into<OwnedBytes> + Send>(
        &self,
        topic: S,
        payload: P,
    ) {
        let message = Message::raw(topic, payload);
        self.publish_message(message);
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` fails to serialize with `pot`.
    pub fn publish_to_all<P: Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), pot::Error> {
        topics
            .into_iter()
            .map(|topic| Message::new(topic, payload).map(|message| self.publish_message(message)))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(())
    }

    /// Publishes a `payload` to all subscribers of `topic`.
    pub async fn publish_raw_to_all(
        &self,
        topics: Vec<String>,
        payload: impl Into<OwnedBytes> + Send,
    ) {
        let payload = payload.into();
        for topic in topics {
            self.publish_message(Message {
                topic,
                payload: payload.clone(),
            });
        }
    }

    /// Publishes a message to all subscribers of its topic.
    pub fn publish_message(&self, message: Message) {
        if let Some(topic_id) = self.topic_id(&message.topic) {
            self.post_message_to_topic(message, topic_id);
        }
    }

    fn add_subscriber_to_topic(&self, subscriber_id: u64, topic: String) {
        let mut subscribers = self.data.subscribers.write();
        let mut topics = self.data.topics.write();
        let mut subscriptions = self.data.subscriptions.write();

        // Lookup or create a topic id
        let topic_id = *topics
            .entry(topic)
            .or_insert_with(|| self.data.last_topic_id.fetch_add(1, Ordering::SeqCst));
        if let Some(subscriber) = subscribers.get_mut(&subscriber_id) {
            subscriber.topics.insert(topic_id);
        }
        let subscribers = subscriptions
            .entry(topic_id)
            .or_insert_with(HashSet::default);
        subscribers.insert(subscriber_id);
    }

    fn remove_subscriber_from_topic(&self, subscriber_id: u64, topic: &str) {
        let mut subscribers = self.data.subscribers.write();
        let mut topics = self.data.topics.write();

        let remove_topic = if let Some(topic_id) = topics.get(topic) {
            if let Some(subscriber) = subscribers.get_mut(&subscriber_id) {
                if !subscriber.topics.remove(topic_id) {
                    // subscriber isn't subscribed to this topic
                    return;
                }
            } else {
                // subscriber_id is not subscribed anymore
                return;
            }

            let mut subscriptions = self.data.subscriptions.write();
            let remove_topic = if let Some(subscriptions) = subscriptions.get_mut(topic_id) {
                subscriptions.remove(&subscriber_id);
                subscriptions.is_empty()
            } else {
                // Shouldn't be reachable, but this allows cleanup to proceed if it does.
                true
            };

            if remove_topic {
                subscriptions.remove(topic_id);
                true
            } else {
                false
            }
        } else {
            false
        };
        if remove_topic {
            topics.remove(topic);
        }
    }

    fn topic_id(&self, topic: &str) -> Option<TopicId> {
        let topics = self.data.topics.read();
        topics.get(topic).copied()
    }

    fn post_message_to_topic(&self, message: Message, topic: TopicId) {
        let failures = {
            // For an optimal-flow case, we're going to acquire read permissions
            // only, allowing messages to be posted in parallel. This block is
            // responsible for returning `SubscriberId`s that failed to send.
            let subscribers = self.data.subscribers.read();
            let subscriptions = self.data.subscriptions.read();
            if let Some(registered) = subscriptions.get(&topic) {
                let message = Arc::new(message);
                let failures = registered
                    .iter()
                    .filter_map(|id| {
                        subscribers.get(id).and_then(|subscriber| {
                            let message = message.clone();
                            if subscriber.sender.send(message).is_ok() {
                                None
                            } else {
                                Some(*id)
                            }
                        })
                    })
                    .collect::<Vec<SubscriberId>>();

                failures
            } else {
                return;
            }
        };

        if !failures.is_empty() {
            for failed in failures {
                self.unsubscribe_all(failed);
            }
        }
    }

    fn unsubscribe_all(&self, subscriber_id: SubscriberId) {
        let mut subscribers = self.data.subscribers.write();
        let mut topics = self.data.topics.write();
        let mut subscriptions = self.data.subscriptions.write();
        if let Some(subscriber) = subscribers.remove(&subscriber_id) {
            for topic in &subscriber.topics {
                let remove = if let Some(subscriptions) = subscriptions.get_mut(topic) {
                    subscriptions.remove(&subscriber_id);
                    subscriptions.is_empty()
                } else {
                    false
                };

                if remove {
                    subscriptions.remove(topic);
                    topics.retain(|_name, id| id != topic);
                }
            }
        }
    }
}

#[derive(Debug)]
struct SubscriberInfo {
    sender: flume::Sender<Arc<Message>>,
    topics: HashSet<u64>,
}

/// A subscriber for [`Message`]s published to subscribed topics.
#[derive(Debug, Clone)]
#[must_use]
pub struct Subscriber {
    data: Arc<SubscriberData>,
}

impl Subscriber {
    /// Subscribe to [`Message`]s published to `topic`.
    pub fn subscribe_to<S: Into<String> + Send>(&self, topic: S) {
        self.data
            .relay
            .add_subscriber_to_topic(self.data.id, topic.into());
    }

    /// Unsubscribe from [`Message`]s published to `topic`.
    pub fn unsubscribe_from(&self, topic: &str) {
        self.data
            .relay
            .remove_subscriber_from_topic(self.data.id, topic);
    }

    /// Returns the receiver to receive [`Message`]s.
    #[must_use]
    pub fn receiver(&self) -> &'_ flume::Receiver<Arc<Message>> {
        &self.data.receiver
    }

    #[must_use]
    /// Returns the unique ID of the subscriber.
    pub fn id(&self) -> u64 {
        self.data.id
    }
}

#[derive(Debug)]
struct SubscriberData {
    id: SubscriberId,
    relay: Relay,
    receiver: flume::Receiver<Arc<Message>>,
}

impl Drop for SubscriberData {
    fn drop(&mut self) {
        self.relay.unsubscribe_all(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_pubsub_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber();
        subscriber.subscribe_to("mytopic");
        pubsub.publish("mytopic", &String::from("test"))?;
        let receiver = subscriber.receiver().clone();
        let message = receiver.recv_async().await.expect("No message received");
        assert_eq!(message.topic, "mytopic");
        assert_eq!(message.payload::<String>()?, "test");
        // The message should only be received once.
        assert!(matches!(
            tokio::task::spawn_blocking(
                move || receiver.recv_timeout(std::time::Duration::from_millis(100))
            )
            .await,
            Ok(Err(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn multiple_subscribers_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber_a = pubsub.create_subscriber();
        let subscriber_ab = pubsub.create_subscriber();
        subscriber_a.subscribe_to("a");
        subscriber_ab.subscribe_to("a");
        subscriber_ab.subscribe_to("b");

        pubsub.publish("a", &String::from("a1"))?;
        pubsub.publish("b", &String::from("b1"))?;
        pubsub.publish("a", &String::from("a2"))?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber_a.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_a.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a2");

        let message = subscriber_ab.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_ab.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "b1");
        let message = subscriber_ab.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a2");

        Ok(())
    }

    #[tokio::test]
    async fn unsubscribe_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber();
        subscriber.subscribe_to("a");

        pubsub.publish("a", &String::from("a1"))?;
        subscriber.unsubscribe_from("a");
        pubsub.publish("a", &String::from("a2"))?;
        subscriber.subscribe_to("a");
        pubsub.publish("a", &String::from("a3"))?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a3");

        Ok(())
    }

    #[tokio::test]
    async fn drop_and_send_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber_a = pubsub.create_subscriber();
        let subscriber_to_drop = pubsub.create_subscriber();
        subscriber_a.subscribe_to("a");
        subscriber_to_drop.subscribe_to("a");

        pubsub.publish("a", &String::from("a1"))?;
        drop(subscriber_to_drop);
        pubsub.publish("a", &String::from("a2"))?;

        // Check subscriber_a for a1 and a2.
        let message = subscriber_a.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a1");
        let message = subscriber_a.receiver().recv()?;
        assert_eq!(message.payload::<String>()?, "a2");

        let subscribers = pubsub.data.subscribers.read();
        assert_eq!(subscribers.len(), 1);
        let topics = pubsub.data.topics.read();
        let topic_id = topics.get("a").expect("topic not found");
        let subscriptions = pubsub.data.subscriptions.read();
        assert_eq!(
            subscriptions
                .get(topic_id)
                .expect("subscriptions not found")
                .len(),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn drop_cleanup_test() -> anyhow::Result<()> {
        let pubsub = Relay::default();
        let subscriber = pubsub.create_subscriber();
        subscriber.subscribe_to("a");
        drop(subscriber);

        let subscribers = pubsub.data.subscribers.read();
        assert_eq!(subscribers.len(), 0);
        let subscriptions = pubsub.data.subscriptions.read();
        assert_eq!(subscriptions.len(), 0);
        let topics = pubsub.data.topics.read();
        assert_eq!(topics.len(), 0);

        Ok(())
    }
}
