use anyhow::{Context, Result};
use std::sync::Arc;
use xai_kafka::KafkaProducerConfig;
use xai_kafka::config::{KafkaConfig, KafkaConsumerConfig, SslConfig};
use xai_wily::WilyConfig;

use crate::{
    args,
    kafka::{
        tweet_events_listener::start_tweet_event_processing,
        tweet_events_listener_v2::start_tweet_event_processing_v2,
    },
};

// Kafka cluster DNS, topics, and SASL passwords must not be baked into the binary.
// Configure via environment (or CLI where `Args` provides fallbacks).

const ENV_KAFKA_SASL_PASSWORD: &str = "THUNDER_KAFKA_SASL_PASSWORD";
const ENV_KAFKA_PRODUCER_SASL_PASSWORD: &str = "THUNDER_KAFKA_PRODUCER_SASL_PASSWORD";

const ENV_TWEET_EVENTS_TOPIC: &str = "THUNDER_KAFKA_TWEET_EVENTS_TOPIC";
const ENV_TWEET_EVENTS_DEST: &str = "THUNDER_KAFKA_TWEET_EVENTS_DEST";
const ENV_IN_NETWORK_TOPIC: &str = "THUNDER_KAFKA_IN_NETWORK_EVENTS_TOPIC";
const ENV_IN_NETWORK_DEST: &str = "THUNDER_KAFKA_IN_NETWORK_EVENTS_DEST";

pub async fn start_kafka(
    args: &args::Args,
    post_store: Arc<crate::posts::post_store::PostStore>,
    user: &str,
    tx: tokio::sync::mpsc::Sender<i64>,
) -> Result<()> {
    let sasl_password = std::env::var(ENV_KAFKA_SASL_PASSWORD)
        .ok()
        .or_else(|| args.sasl_password.clone())
        .with_context(|| {
            format!(
                "Kafka consumer SASL password: set {ENV_KAFKA_SASL_PASSWORD} or pass the equivalent CLI flag"
            )
        })?;

    let producer_sasl_password = std::env::var(ENV_KAFKA_PRODUCER_SASL_PASSWORD)
        .ok()
        .or_else(|| args.producer_sasl_password.clone());

    let tweet_events_topic = std::env::var(ENV_TWEET_EVENTS_TOPIC).unwrap_or_default();
    let tweet_events_dest = std::env::var(ENV_TWEET_EVENTS_DEST).unwrap_or_default();
    let in_network_topic = std::env::var(ENV_IN_NETWORK_TOPIC).unwrap_or_default();
    let in_network_dest = std::env::var(ENV_IN_NETWORK_DEST).unwrap_or_default();

    if args.is_serving {
        let unique_id = uuid::Uuid::new_v4().to_string();

        let v2_tweet_events_consumer_config = KafkaConsumerConfig {
            base_config: KafkaConfig {
                dest: args.in_network_events_consumer_dest.clone(),
                topic: in_network_topic.clone(),
                wily_config: Some(WilyConfig::default()),
                ssl: Some(SslConfig {
                    security_protocol: args.security_protocol.clone(),
                    sasl_mechanism: Some(args.producer_sasl_mechanism.clone()),
                    sasl_username: Some(args.producer_sasl_username.clone()),
                    sasl_password: producer_sasl_password.clone(),
                }),
                ..Default::default()
            },
            group_id: format!("{}-{}", args.kafka_group_id, unique_id),
            auto_offset_reset: args.auto_offset_reset.clone(),
            fetch_timeout_ms: args.fetch_timeout_ms,
            max_partition_fetch_bytes: Some(1024 * 1024 * 100),
            skip_to_latest: args.skip_to_latest,
            ..Default::default()
        };

        // Start Kafka background tasks
        start_tweet_event_processing_v2(
            v2_tweet_events_consumer_config,
            Arc::clone(&post_store),
            args,
            tx,
        )
        .await;
    }

    // Only start Kafka processing and background tasks if not in serving mode
    if !args.is_serving {
        // Create Kafka consumer config
        let tweet_events_consumer_config = KafkaConsumerConfig {
            base_config: KafkaConfig {
                dest: tweet_events_dest.clone(),
                topic: tweet_events_topic.clone(),
                wily_config: Some(WilyConfig::default()),
                ssl: Some(SslConfig {
                    security_protocol: args.security_protocol.clone(),
                    sasl_mechanism: Some(args.sasl_mechanism.clone()),
                    sasl_username: Some(args.sasl_username.clone()),
                    sasl_password: Some(sasl_password.clone()),
                }),
                ..Default::default()
            },
            group_id: format!("{}-{}", args.kafka_group_id, user),
            auto_offset_reset: args.auto_offset_reset.clone(),
            enable_auto_commit: false,
            fetch_timeout_ms: args.fetch_timeout_ms,
            max_partition_fetch_bytes: Some(1024 * 1024 * 10),
            partitions: None,
            skip_to_latest: args.skip_to_latest,
            ..Default::default()
        };

        let producer_config = KafkaProducerConfig {
            base_config: KafkaConfig {
                dest: in_network_dest.clone(),
                topic: in_network_topic.clone(),
                wily_config: Some(WilyConfig::default()),
                ssl: Some(SslConfig {
                    security_protocol: args.security_protocol.clone(),
                    sasl_mechanism: Some(args.producer_sasl_mechanism.clone()),
                    sasl_username: Some(args.producer_sasl_username.clone()),
                    sasl_password: producer_sasl_password.clone(),
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        start_tweet_event_processing(tweet_events_consumer_config, producer_config, args).await;
    }

    Ok(())
}
