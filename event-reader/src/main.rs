use std::env;

use chrono::Utc;
use futures::StreamExt;
use google_cloud_googleapis::pubsub::v1::{PubsubMessage, ReceivedMessage};
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Event {
    timestamp: String,
    source: String,
    event_type: String,
    payload: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransformedEvent {
    processed_at: String,
    original_source: String,
    kind: String,
    details: String,
    local_transformer: bool,
}

fn transform_event(event: Event) -> TransformedEvent {
    TransformedEvent {
        processed_at: Utc::now().to_rfc3339(),
        original_source: event.source,
        kind: event.event_type,
        details: event.payload,
        local_transformer: true,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe { env::set_var("PUBSUB_EMULATOR_HOST", "localhost:8085"); }
    let config = ClientConfig {
        endpoint: "http://localhost:8085".to_string(),
        project_id: Some("local-project".to_string()),
        ..Default::default()
    };

    let client = Client::new(config).await?;
    let project_id ="local-project";
    let topic_name = "events-topic";
    let transformed_topic_name = "transformed-events-topic";
    let subscription_name = "transformer-subscription"; // Unique subscription for the transformer
    let fully_qualified_topic_name = format!("projects/{}/topics/{}", project_id, topic_name);

    // Ensure the original topic exists
    let topic = client.topic(topic_name);
    if !topic.exists(None).await? {
        topic.create(None, None).await?;
        println!("✅ Topic '{}' created.", topic_name);
    }

    // Ensure the transformed topic exists
    let transformed_topic = client.topic(transformed_topic_name);
    if !transformed_topic.exists(None).await? {
        transformed_topic.create(None, None).await?;
        println!("✅ Topic '{}' created.", transformed_topic_name);
    }

    // Ensure the subscription for the transformer exists
    let subscription = client.subscription(subscription_name);
    if !subscription.exists(None).await? {
        let sub_config = SubscriptionConfig::default();
        subscription
            .create(&fully_qualified_topic_name, sub_config, None)
            .await?;
        println!("✅ Subscription '{}' created on topic '{}'.", subscription_name, topic_name);
    }

    println!("\n👂 Listening for events on '{}' and transforming...", topic_name);
    let cancellation_token = CancellationToken::new();
    let ct_clone = cancellation_token.clone();
    let mut transformed_publisher = transformed_topic.new_publisher(None);

    let mut subscriber = client.subscription(subscription_name).subscribe(None).await?;

    while let Some(result) = subscriber.next().await {
       
                let message = &result.message;
                let message_data = message.data.clone();
                let data = String::from_utf8_lossy(&message_data).to_string();

                println!("📥 Received message: {}", data);

                match serde_json::from_str::<Event>(&data) {
                    Ok(event) => {
                        let transformed = transform_event(event);
                        println!("✨ Transformed event: {:?}", transformed);
                        match serde_json::to_string(&transformed) {
                            Ok(transformed_json) => {
                                let pubsub_message = PubsubMessage {
                                    data: transformed_json.into_bytes(),
                                    ordering_key: "".to_string(),
                                    ..Default::default()
                                };
                                match transformed_publisher.publish(pubsub_message).await.get().await {
                                    Ok(message_id) => {
                                        println!("✅ Published transformed message with ID: {}", message_id);
                                    }
                                    Err(e) => {
                                        eprintln!("❌ Error publishing transformed message: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("❌ Error serializing transformed event: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("❌ Error deserializing received message: {}", e);
                    }
                }
                match result.ack().await {
                    Ok(_) => println!("✅ Acknowledged message."),
                    Err(e) => eprintln!("❌ Error acknowledging message: {}", e),
                    
                }
    }
    

    println!("\n✨ Transformer finished (receive stream ended).");
    transformed_publisher.shutdown().await;

    Ok(())
}