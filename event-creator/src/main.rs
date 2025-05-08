use std::env;

use chrono::Utc;
use rand::Rng;
use serde::Serialize;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;

#[derive(Serialize)]
struct Event {
    timestamp: String,
    source: String,
    event_type: String,
    payload: String,
}

fn generate_event() -> Event {
    let sources = ["marketing", "user_activity", "monitoring"];
    let event_types = ["click", "login", "error", "conversion"];
    let payloads = [
        "User clicked on ad",
        "User logged in",
        "CPU usage high",
        "Campaign conversion recorded",
    ];

    let mut rng = rand::thread_rng();
    let index = rng.gen_range(0..sources.len());

    Event {
        timestamp: Utc::now().to_rfc3339(),
        source: sources[index].to_string(),
        event_type: event_types[index].to_string(),
        payload: payloads[index].to_string(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
   unsafe { env::set_var("PUBSUB_EMULATOR_HOST", "localhost:8085");}
    // 🧪 Use the emulator endpoint
    let config = ClientConfig {
        endpoint: "http://localhost:8085".to_string(),
        project_id: Some("local-project".to_string()),
        ..Default::default()
    };

    // ✅ Use default auth (will be ignored for emulator)
    let client = Client::new(config).await?;

    let topic = client.topic("events-topic");

    if !topic.exists(None).await? {
        topic.create(None, None).await?;
    }

    let mut publisher = topic.new_publisher(None);

    for _ in 0..5 {
        let event = generate_event();
        let json = serde_json::to_string(&event)?;

        let msg = PubsubMessage {
            data: json.into_bytes(),
            ordering_key: "".to_string(),
            ..Default::default()
        };

        let awaiter = publisher.publish(msg).await;
        let message_id = awaiter.get().await?;
        println!("✅ Published message with ID: {}", message_id);
    }

    publisher.shutdown().await;

    Ok(())
}