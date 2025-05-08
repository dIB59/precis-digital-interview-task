use chrono::Utc;
use rand::Rng;
use serde::Serialize;
use std::thread;
use std::time::Duration;

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

fn main() {
    let events_per_batch = 5; // You can change this number

    loop {
        let mut batch = Vec::with_capacity(events_per_batch);

        for _ in 0..events_per_batch {
            batch.push(generate_event());
        }

        let json_batch = serde_json::to_string_pretty(&batch).unwrap();
        println!("{}", json_batch);

        thread::sleep(Duration::from_millis(20)); // Delay between batches
    }
}