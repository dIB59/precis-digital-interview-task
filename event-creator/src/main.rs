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
    loop {
        let event = generate_event();
        let json = serde_json::to_string(&event).unwrap();
        println!("{}", json);
        thread::sleep(Duration::from_secs(1)); // Simulate real-time event generation
    }
}
