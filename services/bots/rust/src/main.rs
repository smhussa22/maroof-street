// Mean-reversion bot: tracks a 20-point rolling average of mid prices per symbol.
// When the current mid deviates by more than THRESHOLD ticks from the mean,
// it bets on reversion: sells when above average, buys when below.
// Directly opposes the momentum bot — when Go is piling in, Rust is fading it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{sleep, Duration};

mod orders {
    tonic::include_proto!("maroof_street.orders");
}

use orders::{
    order_service_client::OrderServiceClient, OrderDirection, OrderType, SubmitOrder,
};

const GATEWAY:   &str = "http://localhost:50051";
const REDIS_URL: &str = "redis://localhost:6379";
const API_KEY:   &str = "rust-bot-key-444";
const SYMBOLS:   &[&str] = &["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"];
const WINDOW:    usize = 20;
const THRESHOLD: f64 = 1.0; // tick deviation from mean to trigger
const QTY:       u32 = 10;

// the engine only has 1,000,000 id slots, so this bot owns the 100k block starting at 400,000 and wraps
static ORDER_ID: AtomicU64 = AtomicU64::new(0);

fn next_id() -> u64 {
    400_000 + ORDER_ID.fetch_add(1, Ordering::Relaxed) % 100_000
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = tonic::transport::Channel::from_static(GATEWAY)
        .connect()
        .await?;
    let mut client = OrderServiceClient::new(channel);

    let redis = redis::Client::open(REDIS_URL)?;
    let mut con = redis.get_multiplexed_async_connection().await?;

    let mut windows: HashMap<&str, VecDeque<f64>> = SYMBOLS
        .iter()
        .map(|&s| (s, VecDeque::with_capacity(WINDOW + 1)))
        .collect();

    println!("rust mean-reversion bot running");

    loop {
        sleep(Duration::from_secs(1)).await;

        for &sym in SYMBOLS {
            let top_key = format!("md:{}:top", sym);
            let top: HashMap<String, String> = redis::cmd("HGETALL")
                .arg(&top_key)
                .query_async(&mut con)
                .await
                .unwrap_or_default();

            let bid: f64 = top.get("best_bid").and_then(|v| v.parse().ok()).unwrap_or(0.0);
            let ask: f64 = top.get("best_ask").and_then(|v| v.parse().ok()).unwrap_or(0.0);

            if bid == 0.0 || ask == 0.0 {
                continue;
            }

            let mid = (bid + ask) / 2.0;
            let w = windows.get_mut(sym).unwrap();

            if w.len() == WINDOW {
                w.pop_front();
            }
            w.push_back(mid);

            if w.len() < WINDOW {
                continue; // still warming up the window
            }

            let mean: f64 = w.iter().sum::<f64>() / w.len() as f64;
            let deviation = mid - mean;

            if deviation.abs() <= THRESHOLD {
                continue;
            }

            let (price, direction) = if deviation > THRESHOLD {
                // price above average → expect reversion downward → sell at bid
                (bid as i32, OrderDirection::SideSell)
            } else {
                // price below average → expect reversion upward → buy at ask
                (ask as i32, OrderDirection::SideBuy)
            };

            let mut req = tonic::Request::new(SubmitOrder {
                api_key:         API_KEY.to_string(),
                symbol:          sym.to_string(),
                price,
                quantity:        QTY,
                order_direction: direction as i32,
                order_type:      OrderType::Fak as i32,
                client_order_id: next_id(),
            });
            req.metadata_mut().insert("x-api-key", API_KEY.parse().unwrap());

            match client.submit_order(req).await {
                Ok(_) => {
                    let side = if deviation > THRESHOLD { "sell" } else { "buy" };
                    println!("mean-rev {} {} {}@{} (dev={:.2})", sym, side, QTY, price, deviation);
                }
                Err(e) => eprintln!("rust-bot {}: {}", sym, e),
            }
        }
    }
}
