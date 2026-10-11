use bytes::Bytes;

pub const HEARTBEAT_RATE_SEC: u64 = 30;

// for outer control flow
pub const RETRY_INTERVAL_MS: u64 = 5000;
pub const INIT_INTERVAL_MS: u64 = 100;
pub const INIT_RETRY_INTERVAL_SEC: u64 = 5;

pub const WEB_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36";
pub const WEB_ORIGIN: &str = concat!("https://live.", include!("../name"), ".com");

pub fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().try_into().unwrap()
}

pub struct Payload {
    pub time: u64,
    pub data: Bytes,
}

impl Payload {
    pub fn new_now(data: Bytes) -> Payload {
        Payload {
            time: now(),
            data,
        }
    }
}

pub mod package;

pub mod schema;

pub mod stream;
