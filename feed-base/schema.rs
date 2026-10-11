use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize)]
pub struct InitRequest {
    pub uid: u64,
    pub roomid: u32,
    pub protover: u8,
    pub buvid: String,
    pub platform: &'static str,
    pub r#type: u8,
    pub key: String,
}

impl InitRequest {
    pub fn new_v3_web_with_access(roomid: u32, uid: u64, devid3: String, token: String) -> InitRequest {
        InitRequest {
            uid,
            roomid,
            protover: 3,
            buvid: devid3,
            platform: "web",
            r#type: 2,
            key: token,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct InitResponse {
    pub code: i32,
}
