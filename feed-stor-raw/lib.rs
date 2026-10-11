use std::path::PathBuf;

use bytes::Bytes;
pub use crc32fast::hash as crc32;
use foundations::{byterepr_struct, byterepr::ByteRepr};
use kv_storage::{
    Metadata, error::Result,
    actor::{
        WriterContextConfig,
        auto_domain_id::{
            DomainWriteHandle as RawDomainWriteHandle,
            WriteHandle as RawWriteHandle,
            create as create_raw_handle,
            CloseHandle,
        }
    }
};
use livekit_feed_base::Payload;

pub const IDENT: &str = "livekit-feed-raw";

byterepr_struct! {
    #[derive(Debug)]
    pub struct Key {
        pub time: u64,
        pub hash: u32,
    }
}

impl Key {
    pub fn from_payload(payload: &Payload) -> Key {
        Key {
            time: payload.time,
            hash: crc32(&payload.data),
        }
    }
}

pub struct WriteHandle {
    raw: RawWriteHandle,
}

pub struct RoomWriteHandle {
    raw: RawDomainWriteHandle,
    roomid: u32,
}

impl WriteHandle {
    pub async fn open(path: PathBuf) -> Result<(WriteHandle, CloseHandle)> {
        let metadata = Metadata {
            ident: IDENT.as_bytes().into(),
        };
        let config = WriterContextConfig { path, metadata };
        let (raw, close_handle) = create_raw_handle(config).await?;
        Ok((WriteHandle { raw }, close_handle))
    }

    pub async fn open_room(&self, roomid: u32) -> Result<RoomWriteHandle> {
        let domain = Bytes::copy_from_slice(&roomid.to_be_bytes());
        let raw = self.raw.spawn_domain(domain).await?;
        Ok(RoomWriteHandle { raw, roomid })
    }
}

impl RoomWriteHandle {
    pub fn roomid(&self) -> u32 {
        self.roomid
    }

    pub async fn write_payload(&self, payload: Payload) -> Result<()> {
        let key = Key::from_payload(&payload);
        let key = Bytes::copy_from_slice(&key.to_bytes());
        self.raw.write_kv(key, payload.data).await
    }
}
