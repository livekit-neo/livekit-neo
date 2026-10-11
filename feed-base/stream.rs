use std::time::Duration;
use futures_util::StreamExt;
use async_io::Timer;
use async_net::TcpStream;
use async_tungstenite::{
    WebSocketStream, WebSocketReceiver,
    smol::ClientStream,
    tungstenite::{protocol::Message, Error as WsError},
};
use crate::{WEB_USER_AGENT, WEB_ORIGIN, HEARTBEAT_RATE_SEC, Payload, package::Package, schema::InitRequest};

#[inline]
fn create_init_request(roomid: u32, uid: u64, devid3: String, token: String) -> Package {
    Package::InitRequest(serde_json::to_string(&InitRequest::new_v3_web_with_access(roomid, uid, devid3, token)).unwrap())
}

pub struct FeedStream<T> {
    roomid: u32,
    rx: T,
}

type WsStream = WebSocketStream<ClientStream<TcpStream>>;
type WsStreamRx = WebSocketReceiver<ClientStream<TcpStream>>;
pub type WsFeedStream = FeedStream<WsStreamRx>;

async fn ws_connect(host: &str, port: u16) -> Result<WsStream, WsError> {
    use async_tungstenite::{smol::connect_async, tungstenite::{handshake::client::{Request, generate_key}, http::{Uri, header}}};
    let req = Request::builder()
        .method("GET")
        .header(header::HOST, host)
        .header(header::CONNECTION, "Upgrade")
        .header(header::USER_AGENT, WEB_USER_AGENT)
        .header(header::ORIGIN, WEB_ORIGIN)
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, generate_key())
        .uri(Uri::builder()
            .scheme("wss")
            .authority(format!("{host}:{port}"))
            .path_and_query("/sub")
            .build()?)
        .body(())?;
    let (stream, _) = connect_async(req).await?;
    Ok(stream)
}

impl WsFeedStream {
    pub async fn connect_ws(host: &str, port: u16, roomid: u32, uid: u64, devid3: String, token: String) -> Result<WsFeedStream, WsError> {
        let stream = ws_connect(host, port).await?;
        let (mut tx, rx) = stream.split();
        tracing::debug!("[{: >10}] (ws) connected", roomid);

        let init = Message::Binary(create_init_request(roomid, uid, devid3, token).encode().unwrap().into());
        tx.send(init).await?;
        tracing::debug!("[{: >10}] (ws) sent: init", roomid);

        async_global_executor::spawn(async move {
            let heartbeat = Message::Binary(Package::HeartbeatRequest.encode().unwrap().into());
            let mut interval = Timer::interval(Duration::from_secs(HEARTBEAT_RATE_SEC));
            loop {
                interval.next().await;
                if let Err(error) = tx.send(heartbeat.clone()).await {
                    tracing::warn!("[{: >10}] (ws) stop sending: (heartbeat-thread) caused by {:?}", roomid, error);
                    break;
                }
                tracing::debug!("[{: >10}] (ws) sent: (heartbeat-thread) heartbeat", roomid);
            }
        }).detach();

        Ok(WsFeedStream { roomid, rx })
    }

    pub async fn recv(&mut self) -> Option<Payload> {
        loop {
            match self.rx.next().await {
                Some(Ok(message)) => match message {
                    Message::Binary(payload) => {
                        tracing::debug!("[{: >10}] (ws) recv: message {}", self.roomid, payload.len());
                        return Some(Payload::new_now(payload));
                    },
                    Message::Ping(payload) => {
                        if payload.is_empty() {
                            tracing::debug!("[{: >10}] (ws) recv: empty ping", self.roomid);
                        } else {
                            tracing::error!("[{: >10}] (ws) recv: non-empty ping {:?}", self.roomid, payload);
                        }
                        continue;
                    },
                    message => {
                        tracing::error!("[{: >10}] (ws) recv: unexpected message type {:?}", self.roomid, message);
                        continue;
                    },
                },
                Some(Err(error)) => {
                    tracing::warn!("[{: >10}] (ws) close: caused by {:?}", self.roomid, error);
                    return None;
                },
                None => {
                    tracing::warn!("[{: >10}] (ws) close: normally", self.roomid);
                    return None;
                },
            }
        }
    }
}
