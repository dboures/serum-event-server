
use std::collections::HashMap;
use anyhow::Result;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use log::*;

use futures_channel::mpsc::{unbounded, UnboundedSender};


type PeerMap = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

#[tokio::main]
async fn main() -> Result<()> {

    info!("startup");

    // open websocket
    let ws_address = "127.0.0.1:4000";
    info!("ws listen: {}", ws_address);
    let try_socket = TcpListener::bind(ws_address).await;
    let listener = try_socket.expect("Failed to bind");
    let peers = PeerMap::new(Mutex::new(HashMap::new()));

    tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(peers.clone(), stream, addr));
        }
    });

    loop {}
    Ok(())
}


async fn handle_connection(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
    info!("ws connected: {}", addr);
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the ws handshake occurred");
    let (ws_tx, _ws_rx) = ws_stream.split();

    // 1: publish channel in peer map
    let (chan_tx, chan_rx) = unbounded();
    {
        peer_map.lock().unwrap().insert(addr, chan_tx);
        info!("peer published: {}", addr);
    }

    // 2: forward all events from channel to peer


}