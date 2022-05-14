use anyhow::Result;
use async_channel::{unbounded, Sender};
use futures_util::{pin_mut, StreamExt};
use log::*;
use serum_event_server::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::pin;
use tokio_tungstenite::tungstenite::protocol::Message;

type PeerMap = Arc<Mutex<HashMap<SocketAddr, Sender<Message>>>>;

#[tokio::main]
async fn main() -> Result<()> {
    solana_logger::setup_with_default("info");
    info!("startup");

    // Start websocket
    let ws_address = "127.0.0.1:4000";
    info!("ws listen: {}", ws_address);
    let try_socket = TcpListener::bind(ws_address).await;
    let listener = try_socket.expect("Failed to bind");
    let peers = PeerMap::new(Mutex::new(HashMap::new()));
    let peers_ref_thread = peers.clone();

    // Open channel with ws that listens for Serum account data changes
    let (account_write_queue_sender, event_receiver) =
        serum_listener::handle_account_updates().await?;

    // Start thread that relays events to all clients
    tokio::spawn(async move {
        pin!(event_receiver);
        loop {
            let message = event_receiver.recv().await.unwrap();
            let mut peer_copy = peers_ref_thread.lock().unwrap().clone();

            for (addr, chan_tx) in peer_copy.iter_mut() {
                debug!("  > {}", addr);
                let json = serde_json::to_string(&message);
                chan_tx.send(Message::Text(json.unwrap())).await.unwrap()
            }
        }
    });

    // Handle client connections
    tokio::spawn(async move {
        while let Ok((stream, addr)) = listener.accept().await {
            tokio::spawn(handle_connection(peers.clone(), stream, addr));
        }
    });

    serum_listener::watch_chain(account_write_queue_sender).await;

    Ok(())
}

/// Handles a client connection by:
/// 1. Saving the connection addr and channel sender in the peer_map
/// 2. Forwarding all Serum events received by the channel receiver to each client via the ws
async fn handle_connection(peer_map: PeerMap, raw_stream: TcpStream, addr: SocketAddr) {
    info!("ws connected: {}", addr);
    let ws_stream = tokio_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the ws handshake occurred");
    let (ws_tx, _ws_rx) = ws_stream.split();

    // 1: Save each ws connection and async-channel sender in Hashmap to relay events
    let (chan_tx, chan_rx) = unbounded();
    {
        peer_map.lock().unwrap().insert(addr, chan_tx);
        info!("peer published: {}", addr);
    }

    // 2: forward all events from channel to peer
    let forward_updates = chan_rx.map(Ok).forward(ws_tx);
    pin_mut!(forward_updates);
    let result_forward = forward_updates.await;

    //info!("ws disconnected: {} err: {:?}", &addr, result_forward);
    peer_map.lock().unwrap().remove(&addr);
    result_forward.unwrap();
}
