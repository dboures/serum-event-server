use crate::AnyhowWrap;
use crate::{AccountData, AccountWrite, ChainData};
use anyhow::Result;
use async_channel::Sender;
use log::*;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::RpcAccountInfoConfig;
use solana_client::rpc_response::{Response, RpcKeyedAccount};
use solana_rpc::rpc_pubsub::RpcSolPubSubClient;
use solana_sdk::account::Account;
use solana_sdk::account::WritableAccount;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake_history::Epoch;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::ws;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventMessage {
    pub event: String,
    pub market: String,
    pub queue: String,
}

pub struct MarketConfig {
    pub name: String,
    pub market: String,
    pub event_queue: String,
}

/// Listens to the
pub async fn handle_account_updates() -> Result<(
    async_channel::Sender<AccountWrite>,
    async_channel::Receiver<EventMessage>,
)> {
    let sol_config = MarketConfig {
        name: "SOL/USDC".to_string(),
        market: "9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT".to_string(),
        event_queue: "5KKsLVU6TcbVDK4BS6K1DGDxnh4Q9xjYJ8XaDCG5t8ht".to_string(),
    };

    let markets: Vec<MarketConfig> = vec![sol_config];

    // Channels to send account changes
    let (account_write_queue_sender, account_write_queue_receiver) =
        async_channel::unbounded::<AccountWrite>();

    // Event updates can be consumed by client connections, they contain all events for a markets
    let (event_sender, event_receiver) = async_channel::unbounded::<EventMessage>();

    let mut chain_cache = ChainData::new();
    let mut last_write_infos = HashMap::<String, (u64, u64)>::new(); //Slot and Write Version

    let relevant_pubkeys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.event_queue).unwrap())
        .collect::<HashSet<Pubkey>>();

    // Spawn a thread to monitor event queue account changes
    // Any time the account data changes on chain, it means an event happened (or events were consumed)
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(account_write) = account_write_queue_receiver.recv() => {
                    if !relevant_pubkeys.contains(&account_write.pubkey) {
                        continue;
                    }

                    chain_cache.update_account(
                        account_write.pubkey,
                        AccountData {
                            slot: account_write.slot,
                            write_version: account_write.write_version,
                            account: WritableAccount::create(
                                account_write.lamports,
                                account_write.data.clone(),
                                account_write.owner,
                                account_write.executable,
                                account_write.rent_epoch as Epoch,
                            ),
                        },
                    );
                }
                else => { }
            }

            for mkt in markets.iter() {
                let last_write_info = last_write_infos.get(&mkt.event_queue);
                let mkt_pk = mkt.event_queue.parse::<Pubkey>().unwrap();

                match chain_cache.account(&mkt_pk) {
                    Ok(account_info) => {
                        // only process if the account state changed
                        let write_info = (account_info.slot, account_info.write_version);
                        if write_info == *last_write_info.unwrap_or(&(0, 0)) {
                            continue;
                        }
                        last_write_infos.insert(mkt.event_queue.clone(), write_info);
                        info!(
                            "Slot: {:?}, Write version: {:?}",
                            account_info.slot, account_info.write_version
                        );

                        let account = &account_info.account;
                        //let (header, events) = deserialize_queue(account.data().to_vec()).unwrap();

                        event_sender
                            .try_send(EventMessage {
                                event: format!("Slot update: {}", account_info.slot),
                                market: mkt.name.clone(),
                                queue: mkt.event_queue.clone(),
                            })
                            .unwrap();
                    }
                    Err(_) => info!("chain_cache could not find {}", mkt.name),
                }
            }
        }
    });

    Ok((account_write_queue_sender, event_receiver))
}

/// Opens a connection to Solana blockchain that listens for account data changes (in Serum event queues).
/// When account data does change, the data is sent across an unbounded channel (received in )
pub async fn watch_chain(
    // config: &SourceConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
) {
    let (update_sender, update_receiver) = async_channel::unbounded::<Response<RpcKeyedAccount>>();

    // Spawn thread to listen for account updates
    tokio::spawn(async move {
        // if the websocket disconnects, we get no data in a while etc, reconnect and try again
        loop {
            let out = subcribe_to_chain(update_sender.clone());
            let _ = out.await;
        }
    });

    // Transmit account updates to the handler
    loop {
        let update = update_receiver.recv().await.unwrap();

        let account: Account = update.value.account.decode().unwrap();
        let pubkey = Pubkey::from_str(&update.value.pubkey).unwrap();

        account_write_queue_sender
            .send(AccountWrite::from(pubkey, update.context.slot, 0, account))
            .await
            .expect("send success");
    }
}

/// Subscribes to a Solana RPC, and send messages whenever an account's data is updated
async fn subcribe_to_chain(
    // TODO: magic strings
    // config: &SourceConfig,
    sender: Sender<Response<RpcKeyedAccount>>,
) -> Result<()> {
    let connect =
        ws::try_connect::<RpcSolPubSubClient>("https://ssc-dao.genesysgo.net/").map_err_anyhow()?;
    let client = connect.await.map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::processed()),
        data_slice: None,
    };

    let mut account_sub = client
        .account_subscribe(
            String::from("5KKsLVU6TcbVDK4BS6K1DGDxnh4Q9xjYJ8XaDCG5t8ht"),
            Some(account_info_config),
        )
        .map_err_anyhow()?;

    loop {
        tokio::select! {
            account = account_sub.next() => {
                match account {
                    Some(account) => {

                        //info!("account update");
                        let response = account.unwrap();
                        let new_resp = Response {
                            context: response.context,
                            value: RpcKeyedAccount {
                                pubkey: String::from("5KKsLVU6TcbVDK4BS6K1DGDxnh4Q9xjYJ8XaDCG5t8ht"),
                                account: response.value,
                            }
                        };
                        sender.send(new_resp).await.expect("sending must succeed");
                    },
                    None => {
                        warn!("account stream closed");
                        return Ok(());
                    },
                }
            },
            _ = (tokio::time::sleep(Duration::from_millis(1000))) => {
                //info!("pass");
                return Ok(())
            }
        }
    }
}
