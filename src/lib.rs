use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;

pub mod chain_data;
pub mod serum_listener;

pub use chain_data::*;
pub use serum_listener::*;

#[derive(Clone, PartialEq, Debug)]
pub struct AccountWrite {
    pub pubkey: Pubkey,
    pub slot: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub is_selected: bool,
}

impl AccountWrite {
    fn from(pubkey: Pubkey, slot: u64, write_version: u64, account: Account) -> AccountWrite {
        AccountWrite {
            pubkey,
            slot,
            write_version,
            lamports: account.lamports,
            owner: account.owner,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data,
            is_selected: true,
        }
    }
}

trait AnyhowWrap {
    type Value;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
    type Value = T;
    fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
        self.map_err(|err| anyhow::anyhow!("{:?}", err))
    }
}
