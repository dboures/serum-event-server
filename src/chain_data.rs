use {
    solana_sdk::account::AccountSharedData, solana_sdk::pubkey::Pubkey, std::collections::HashMap,
};

#[derive(Clone, Debug)]
pub struct AccountData {
    pub slot: u64,
    pub write_version: u64,
    pub account: AccountSharedData,
}

/// Track account writes
///
/// - use account() to retrieve the current data for an account
pub struct ChainData {
    /// writes to accounts, only the latest value is retained
    accounts: HashMap<Pubkey, AccountData>,
}

impl ChainData {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    // Simplistic implementation, does not take into account chain forks, see: https://github.com/blockworks-foundation/solana-accountsdb-connector/blob/ac3eb65e5c28cb897f3edc9a643141bf8cdfac78/lib/src/chain_data.rs
    pub fn update_account(&mut self, pubkey: Pubkey, account: AccountData) {
        self.accounts.insert(pubkey, account);
    }

    /// Ref to the most recent live write of the pubkey
    pub fn account<'a>(&'a self, pubkey: &Pubkey) -> anyhow::Result<&'a AccountData> {
        self.accounts
            .get(pubkey)
            .ok_or_else(|| anyhow::anyhow!("account {} not found", pubkey))
    }
}

impl Default for ChainData {
    fn default() -> Self {
        Self::new()
    }
}
