use dashmap::DashMap;
use itertools::Itertools;
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, slot_hashes::SlotHashes,
    slot_history::Slot,
};
use std::sync::Arc;

use crate::block_info::TransactionAccount;

pub struct ALTStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<Pubkey, Vec<u8>>>,
}

impl ALTStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
        }
    }

    pub async fn load_alt_from_rpc(&self, alt: &Pubkey) {
        if !self.map.contains_key(&alt) {
            self.reload_alt_from_rpc(&alt).await;
        }
    }

    pub async fn reload_alt_from_rpc(&self, alt: &Pubkey) {
        let response = self
            .rpc_client
            .get_account_with_commitment(alt, CommitmentConfig::processed())
            .await;
        if let Ok(account_res) = response {
            if let Some(account) = account_res.value {
                self.map.insert(*alt, account.data);
            }
        }
    }

    pub async fn load_accounts(
        &self,
        slot: Slot,
        alt: &Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Option<Vec<TransactionAccount>> {
        match self.map.get(&alt) {
            Some(account) => {
                let lookup_table = AddressLookupTable::deserialize(&account.value()).unwrap();
                let write_accounts =
                    lookup_table.lookup(slot, write_accounts, &SlotHashes::default());
                let read_account = lookup_table.lookup(slot, read_account, &SlotHashes::default());

                let write_accounts = if let Ok(write_accounts) = write_accounts {
                    write_accounts
                } else {
                    return None;
                };
                let read_account = if let Ok(read_account) = read_account {
                    read_account
                } else {
                    return None;
                };

                let wa = write_accounts
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.to_string(),
                        is_writable: true,
                        is_signer: false,
                        is_alt: true,
                    })
                    .collect_vec();
                let ra = read_account
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.to_string(),
                        is_writable: false,
                        is_signer: false,
                        is_alt: true,
                    })
                    .collect_vec();
                Some([wa, ra].concat())
            }
            None => Some(vec![]),
        }
    }

    pub async fn get_accounts(
        &self,
        current_slot: Slot,
        alt: &Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Vec<TransactionAccount> {
        self.load_alt_from_rpc(&alt).await;
        match self
            .load_accounts(current_slot, alt, write_accounts, read_account)
            .await
        {
            Some(x) => x,
            None => {
                //load alt
                self.reload_alt_from_rpc(&alt).await;
                match self
                    .load_accounts(current_slot, alt, write_accounts, read_account)
                    .await
                {
                    Some(x) => x,
                    None => {
                        // reloading did not work
                        log::error!("cannot load alt even after");
                        vec![]
                    }
                }
            }
        }
    }
}
