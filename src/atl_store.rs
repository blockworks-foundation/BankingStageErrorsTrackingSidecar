use dashmap::DashMap;
use itertools::Itertools;
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, slot_hashes::SlotHashes, slot_history::Slot};
use std::sync::Arc;

use crate::block_info::TransactionAccount;

pub struct ATLStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<String, Vec<u8>>>,
}

impl ATLStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
        }
    }

    pub async fn load_atl_from_rpc(&self, atl: &Pubkey) {
        if !self.map.contains_key(&atl.to_string()) {
            if let Ok(account) = self.rpc_client.get_account(&atl).await {
                self.map.insert(atl.to_string(), account.data);
            }
        }
    }

    pub async fn get_accounts(
        &self,
        current_slot: Slot,
        atl: Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Vec<TransactionAccount> {
        self.load_atl_from_rpc(&atl).await;
        match self.map.get(&atl.to_string()) {
            Some(account) => {
                let lookup_table = AddressLookupTable::deserialize(&account.value()).unwrap();
                let write_accounts = lookup_table
                    .lookup(current_slot, write_accounts, &SlotHashes::default())
                    .unwrap();
                let read_account = lookup_table
                    .lookup(current_slot, read_account, &SlotHashes::default())
                    .unwrap();

                let wa = write_accounts
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.to_string(),
                        is_writable: true,
                        is_signer: false,
                        is_atl: true,
                    })
                    .collect_vec();
                let ra = read_account
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.to_string(),
                        is_writable: false,
                        is_signer: false,
                        is_atl: true,
                    })
                    .collect_vec();
                [wa, ra].concat()
            }
            None => {
                vec![]
            }
        }
    }
}
