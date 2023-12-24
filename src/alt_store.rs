use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{account::ReadableAccount, commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{sync::Arc, time::Duration};

use crate::block_info::TransactionAccount;
lazy_static::lazy_static! {
    static ref ALTS_IN_STORE: IntGauge =
       register_int_gauge!(opts!("banking_stage_sidecar_alts_stored", "Alts stored in sidecar")).unwrap();
}

pub struct ALTStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
}

impl ALTStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
        }
    }

    pub async fn load_all_alts(&self) {
        let get_pa = self.rpc_client.get_program_accounts(&solana_address_lookup_table_program::id()).await;
        if let Ok(pas) = get_pa {
            for (key, acc) in pas {
                self.save_account(&key, acc.data());
            }
        }
    }

    pub fn save_account(&self, address: &Pubkey, data: &[u8]) {
        let lookup_table = AddressLookupTable::deserialize(&data).unwrap();
        if self
            .map
            .insert(address.clone(), lookup_table.addresses.to_vec())
            .is_none()
        {
            ALTS_IN_STORE.inc();
        }
        drop(lookup_table);
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
                self.save_account(alt, account.data());
            }
        }
    }

    pub async fn load_accounts(
        &self,
        alt: &Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Option<Vec<TransactionAccount>> {
        match self.map.get(&alt) {
            Some(lookup_table) => {
                if write_accounts
                    .iter()
                    .any(|x| *x as usize >= lookup_table.len())
                    || read_account
                        .iter()
                        .any(|x| *x as usize >= lookup_table.len())
                {
                    return None;
                }
                let write_accounts = write_accounts
                    .iter()
                    .map(|i| lookup_table[*i as usize])
                    .collect_vec();
                let read_account = read_account
                    .iter()
                    .map(|i| lookup_table[*i as usize])
                    .collect_vec();

                let wa = write_accounts
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.clone(),
                        is_writable: true,
                        is_signer: false,
                        is_alt: true,
                    })
                    .collect_vec();
                let ra = read_account
                    .iter()
                    .map(|key| TransactionAccount {
                        key: key.clone(),
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
        alt: &Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Vec<TransactionAccount> {
        self.load_alt_from_rpc(&alt).await;
        match self.load_accounts(alt, write_accounts, read_account).await {
            Some(x) => x,
            None => {
                //load alt
                self.reload_alt_from_rpc(&alt).await;
                match self.load_accounts(alt, write_accounts, read_account).await {
                    Some(x) => x,
                    None => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        // reloading did not work
                        log::error!("cannot load alt even after");
                        vec![]
                    }
                }
            }
        }
    }
}
