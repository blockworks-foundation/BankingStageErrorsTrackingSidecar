use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{account::ReadableAccount, commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{str::FromStr, sync::Arc, time::Duration};

use crate::block_info::TransactionAccount;
lazy_static::lazy_static! {
    static ref ALTS_IN_STORE: IntGauge =
       register_int_gauge!(opts!("banking_stage_sidecar_alts_stored", "Alts stored in sidecar")).unwrap();
}

#[derive(Clone)]
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

    pub async fn load_all_alts(&self, alts_list: Vec<String>) {
        let alts_list = alts_list
            .iter()
            .map(|x| x.trim())
            .filter(|x| x.len() > 0)
            .map(|x| Pubkey::from_str(&x).unwrap())
            .collect_vec();
        log::info!("Preloading {} ALTs", alts_list.len());
        for batches in alts_list.chunks(1000).map(|x| x.to_vec()) {
            let tasks = batches.chunks(10).map(|batch| {
                let batch = batch.to_vec();
                let rpc_client = self.rpc_client.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    if let Ok(multiple_accounts) = rpc_client
                        .get_multiple_accounts_with_commitment(
                            &batch,
                            CommitmentConfig::processed(),
                        )
                        .await
                    {
                        for (index, acc) in multiple_accounts.value.iter().enumerate() {
                            if let Some(acc) = acc {
                                this.save_account(&batch[index], &acc.data);
                            }
                        }
                    }
                })
            });
            futures::future::join_all(tasks).await;
        }
        log::info!("Finished Loading {} ALTs", alts_list.len());
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
