use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use serde::{Deserialize, Serialize};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{account::ReadableAccount, commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{collections::HashSet, sync::Arc, time::Duration};
use crate::block_info::TransactionAccount;
use tokio::sync::RwLock;

lazy_static::lazy_static! {
    static ref ALTS_IN_STORE: IntGauge =
       register_int_gauge!(opts!("banking_stage_sidecar_alts_stored", "Alts stored in sidecar")).unwrap();
}

#[derive(Clone)]
pub struct ALTStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
    under_loading: Arc<RwLock<HashSet<Pubkey>>>,
}

impl ALTStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
            under_loading: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn load_all_alts(&self, alts_list: Vec<Pubkey>) {
        let alts_list = {
            let lk = self.under_loading.read().await;
            alts_list
            .iter()
            .filter(|x| !self.map.contains_key(x) && !lk.contains(x))
            .cloned()
            .collect_vec()
        };

        if alts_list.is_empty() {
            return;
        }
        // add in loading list
        self.is_loading(&alts_list).await;

        log::info!("Preloading {} ALTs", alts_list.len());

        for batches in alts_list.chunks(1000).map(|x| x.to_vec()) {
            let tasks = batches.chunks(100).map(|batch| {
                let batch = batch.to_vec();
                let rpc_client = self.rpc_client.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(multiple_accounts)) = tokio::time::timeout( Duration::from_secs(30), rpc_client
                        .get_multiple_accounts_with_commitment(
                            &batch,
                            CommitmentConfig::processed(),
                        ))
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
            if let Err(_) =
                tokio::time::timeout(Duration::from_secs(20), futures::future::join_all(tasks))
                    .await
            {
                log::warn!("timedout getting {} alts", alts_list.len());
            }
        }

        self.finished_loading(&alts_list).await;
        ALTS_IN_STORE.set(alts_list.len() as i64);

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

    pub async fn reload_alt_from_rpc(&self, alt: &Pubkey) {
        self.is_loading(&vec![*alt]).await;
        let response = self
            .rpc_client
            .get_account_with_commitment(alt, CommitmentConfig::processed())
            .await;
        if let Ok(account_res) = response {
            if let Some(account) = account_res.value {
                self.save_account(alt, account.data());
            }
        }
        self.finished_loading(&vec![*alt]).await;
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
        
        let mut times = 0;
        const MAX_TIMES_RETRY : usize = 100;
        while self.is_loading_contains(alt).await {
            if times > MAX_TIMES_RETRY {
                break;
            }
            times += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        match self.load_accounts(alt, write_accounts, read_account).await {
            Some(x) => x,
            None => {
                //load alt
                self.reload_alt_from_rpc(&alt).await;
                match self.load_accounts(alt, write_accounts, read_account).await {
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

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize::<BinaryALTData>(&BinaryALTData::new(&self.map)).unwrap()
    }

    pub fn load_binary(&self, binary_data: Vec<u8>) {
        let binary_alt_data = bincode::deserialize::<BinaryALTData>(&binary_data).unwrap();
        for (alt, accounts) in binary_alt_data.data.iter() {
            self.map.insert(alt.clone(), accounts.clone());
        }
    }

    async fn is_loading(&self,  alts_list: &Vec<Pubkey>) {
        let mut write = self.under_loading.write().await;
        for alt in alts_list {
            write.insert(alt.clone());
        }
    }

    async fn finished_loading(&self,  alts_list: &Vec<Pubkey>) {
        let mut write = self.under_loading.write().await;
        for alt in alts_list {
            write.remove(alt);
        }
    }

    async fn is_loading_contains(&self, alt: &Pubkey) -> bool {
        let read = self.under_loading.read().await;
        read.contains(alt)
    }
}

#[derive(Serialize, Deserialize)]
pub struct BinaryALTData {
    pub data: Vec<(Pubkey, Vec<Pubkey>)>
}

impl BinaryALTData {
    pub fn new(map: &Arc<DashMap<Pubkey, Vec<Pubkey>>>) -> Self {
        let data = map.iter().map(|x| (x.key().clone(), x.value().clone())).collect_vec();
        Self {
            data
        }
    }
}