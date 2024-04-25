use crate::block_info::TransactionAccount;
use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use serde::{Deserialize, Serialize};
use solana_address_lookup_table_program::state::AddressLookupTable;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::{sync::Arc, time::Duration};

lazy_static::lazy_static! {
    static ref ALTS_IN_STORE: IntGauge =
       register_int_gauge!(opts!("banking_stage_sidecar_alts_stored", "Alts stored in sidecar")).unwrap();

    static ref ALTS_IN_LOADING_QUEUE: IntGauge =
        register_int_gauge!(opts!("banking_stage_sidecar_alts_loading_queue", "Alts in loading queue in sidecar")).unwrap();
}

#[derive(Clone)]
pub struct ALTStore {
    rpc_client: Arc<RpcClient>,
    pub map: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
    loading_queue: Arc<async_channel::Sender<Pubkey>>,
}

impl ALTStore {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        let (sx, rx) = async_channel::unbounded();
        let alt_store = Self {
            rpc_client,
            map: Arc::new(DashMap::new()),
            loading_queue: Arc::new(sx),
        };

        {
            let instant = alt_store.clone();
            tokio::task::spawn(async move {
                loop {
                    if let Ok(pk) = rx.recv().await {
                        let mut alts_list = vec![pk];
                        ALTS_IN_LOADING_QUEUE.dec();
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        while let Ok(pk) = rx.try_recv() {
                            alts_list.push(pk);
                            ALTS_IN_LOADING_QUEUE.dec();
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        instant._load_all_alts(&alts_list).await;
                    }
                }
            });
        }

        alt_store
    }

    pub async fn load_alts_list(&self, alts_list: Vec<&Pubkey>) {
        log::info!("Preloading {} ALTs", alts_list.len());

        for batches in alts_list.chunks(1000) {
            let tasks = batches.chunks(100).map(|batch| {
                let batch: Vec<Pubkey> = batch.into_iter().map(|pk| (*pk).clone()).collect_vec();
                let rpc_client = self.rpc_client.clone();
                let this = self.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(multiple_accounts)) = tokio::time::timeout(
                        Duration::from_secs(30),
                        rpc_client.get_multiple_accounts_with_commitment(
                            &batch,
                            CommitmentConfig::processed(),
                        ),
                    )
                    .await
                    {
                        for (index, acc) in multiple_accounts.value.iter().enumerate() {
                            if let Some(acc) = acc {
                                this.save_account(batch[index], &acc.data);
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
        ALTS_IN_STORE.set(self.map.len() as i64);
    }

    async fn _load_all_alts(&self, alts_list: &Vec<Pubkey>) {
        let alts_list = alts_list
            .iter()
            .filter(|x| !self.map.contains_key(x))
            .collect_vec();
        if alts_list.is_empty() {
            return;
        }
        self.load_alts_list(alts_list).await;
    }

    pub async fn start_loading_missing_alts(&self, alts_list: &Vec<&Pubkey>) {
        for key in alts_list.iter().filter(|x| !self.map.contains_key(x)) {
            ALTS_IN_LOADING_QUEUE.inc();
            let _ = self.loading_queue.send(*key.clone()).await;
        }
    }

    pub fn save_account(&self, address: Pubkey, data: &[u8]) {
        let lookup_table = AddressLookupTable::deserialize(&data).unwrap();
        if self
            .map
            .insert(address, lookup_table.addresses.to_vec())
            .is_none()
        {
            ALTS_IN_STORE.inc();
        }
        drop(lookup_table);
    }

    async fn load_accounts<'a>(
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
            None => None,
        }
    }

    pub async fn get_accounts(
        &self,
        alt: &Pubkey,
        write_accounts: &Vec<u8>,
        read_account: &Vec<u8>,
    ) -> Vec<TransactionAccount> {
        match self.load_accounts(alt, write_accounts, read_account).await {
            Some(x) => x,
            None => {
                // forget alt for now, start loading it for next blocks
                // loading should be on its way
                vec![]
            }
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize::<BinaryALTData>(&BinaryALTData::new(self.map.clone())).unwrap()
    }

    pub fn load_binary(&self, binary_data: Vec<u8>) {
        let binary_alt_data = bincode::deserialize::<BinaryALTData>(&binary_data).unwrap();
        for (pubkey, accounts) in binary_alt_data.data {
            self.map.insert(pubkey, accounts);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BinaryALTData {
    pub data: Vec<(Pubkey, Vec<Pubkey>)>,
}

impl BinaryALTData {
    pub fn new(map: Arc<DashMap<Pubkey, Vec<Pubkey>>>) -> Self {
        let data = map
            .iter()
            .map(|x| (x.key().clone(), x.value().clone()))
            .collect_vec();
        Self { data }
    }
}
