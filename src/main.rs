use clap::Parser;
use itertools::Itertools;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use crate::prometheus_sync::PrometheusSync;
use block_info::BlockInfo;
use cli::Args;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error};
use prometheus::{opts, register_int_counter, register_int_gauge, IntCounter, IntGauge};
use solana_sdk::signature::Signature;
use transaction_info::TransactionInfo;

mod block_info;
mod cli;
mod postgres;
mod prometheus_sync;
mod transaction_info;

lazy_static::lazy_static! {
     static ref BLOCK_TXS: IntGauge =
        register_int_gauge!(opts!("block_arrived", "block seen with n transactions")).unwrap();
    static ref BANKING_STAGE_ERROR_COUNT: IntGauge =
        register_int_gauge!(opts!("bankingstage_banking_errors", "banking_stage errors in block")).unwrap();
    static ref TXERROR_COUNT: IntGauge =
        register_int_gauge!(opts!("bankingstage_txerrors", "transaction errors in block")).unwrap();
    static ref BANKING_STAGE_ERROR_EVENT_COUNT: IntCounter =
        register_int_counter!(opts!("bankingstage_banking_stage_events_counter", "Banking stage events received")).unwrap();
    static ref BANKING_STAGE_BLOCKS_COUNTER: IntCounter =
        register_int_counter!(opts!("bankingstage_blocks_counter", "Banking stage blocks received")).unwrap();
    static ref BANKING_STAGE_BLOCKS_TASK: IntGauge =
        register_int_gauge!(opts!("bankingstage_blocks_in_queue", "Banking stage blocks in queue")).unwrap();
    static ref BANKING_STAGE_BLOCKS_IN_RPC_QUEUE: IntGauge =
        register_int_gauge!(opts!("bankingstage_blocks_in_rpc_queue", "Banking stage blocks in rpc queue")).unwrap();
}

pub async fn start_tracking_banking_stage_errors(
    grpc_address: String,
    map_of_infos: Arc<DashMap<(String, u64), TransactionInfo>>,
    slot_by_errors: Arc<DashMap<u64, u64>>,
    slot: Arc<AtomicU64>,
    subscribe_to_slots: bool,
) {
    loop {
        let token: Option<String> = None;
        let mut client = yellowstone_grpc_client::GeyserGrpcClient::connect(
            grpc_address.clone(),
            token.clone(),
            None,
        )
        .unwrap();

        let slot_subscription: HashMap<
            String,
            yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots,
        > = if subscribe_to_slots {
            let mut slot_sub = HashMap::new();
            slot_sub.insert(
                "slot_sub".to_string(),
                yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots {},
            );
            slot_sub
        } else {
            HashMap::new()
        };

        let mut geyser_stream = client
            .subscribe_once(
                slot_subscription,
                Default::default(),
                HashMap::new(),
                Default::default(),
                Default::default(),
                Default::default(),
                Some(yellowstone_grpc_proto::prelude::CommitmentLevel::Processed),
                Default::default(),
                true,
            )
            .await
            .unwrap();
        log::info!("started geyser banking stage subscription");
        while let Some(message) = geyser_stream.next().await {
            let Ok(message) = message else {
            continue;
            };

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update{
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BankingTransactionErrors(transaction) => {
                    if transaction.error.is_none() {
                        continue;
                    }
                    BANKING_STAGE_ERROR_EVENT_COUNT.inc();
                    let sig = transaction.signature.to_string();
                    match slot_by_errors.get_mut(&transaction.slot) {
                        Some(mut value) => {
                            *value += 1;
                        }
                        None => {
                            slot_by_errors.insert(transaction.slot, 1);
                        }
                    }
                    match map_of_infos.get_mut(&(sig.clone(), transaction.slot)) {
                        Some(mut x) => {
                            x.add_notification(&transaction);
                        }
                        None => {
                            let mut x = TransactionInfo::new(&transaction);
                            x.add_notification(&transaction);
                            map_of_infos.insert((sig.clone(), transaction.slot), x);
                        }
                    }
                },
                yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Slot(s) => {
                    let load_slot = slot.load(std::sync::atomic::Ordering::Relaxed);
                    if load_slot < s.slot {
                        // update slot to process updates
                        slot.store(s.slot, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                _=>{}
            }
        }
        error!("geyser banking stage connection failed {}", grpc_address);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn start_tracking_blocks(
    grpc_block_addr: String,
    grpc_x_token: Option<String>,
    postgres: postgres::Postgres,
    slot: Arc<AtomicU64>,
    slot_by_errors: Arc<DashMap<u64, u64>>,
    map_of_infos: Arc<DashMap<(String, u64), TransactionInfo>>,
) {
    let mut client = yellowstone_grpc_client_original::GeyserGrpcClient::connect(
        grpc_block_addr,
        grpc_x_token,
        None,
    )
    .unwrap();

    loop {
        let mut blocks_subs = HashMap::new();
        blocks_subs.insert(
            "sidecar_block_subscription".to_string(),
            yellowstone_grpc_proto_original::prelude::SubscribeRequestFilterBlocks {
                account_include: Default::default(),
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        );

        let mut slot_sub = HashMap::new();
        slot_sub.insert(
            "slot_sub".to_string(),
            yellowstone_grpc_proto_original::prelude::SubscribeRequestFilterSlots {
                filter_by_commitment: None,
            },
        );

        let mut geyser_stream = client
            .subscribe_once(
                slot_sub,
                Default::default(),
                HashMap::new(),
                Default::default(),
                blocks_subs,
                Default::default(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        while let Some(message) = geyser_stream.next().await {
            let Ok(message) = message else {
                    continue;
                };

            let Some(update) = message.update_oneof else {
                    continue;
                };

            match update {
                yellowstone_grpc_proto_original::prelude::subscribe_update::UpdateOneof::Block(
                    block,
                ) => {
                    debug!("got block {}", block.slot);
                    BLOCK_TXS.set(block.transactions.len() as i64);
                    BANKING_STAGE_BLOCKS_COUNTER.inc();
                    BANKING_STAGE_BLOCKS_TASK.inc();
                    let postgres = postgres.clone();
                    let slot = slot.clone();
                    let map_of_infos = map_of_infos.clone();
                    let slot_by_errors = slot_by_errors.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        for transaction in &block.transactions {
                            let Some(tx) = &transaction.transaction else {
                                    continue;
                                };
                            let signature = Signature::try_from(tx.signatures[0].clone()).unwrap();
                            for mut tx_data in map_of_infos.iter_mut() {
                                let  (sig, _) = tx_data.key();
                                if *sig == signature.to_string() {
                                    tx_data.value_mut().add_transaction(transaction, block.slot);
                                }
                            }
                        }

                        let banking_stage_error_count =
                            slot_by_errors.get(&block.slot).map(|x| *x.value() as i64);
                        let block_info = BlockInfo::new(&block, banking_stage_error_count);

                        TXERROR_COUNT.add(
                            block_info.processed_transactions - block_info.successful_transactions,
                        );
                        if let Err(e) = postgres.save_block_info(block_info).await {
                            error!("Error saving block {}", e);
                        }
                        slot.store(block.slot, std::sync::atomic::Ordering::Relaxed);
                        slot_by_errors.remove(&block.slot);
                        BANKING_STAGE_BLOCKS_TASK.dec();
                    });
                    // delay queue so that we get all the banking stage errors before processing block
                }
                _ => {}
            };
        }
        log::error!("stopping the sidecar, geyser block stream is broken");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let _prometheus_jh = PrometheusSync::sync(args.prometheus_addr.clone());

    let grpc_block_addr = args.grpc_address_to_fetch_blocks;
    let map_of_infos = Arc::new(DashMap::<(String, u64), TransactionInfo>::new());
    let slot_by_errors = Arc::new(DashMap::<u64, u64>::new());

    let postgres = postgres::Postgres::new().await;
    let slot = Arc::new(AtomicU64::new(0));
    let no_block_subscription = grpc_block_addr.is_none();
    postgres.spawn_transaction_infos_saver(map_of_infos.clone(), slot.clone());
    let jhs = args
        .banking_grpc_addresses
        .iter()
        .map(|address| {
            let address = address.clone();
            let map_of_infos = map_of_infos.clone();
            let slot_by_errors = slot_by_errors.clone();
            let slot = slot.clone();
            tokio::spawn(async move {
                start_tracking_banking_stage_errors(
                    address,
                    map_of_infos,
                    slot_by_errors,
                    slot,
                    no_block_subscription,
                )
                .await;
            })
        })
        .collect_vec();
    if let Some(gprc_block_addr) = grpc_block_addr {
        start_tracking_blocks(
            gprc_block_addr,
            args.grpc_x_token,
            postgres,
            slot,
            slot_by_errors,
            map_of_infos,
        )
        .await;
    }
    futures::future::join_all(jhs).await;
}
