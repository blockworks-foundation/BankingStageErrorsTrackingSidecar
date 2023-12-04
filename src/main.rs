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

// fn spawn_rpc_block_processor() -> tokio::task::JoinHandle<()> {
//     let map_of_infos = map_of_infos.clone();
//         let postgres = postgres.clone();
//         let slot_by_errors = slot_by_errors.clone();
//         tokio::spawn(async move {
//             let mut rpc_blocks_reciever = rpc_blocks_reciever;
//             let rpc_client = RpcClient::new(rpc_url);
//             while let Some((wait_until, slot)) = rpc_blocks_reciever.recv().await {
//                 tokio::time::sleep_until(wait_until).await;
//                 let block = if let Ok(block) = rpc_client
//                     .get_block_with_config(
//                         slot,
//                         solana_rpc_client_api::config::RpcBlockConfig {
//                             encoding: Some(
//                                 solana_transaction_status::UiTransactionEncoding::Base64,
//                             ),
//                             transaction_details: Some(
//                                 solana_transaction_status::TransactionDetails::Full,
//                             ),
//                             rewards: Some(true),
//                             commitment: Some(CommitmentConfig::confirmed()),
//                             max_supported_transaction_version: Some(0),
//                         },
//                     )
//                     .await
//                 {
//                     block
//                 } else {
//                     continue;
//                 };

//                 let Some(transactions) = &block.transactions else {
//                     continue;
//                 };

//                 for transaction in transactions {
//                     let Some(transaction) = &transaction.transaction.decode() else {
//                         continue;
//                     };
//                     let signature = transaction.signatures[0].to_string();
//                     if let Some(mut info) = map_of_infos.get_mut(&signature) {
//                         info.add_rpc_transaction(slot, transaction);
//                     }
//                 }

//                 let banking_stage_error_count = slot_by_errors
//                     .get(&slot)
//                     .map(|x| *x.value() as i64)
//                     .unwrap_or_default();
//                 let block_info =
//                     BlockInfo::new_from_rpc_block(slot, &block, banking_stage_error_count);
//                 if let Some(block_info) = block_info {
//                     BANKING_STAGE_ERROR_COUNT.add(banking_stage_error_count);
//                     TXERROR_COUNT.add(
//                         block_info.processed_transactions - block_info.successful_transactions,
//                     );
//                     if let Err(e) = postgres.save_block_info(block_info).await {
//                         error!("Error saving block {}", e);
//                     }
//                     slot_by_errors.remove(&slot);
//                 }
//             }
//         })
// }

pub async fn start_tracking_banking_stage_errors(
    grpc_address: String,
    map_of_infos: Arc<DashMap<String, TransactionInfo>>,
    slot_by_errors: Arc<DashMap<u64, u64>>,
) {
    loop {
        let token: Option<String> = None;
        let mut client = yellowstone_grpc_client::GeyserGrpcClient::connect(
            grpc_address.clone(),
            token.clone(),
            None,
        )
        .unwrap();

        let mut geyser_stream = client
            .subscribe_once(
                HashMap::new(),
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

            if let yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BankingTransactionErrors(transaction) = update {
                    if transaction.error.is_none() && transaction.accounts.is_empty() {
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
                    match map_of_infos.get_mut(&sig) {
                        Some(mut x) => {
                            x.add_notification(&transaction);
                        }
                        None => {
                            let mut x = TransactionInfo::new(&transaction);
                            x.add_notification(&transaction);
                            map_of_infos.insert(sig, x);
                        }
                    }
                }
        }
        error!("geyser banking stage connection failed {}", grpc_address);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let _prometheus_jh = PrometheusSync::sync(args.prometheus_addr.clone());

    let grpc_block_addr = args.grpc_address_to_fetch_blocks;
    let mut client = yellowstone_grpc_client_original::GeyserGrpcClient::connect(
        grpc_block_addr,
        args.grpc_x_token,
        None,
    )
    .unwrap();
    let map_of_infos = Arc::new(DashMap::<String, TransactionInfo>::new());
    let slot_by_errors = Arc::new(DashMap::<u64, u64>::new());

    let postgres = postgres::Postgres::new().await;
    let slot = Arc::new(AtomicU64::new(0));

    postgres.spawn_transaction_infos_saver(map_of_infos.clone(), slot.clone());
    let _jhs = args
        .banking_grpc_addresses
        .iter()
        .map(|address| {
            let address = address.clone();
            let map_of_infos = map_of_infos.clone();
            let slot_by_errors = slot_by_errors.clone();
            tokio::spawn(async move {
                start_tracking_banking_stage_errors(address, map_of_infos, slot_by_errors).await;
            })
        })
        .collect_vec();

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
                            if let Some(mut info) = map_of_infos.get_mut(&signature.to_string()) {
                                info.add_transaction(transaction, block.slot);
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
