use clap::Parser;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::time::Instant;

use crate::prometheus_sync::PrometheusSync;
use block_info::BlockInfo;
use cli::Args;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error};
use prometheus::{opts, register_int_counter, register_int_gauge, IntCounter, IntGauge};
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use transaction_info::TransactionInfo;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks
};

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

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let rpc_url = args.rpc_url;

    let _prometheus_jh = PrometheusSync::sync(args.prometheus_addr.clone());

    let grpc_addr = args.grpc_address;
    let mut client = GeyserGrpcClient::connect(grpc_addr, args.grpc_x_token, None).unwrap();
    let map_of_infos = Arc::new(DashMap::<String, TransactionInfo>::new());
    let slot_by_errors = Arc::new(DashMap::<u64, u64>::new());

    let postgres = postgres::Postgres::new().await;
    let slot = Arc::new(AtomicU64::new(0));

    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );
    let commitment_level = CommitmentLevel::Processed;

    let mut geyser_stream = client
        .subscribe_once(
            HashMap::new(),
            Default::default(),
            HashMap::new(),
            Default::default(),
            blocks_subs,
            Default::default(),
            Some(commitment_level),
            Default::default(),
            true,
        )
        .await
        .unwrap();

    postgres.spawn_transaction_infos_saver(map_of_infos.clone(), slot.clone());

    // get blocks from rpc server
    // because validator does not send banking blocks
    let (rpc_blocks_sender, rpc_blocks_reciever) =
        tokio::sync::mpsc::unbounded_channel::<(Instant, u64)>();
    let jh2 = {
        let map_of_infos = map_of_infos.clone();
        let postgres = postgres.clone();
        let slot_by_errors = slot_by_errors.clone();
        tokio::spawn(async move {
            let mut rpc_blocks_reciever = rpc_blocks_reciever;
            let rpc_client = RpcClient::new(rpc_url);
            while let Some((wait_until, slot)) = rpc_blocks_reciever.recv().await {
                tokio::time::sleep_until(wait_until).await;
                let block = if let Ok(block) = rpc_client
                    .get_block_with_config(
                        slot,
                        solana_rpc_client_api::config::RpcBlockConfig {
                            encoding: Some(
                                solana_transaction_status::UiTransactionEncoding::Base64,
                            ),
                            transaction_details: Some(
                                solana_transaction_status::TransactionDetails::Full,
                            ),
                            rewards: Some(true),
                            commitment: Some(CommitmentConfig::confirmed()),
                            max_supported_transaction_version: Some(0),
                        },
                    )
                    .await
                {
                    block
                } else {
                    continue;
                };

                let Some(transactions) = &block.transactions else {
                    continue;
                };

                for transaction in transactions {
                    let Some(transaction) = &transaction.transaction.decode() else {
                        continue;
                    };
                    let signature = transaction.signatures[0].to_string();
                    if let Some(mut info) = map_of_infos.get_mut(&signature) {
                        info.add_rpc_transaction(slot, transaction);
                    }
                }

                let banking_stage_error_count = slot_by_errors
                    .get(&slot)
                    .map(|x| *x.value() as i64)
                    .unwrap_or_default();
                let block_info =
                    BlockInfo::new_from_rpc_block(slot, &block, banking_stage_error_count);
                if let Some(block_info) = block_info {
                    BANKING_STAGE_ERROR_COUNT.add(banking_stage_error_count);
                    TXERROR_COUNT.add(
                        block_info.processed_transactions - block_info.successful_transactions,
                    );
                    if let Err(e) = postgres.save_block_info(block_info).await {
                        error!("Error saving block {}", e);
                    }
                    slot_by_errors.remove(&slot);
                }
            }
        })
    };

    while let Some(message) = geyser_stream.next().await {
        let Ok(message) = message else {
            continue;
        };

        let Some(update) = message.update_oneof else {
            continue;
        };

        match update {
            UpdateOneof::BankingTransactionErrors(transaction) => {
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
                        rpc_blocks_sender
                            .send((Instant::now() + Duration::from_secs(30), transaction.slot))
                            .expect("should works");
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
            UpdateOneof::Block(block) => {
                debug!("got block {}", block.slot);
                BLOCK_TXS.set(block.transactions.len() as i64);
                BANKING_STAGE_BLOCKS_COUNTER.inc();
                BANKING_STAGE_BLOCKS_TASK.inc();
                let postgres = postgres.clone();
                let slot = slot.clone();
                let map_of_infos = map_of_infos.clone();
                let slot_by_error = slot_by_errors.clone();
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
    
                    let block_info = BlockInfo::new(&block);
    
                    TXERROR_COUNT
                        .add(block_info.processed_transactions - block_info.successful_transactions);
                    if let Err(e) = postgres.save_block_info(block_info).await {
                        error!("Error saving block {}", e);
                    }
                    slot.store(block.slot, std::sync::atomic::Ordering::Relaxed);
                    slot_by_error.remove(&block.slot);
                    BANKING_STAGE_BLOCKS_TASK.dec();
                });
                // delay queue so that we get all the banking stage errors before processing block
            }
            _ => {}
        };
    }
    jh2.await.unwrap();
}
