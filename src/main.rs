use clap::Parser;
use tokio::time::Instant;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc}, time::Duration,
};

use block_info::BlockInfo;
use cli::Args;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error, info};
use prometheus::{IntCounter, IntGauge, opts, register_int_counter, register_int_gauge};
use solana_sdk::signature::Signature;
use transaction_info::TransactionInfo;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeUpdateBlock,
};
use crate::prometheus_sync::PrometheusSync;

mod block_info;
mod cli;
mod postgres;
mod transaction_info;
mod prometheus_sync;

lazy_static::lazy_static! {
     static ref BLOCK_TXS: IntGauge =
        register_int_gauge!(opts!("block_arrived", "block seen with n transactions")).unwrap();
    static ref BANKING_STAGE_ERROR_COUNT: IntGauge =
        register_int_gauge!(opts!("bankingstage_banking_errors", "banking_stage errors in block")).unwrap();
    static ref TXERROR_COUNT: IntGauge =
        register_int_gauge!(opts!("bankingstage_txerrors", "transaction errors in block")).unwrap();
}

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let _prometheus_jh = PrometheusSync::sync(args.prometheus_addr.clone());

    let grpc_addr = args.grpc_address;
    let mut client = GeyserGrpcClient::connect(grpc_addr, None::<&'static str>, None).unwrap();
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

    let (send_block, mut recv_block) = tokio::sync::mpsc::unbounded_channel::<(Instant, SubscribeUpdateBlock)>();
    let slot_by_error_task = slot_by_errors.clone();
    let map_of_infos_task = map_of_infos.clone();

    // process blocks with 2 mins delay so that we process all the banking stage errors before processing blocks
    tokio::spawn(async move {
        while let Some((wait_until, block)) = recv_block.recv().await {
            if wait_until > Instant::now() + Duration::from_secs(5) {
                info!("wait until {:?} to collect errors for block {}", wait_until, block.slot);
            }
            tokio::time::sleep_until(wait_until).await;
            for transaction in &block.transactions {
                let Some(tx) = &transaction.transaction else {
                    continue;
                };
                let signature = Signature::try_from(tx.signatures[0].clone()).unwrap();
                if let Some(mut info) = map_of_infos_task.get_mut(&signature.to_string()) {
                    info.add_transaction(&transaction, block.slot);
                }
            }

            let block_info = BlockInfo::new(&block, &slot_by_error_task);
            BANKING_STAGE_ERROR_COUNT.set(block_info.banking_stage_errors);
            TXERROR_COUNT.set(block_info.processed_transactions - block_info.successful_transactions);
            if let Err(e) = postgres.save_block_info(block_info).await {
                error!("Error saving block {}", e);
            }
            info!("saved block {}", block.slot);
        }
    });


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
                info!("got banking stage transaction errors");
                let sig = transaction.signature.to_string();
                match slot_by_errors.get_mut(&transaction.slot) {
                    Some(mut value) => {
                        *value = *value + 1;
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
            UpdateOneof::Block(block) => {
                debug!("got block {}", block.slot);
                BLOCK_TXS.set(block.transactions.len() as i64);
                slot.store(block.slot, std::sync::atomic::Ordering::Relaxed);
                send_block.send(( Instant::now() + Duration::from_secs(30), block)).expect("should works");
                // delay queue so that we get all the banking stage errors before processing block
            }
            _ => {}
        };
    }
}
