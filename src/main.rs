use clap::Parser;
use itertools::Itertools;
use solana_account_decoder::UiDataSliceConfig;
use solana_rpc_client::nonblocking::rpc_client::{self, RpcClient};
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};

use crate::prometheus_sync::PrometheusSync;
use block_info::BlockInfo;
use cli::Args;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, error, info};
use prometheus::{opts, register_int_counter, register_int_gauge, IntCounter, IntGauge};
use tokio::sync::mpsc::Sender;
use yellowstone_grpc_client_original::GeyserGrpcClientBufferConfig;
use transaction_info::TransactionInfo;

mod alt_store;
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
    error_plugin_write_version: Arc<AtomicU64>,
    slot: Arc<AtomicU64>,
    _subscribe_to_slots: bool,
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
        > = {
            log::info!("subscribing to slots on grpc banking errors");
            let mut slot_sub = HashMap::new();
            slot_sub.insert(
                "slot_sub_for_banking_tx".to_string(),
                yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots {},
            );
            slot_sub
        };

        let mut geyser_stream = client
            .subscribe_once(
                slot_subscription,
                Default::default(),
                HashMap::new(),
                Default::default(),
                Default::default(),
                Default::default(),
                Some(yellowstone_grpc_proto::prelude::CommitmentLevel::Confirmed),
                Default::default(),
                true,
            )
            .await
            .unwrap();

        let mut instance = Instant::now();
        log::info!("started geyser banking stage subscription");
        while let Ok(Some(message)) =
            tokio::time::timeout(Duration::from_secs(30), geyser_stream.next()).await
        {
            if instance.elapsed() > Duration::from_secs(600) {
                // reestablish geyser connection
                break;
            }
            let Ok(message) = message else {
                continue;
            };

            let Some(update) = message.update_oneof else {
                continue;
            };
            log::trace!("got banking stage notification");

            match update{
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::BankingTransactionErrors(transaction) => {
                    // if transaction.error.is_none() {
                    //     continue;
                    // }
                    BANKING_STAGE_ERROR_EVENT_COUNT.inc();
                    instance = Instant::now();

                    let sig = transaction.signature.to_string();
                    match map_of_infos.get_mut(&(sig.clone(), transaction.slot)) {
                        Some(mut x) => {
                            let tx_info = x.value_mut();
                            tx_info.add_notification(&transaction);
                        }
                        None => {
                            // map_of_infos might get populated by parallel writers if multiple geyser sources are configured
                            let write_version = error_plugin_write_version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let tx_info = TransactionInfo::new(&transaction, write_version);
                            map_of_infos.insert((sig, transaction.slot), tx_info);
                        }
                    }
                },
                yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Slot(s) => {
                    let load_slot = slot.load(std::sync::atomic::Ordering::Relaxed);
                    if load_slot < s.slot {
                        // update slot to process updates
                        // updated slot
                        slot.store(s.slot, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                _=>{}
            }
        }
        error!("geyser banking stage connection failed {}", grpc_address);
    }
}

async fn start_tracking_blocks(
    rpc_client: Arc<RpcClient>,
    grpc_block_addr: String,
    grpc_x_token: Option<String>,
    block_sender_postgres: Vec<Sender<BlockInfo>>,
    slot: Arc<AtomicU64>,
    alts_list: Vec<Pubkey>,
) {
    let block_counter = Arc::new(AtomicU64::new(0));
    let restart_block_subscription = Arc::new(AtomicBool::new(false));
    let _block_counter_checker = {
        let block_counter = block_counter.clone();
        let restart_block_subscription = restart_block_subscription.clone();
        tokio::spawn(async move {
            let mut old_count = block_counter.load(std::sync::atomic::Ordering::Relaxed);
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                let new_count = block_counter.load(std::sync::atomic::Ordering::Relaxed);
                if old_count > 0 && old_count == new_count {
                    log::error!(
                        "Did not recieve any block for 20 s, restarting block subscription"
                    );
                    restart_block_subscription.store(true, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
                old_count = new_count;
            }
        })
    };

    let buffer_config_large = GeyserGrpcClientBufferConfig {
        buffer_size: Some(65536),  // 64kb (default: 1k)
        conn_window: Some(5242880), // 5mb (=default)
        stream_window: Some(4194304), // 4mb (default: 2m)
    };
    // see https://github.com/blockworks-foundation/geyser-grpc-connector/issues/10
    let mut client = yellowstone_grpc_client_original::GeyserGrpcClient::connect_with_timeout_with_buffers(
        grpc_block_addr,
        grpc_x_token,
        None,
        None,
        None,
        buffer_config_large
    )
    .unwrap();

    // Load all ALTs stores.
    // let alts_list = rpc_client
    //     .get_program_accounts_with_config(
    //         &solana_address_lookup_table_program::id(),
    //         RpcProgramAccountsConfig {
    //             filters: None,
    //             account_config: RpcAccountInfoConfig {
    //                 encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
    //                 data_slice: Some(UiDataSliceConfig {
    //                     offset: 0,
    //                     length: 0,
    //                 }),
    //                 commitment: None,
    //                 min_context_slot: None,
    //             },
    //             with_context: None,
    //         },
    //     )
    //     .await
    //     .unwrap()
    //     .iter()
    //     .map(|(pubkey, _)| pubkey.clone())
    //     .collect_vec();

    // ALT store from binary
    // let atl_store = {
    //     let alt_store = Arc::new(alt_store::ALTStore::new(rpc_client));
    //     let mut alts_file = tokio::fs::File::open(alts_list).await.unwrap();
    //     let mut buf = vec![];
    //     alts_file.read_to_end(&mut buf).await.unwrap();
    //     alt_store.load_binary(buf);
    //     alt_store
    // };

    let atl_store = Arc::new(alt_store::ALTStore::new(rpc_client));
    atl_store.load_alts_list(&alts_list).await;

    // let data = atl_store.serialize();
    // let mut alts_file = tokio::fs::File::create("alt_binary.bin").await.unwrap();
    // alts_file.write_all(&data).await.unwrap();
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

        let mut accounts_subs = HashMap::new();
        accounts_subs.insert(
            "sidecar_atl_accounts_subscription".to_string(),
            yellowstone_grpc_proto_original::prelude::SubscribeRequestFilterAccounts {
                account: vec![],
                filters: vec![],
                owner: vec![solana_address_lookup_table_program::id().to_string()],
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
        while let Ok(Some(message)) =
            tokio::time::timeout(Duration::from_secs(30), geyser_stream.next()).await
        {
            if restart_block_subscription.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

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

                    let count = block_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;
                    let pg_size = block_sender_postgres.len();
                    let block_sender = block_sender_postgres[count%pg_size].clone();
                    let slot = slot.clone();
                    let atl_store = atl_store.clone();

                    tokio::spawn(async move {
                        // to support address lookup tables delay processing a littlebit
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        let block_info = BlockInfo::new(atl_store, &block).await;
                        TXERROR_COUNT.add(
                            block_info.processed_transactions - block_info.successful_transactions,
                        );
                        if let Err(e) = postgres::send_block_info_to_buffer(block_sender, block_info).await {
                            panic!("Error saving block {}", e);
                        }
                        slot.store(block.slot, std::sync::atomic::Ordering::Relaxed);
                        BANKING_STAGE_BLOCKS_TASK.dec();
                    });
                    // delay queue so that we get all the banking stage errors before processing block
                },
                yellowstone_grpc_proto_original::prelude::subscribe_update::UpdateOneof::Account(account_update) => {
                    info!("ATL updated");
                    if let Some(account) = account_update.account {
                        let bytes: [u8; 32] = account.pubkey.try_into().unwrap_or(Pubkey::default().to_bytes());
                        let pubkey = Pubkey::new_from_array(bytes);
                        atl_store.save_account(&pubkey, &account.data);
                    }
                },
                _ => {}
            };
        }
        restart_block_subscription.store(false, std::sync::atomic::Ordering::Relaxed);
        log::error!("geyser block stream is broken, retrying");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let rpc_client = Arc::new(rpc_client::RpcClient::new(args.rpc_url));

    let _prometheus_jh = PrometheusSync::sync(args.prometheus_addr.clone());

    let grpc_block_addr = args.grpc_address_to_fetch_blocks;
    let map_of_infos = Arc::new(DashMap::<(String, u64), TransactionInfo>::new());
    // maintain a global serial version for deterministic transaction ordering
    let error_plugin_write_version = Arc::new(AtomicU64::new(0));

    let postgres1 = postgres::Postgres::new_with_workmem(0).await;
    let slot = Arc::new(AtomicU64::new(0));
    let no_block_subscription = grpc_block_addr.is_none();
    let alts = args.alts;

    //load alts from the file
    let mut alts_string = String::new();
    let mut alts_file = tokio::fs::File::open(alts).await?;
    alts_file.read_to_string(&mut alts_string).await?;
    let alts_list = alts_string
        .split("\r\n")
        .map(|x| x.trim().to_string())
        .filter(|x| x.len() > 0)
        .map(|x| Pubkey::from_str(&x).unwrap())
        .collect_vec();

    let mut block_senders = vec![];
    for i in 1..=4 {
        let s = postgres::Postgres::new_with_workmem(i)
            .await
            .spawn_block_saver();
        block_senders.push(s);
    }

    postgres1.spawn_transaction_infos_saver(map_of_infos.clone(), slot.clone());

    let jhs = args
        .banking_grpc_addresses
        .iter()
        .map(|address| {
            let address = address.clone();
            let map_of_infos = map_of_infos.clone();
            let slot = slot.clone();
            let global_error_plugin_write_version = error_plugin_write_version.clone();
            tokio::spawn(async move {
                start_tracking_banking_stage_errors(
                    address,
                    map_of_infos,
                    global_error_plugin_write_version,
                    slot,
                    no_block_subscription,
                )
                .await;
            })
        })
        .collect_vec();
    if let Some(gprc_block_addr) = grpc_block_addr {
        start_tracking_blocks(
            rpc_client,
            gprc_block_addr,
            args.grpc_x_token,
            block_senders,
            slot,
            alts_list,
        )
        .await;
    }
    futures::future::join_all(jhs).await;
    Ok(())
}
