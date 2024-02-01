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
use log::info;
use solana_sdk::signature::Signature;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};
use grpc_banking_transactions_notifications::postgres;

use grpc_banking_transactions_notifications::postgres::{AccountsForTransaction, AccountUsed, PostgresSession};




#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    pub tx_count: usize,
}

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args { tx_count } = Args::parse();

    let postgres_session = PostgresSession::new(0).await.unwrap();


    let amt_data = (0..tx_count).map(|i| {
        let tx_sig = Signature::new_unique();

        let accounts = (1..10).map(|j| {
            let account_key = Pubkey::new_unique();
            AccountUsed::new(account_key.to_string(), false, false, false)
        }).collect_vec();

        AccountsForTransaction {
            signature: tx_sig.to_string(),
            accounts,
        }
    }).collect_vec();


    let batch_size = amt_data.len();

    let started_at = Instant::now();
    postgres_session.create_transaction_ids(amt_data.iter().map(|x| x.signature.clone()).collect()).await.unwrap();
    postgres_session.create_accounts_for_transaction(amt_data.iter().flat_map(|x| x.accounts.iter().map(|au| au.pubkey())).collect()).await.unwrap();
    info!("inserted lookup data in {:?}", started_at.elapsed());

    postgres_session.insert_accounts_for_transaction(amt_data).await.unwrap();
    info!("inserted {} accounts_for_transaction in {:?}", batch_size, started_at.elapsed());
    //  inserted 4 accounts_for_transaction in 18.47626575s

    drop(postgres_session);
    Ok(())
}
