use std::{sync::{Arc, atomic::AtomicU64}, time::Duration};

use anyhow::Context;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use itertools::Itertools;
use tokio_postgres::{Client, NoTls, tls::MakeTlsConnect, Socket, types::ToSql};

use crate::{transaction_info::TransactionInfo, block_info::BlockInfo};


pub struct PostgresSession {
    client: Client,
}

impl PostgresSession {
    pub async fn new() -> anyhow::Result<Self> {
        let pg_config = std::env::var("PG_CONFIG").context("env PG_CONFIG not found")?;
        let pg_config = pg_config.parse::<tokio_postgres::Config>()?;

        let client =
            Self::spawn_connection(pg_config, NoTls).await?;

        Ok(Self { client })
    }

    async fn spawn_connection<T>(
        pg_config: tokio_postgres::Config,
        connector: T,
    ) -> anyhow::Result<Client>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send,
    {
        let (client, connection) = pg_config
            .connect(connector)
            .await
            .context("Connecting to Postgres failed")?;

        tokio::spawn(async move {
            log::info!("Connecting to Postgres");

            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
                return;
            }
            unreachable!("Postgres thread returned")
        });

        Ok(client)
    }

    pub fn multiline_query(query: &mut String, args: usize, rows: usize, types: &[&str]) {
        let mut arg_index = 1usize;
        for row in 0..rows {
            query.push('(');

            for i in 0..args {
                if row == 0 && !types.is_empty() {
                    query.push_str(&format!("(${arg_index})::{}", types[i]));
                } else {
                    query.push_str(&format!("${arg_index}"));
                }
                arg_index += 1;
                if i != (args - 1) {
                    query.push(',');
                }
            }

            query.push(')');

            if row != (rows - 1) {
                query.push(',');
            }
        }
    }

    pub async fn save_banking_transaction_results(&self, txs: &[TransactionInfo]) -> anyhow::Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        const NUMBER_OF_ARGS : usize = 10;

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());
        let txs: Vec<PostgresTransactionInfo> = txs.iter().map(|x| PostgresTransactionInfo::from(x)).collect();
        for tx in txs.iter() {
            args.push(&tx.signature);
            args.push(&tx.transaction_message);
            args.push(&tx.errors);
            args.push(&tx.is_executed);
            args.push(&tx.is_confirmed);
            args.push(&tx.first_notification_slot);
            args.push(&tx.cu_requested);
            args.push(&tx.prioritization_fees);
            args.push(&tx.utc_timestamp);
            args.push(&tx.accounts_used);
        }

        let mut query = String::from(
            r#"
                INSERT INTO banking_stage_results.transaction_infos 
                (signature, message, errors, is_executed, is_confirmed, first_notification_slot, cu_requested, prioritization_fees, utc_timestamp, accounts_used)
                VALUES
            "#,
        );

        Self::multiline_query(&mut query, NUMBER_OF_ARGS, txs.len(), &[]);
        self.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn save_block(&self, block_info: BlockInfo) -> anyhow::Result<()>  {
        const NUMBER_OF_ARGS : usize = 9; 
        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS);
        args.push(&block_info.block_hash);
        args.push(&block_info.slot);
        args.push(&block_info.leader_identity);
        args.push(&block_info.successful_transactions);
        args.push(&block_info.banking_stage_errors);
        args.push(&block_info.processed_transactions);
        args.push(&block_info.total_cu_used);
        args.push(&block_info.total_cu_requested);
        args.push(&block_info.heavily_writelocked_accounts);

        let mut query = String::from(
            r#"
                INSERT INTO banking_stage_results.blocks 
                (block_hash, slot, leader_identity, successful_transactions, banking_stage_errors, processed_transactions, total_cu_used, total_cu_requested, heavily_writelocked_accounts)
                VALUES
            "#,
        );

        Self::multiline_query(&mut query, NUMBER_OF_ARGS, 1, &[]);
        self.client.execute(&query, &args).await?;

        Ok(())
    }
}

pub struct Postgres {
    session: Arc<PostgresSession>,
}

impl Postgres {
    pub async fn new() -> Self {
        let session = PostgresSession::new().await.unwrap();
        Self { session: Arc::new(session) }
    }

    pub fn start_saving_transaction(&self, map_of_transaction : Arc<DashMap<String, TransactionInfo>>, slots : Arc<AtomicU64>) {
        let session = self.session.clone();
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let slot = slots.load(std::sync::atomic::Ordering::Relaxed);
                let mut txs_to_store = vec![];
                for tx in map_of_transaction.iter() {
                    if slot > tx.first_notification_slot + 300 {
                        txs_to_store.push(tx.clone());
                    }
                }

                if !txs_to_store.is_empty() {
                    println!("saving {}", txs_to_store.len());
                    for tx in &txs_to_store {
                        map_of_transaction.remove(&tx.signature);
                    }
                    let batches = txs_to_store.chunks(8).collect_vec();
                    for batch in batches {
                        session.save_banking_transaction_results(batch).await.unwrap();
                    }
                }
            }
        });
    }

    pub async fn save_block_info(&self, block: BlockInfo) -> anyhow::Result<()> {
        self.session.save_block(block).await
    }
}

pub struct PostgresTransactionInfo {
    pub signature: String,
    pub transaction_message: Option<String>,
    pub errors: String,
    pub is_executed: bool,
    pub is_confirmed: bool,
    pub first_notification_slot: i64,
    pub cu_requested: Option<i64>,
    pub prioritization_fees: Option<i64>,
    pub utc_timestamp: DateTime<Utc>,
    pub accounts_used: Vec<String>,
}

impl From<&TransactionInfo> for PostgresTransactionInfo {
    fn from(value: &TransactionInfo) -> Self {
        let errors = value.errors.iter().fold(String::new(), |is, x| {
            let str = is + x.0.to_string().as_str() + ":" + x.1.to_string().as_str() + ";";
            str
        });
        let accounts_used = value.account_used.iter().map(|x| format!("{}({})", x.0, x.1).to_string()).collect();
        Self {
            signature: value.signature.clone(),
            transaction_message: value.transaction_message.as_ref().map(|x| base64::encode(bincode::serialize(&x).unwrap())),
            errors,
            is_executed: value.is_executed,
            is_confirmed: value.is_confirmed,
            cu_requested: value.cu_requested.map(|x| x as i64),
            first_notification_slot: value.first_notification_slot as i64,
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            utc_timestamp: value.utc_timestamp,
            accounts_used,
        }
    }
}