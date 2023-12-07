use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use anyhow::Context;
use base64::Engine;
use dashmap::DashMap;
use futures::pin_mut;
use itertools::Itertools;
use log::{debug, error, info};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use serde::Serialize;
use solana_sdk::transaction::TransactionError;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    config::SslMode,
    tls::MakeTlsConnect,
    types::{ToSql, Type},
    Client, CopyInSink, NoTls, Socket,
};

use crate::{
    block_info::{BlockInfo, BlockTransactionInfo},
    transaction_info::TransactionInfo,
};

pub struct TempTableTracker {
    count: AtomicU64,
}

impl TempTableTracker {
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(1),
        }
    }

    pub fn get_new_temp_table(&self) -> String {
        format!(
            "temp_table_{}",
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }
}

pub struct PostgresSession {
    client: Client,
    temp_table_tracker: TempTableTracker,
}

impl PostgresSession {
    pub async fn new() -> anyhow::Result<Self> {
        let pg_config = std::env::var("PG_CONFIG").context("env PG_CONFIG not found")?;
        let pg_config = pg_config.parse::<tokio_postgres::Config>()?;

        let client = if let SslMode::Disable = pg_config.get_ssl_mode() {
            Self::spawn_connection(pg_config, NoTls).await?
        } else {
            let ca_pem_b64 = std::env::var("CA_PEM_B64").context("env CA_PEM_B64 not found")?;
            let client_pks_b64 =
                std::env::var("CLIENT_PKS_B64").context("env CLIENT_PKS_B64 not found")?;
            let client_pks_password =
                std::env::var("CLIENT_PKS_PASS").context("env CLIENT_PKS_PASS not found")?;

            let ca_pem = base64::engine::general_purpose::STANDARD
                .decode(ca_pem_b64)
                .context("ca pem decode")?;
            let client_pks = base64::engine::general_purpose::STANDARD
                .decode(client_pks_b64)
                .context("client pks decode")?;

            let connector = TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_pem)?)
                .identity(
                    Identity::from_pkcs12(&client_pks, &client_pks_password).context("Identity")?,
                )
                .danger_accept_invalid_hostnames(true)
                .danger_accept_invalid_certs(true)
                .build()?;

            Self::spawn_connection(pg_config, MakeTlsConnector::new(connector)).await?
        };

        Ok(Self {
            client,
            temp_table_tracker: TempTableTracker::new(),
        })
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
            info!("Connecting to Postgres");

            if let Err(err) = connection.await {
                error!("Connection to Postgres broke {err:?}");
                // should restart the side car / currently no way around it
                panic!("Connection to Postgres broke {err:?}");
            }
            unreachable!("Postgres thread returned")
        });

        Ok(client)
    }

    pub async fn drop_temp_table(&self, table: String) -> anyhow::Result<()> {
        self.client
            .execute(format!("drop table if exists {};", table).as_str(), &[])
            .await?;
        Ok(())
    }

    pub async fn create_transaction_ids(&self, signatures: Vec<String>) -> anyhow::Result<()> {
        // create temp table
        let temp_table = self.temp_table_tracker.get_new_temp_table();

        self.client
            .execute(
                format!(
                    r#"
        CREATE TEMP TABLE {}(
            signature char(88)
        );
        "#,
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                signature
            ) FROM STDIN BINARY
        "#,
            temp_table
        );
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
        pin_mut!(writer);
        for signature in signatures {
            writer.as_mut().write(&[&signature]).await?;
        }
        writer.finish().await?;

        let statement = format!(
            r#"
        INSERT INTO banking_stage_results_2.transactions(signature) SELECT signature from {}
        ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn create_accounts_for_transaction(
        &self,
        accounts: Vec<String>,
    ) -> anyhow::Result<()> {
        // create temp table
        let temp_table = self.temp_table_tracker.get_new_temp_table();

        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            key char(44)
        );",
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                key
            ) FROM STDIN BINARY
        "#,
            temp_table
        );
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
        pin_mut!(writer);
        for account in accounts {
            writer.as_mut().write(&[&account]).await?;
        }
        writer.finish().await?;

        let statement = format!(
            r#"
        INSERT INTO banking_stage_results_2.accounts(account_key) SELECT key from {}
        ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;
        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn insert_transaction_in_txslot_table(
        &self,
        txs: &[TransactionInfo],
    ) -> anyhow::Result<()> {
        let temp_table = self.temp_table_tracker.get_new_temp_table();

        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            sig char(88),
            slot BIGINT,
            error_code INT,
            count INT,
            utc_timestamp TIMESTAMP
        );",
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                sig, slot, error_code, count, utc_timestamp
            ) FROM STDIN BINARY
        "#,
            temp_table
        );
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::TEXT,
                Type::INT8,
                Type::INT4,
                Type::INT4,
                Type::TIMESTAMP,
            ],
        );
        pin_mut!(writer);
        for tx in txs {
            let slot: i64 = tx.slot as i64;
            for (error, count) in &tx.errors {
                let error_code = error.to_int();
                let count = *count as i32;
                let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(5);
                let timestamp = tx.utc_timestamp.naive_local();
                args.push(&tx.signature);
                args.push(&slot);
                args.push(&error_code);
                args.push(&count);
                args.push(&timestamp);

                writer.as_mut().write(&args).await?;
            }
        }
        writer.finish().await?;

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.transaction_slot(transaction_id, slot, error_code, count, utc_timestamp)
                SELECT ( select transaction_id from banking_stage_results_2.transactions where signature = t.sig ), t.slot, t.error_code, t.count, t.utc_timestamp
                FROM (
                    SELECT sig, slot, error_code, count, utc_timestamp from {}
                )
                as t (sig, slot, error_code, count, utc_timestamp) ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;
        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn insert_accounts_for_transaction(
        &self,
        accounts_for_transaction: Vec<AccountsForTransaction>,
    ) -> anyhow::Result<()> {
        let temp_table = self.temp_table_tracker.get_new_temp_table();
        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            account_key char(44),
            signature char(88),
            is_writable BOOL,
            is_signer BOOL
        );",
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                account_key, signature, is_writable, is_signer
            ) FROM STDIN BINARY
        "#,
            temp_table
        );

        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer =
            BinaryCopyInWriter::new(sink, &[Type::TEXT, Type::TEXT, Type::BOOL, Type::BOOL]);
        pin_mut!(writer);
        for acc_tx in accounts_for_transaction {
            for acc in &acc_tx.accounts {
                let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(4);
                args.push(&acc.key);
                args.push(&acc_tx.signature);
                args.push(&acc.writable);
                args.push(&acc.is_signer);
                writer.as_mut().write(&args).await?;
            }
        }
        writer.finish().await?;

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.accounts_map_transaction(acc_id, transaction_id, is_writable, is_signer)
                SELECT 
                    ( select acc_id from banking_stage_results_2.accounts where account_key = t.account_key ),
                    ( select transaction_id from banking_stage_results_2.transactions where signature = t.signature ),
                    t.is_writable,
                    t.is_signer
                FROM (
                    SELECT account_key, signature, is_writable, is_signer from {}
                )
                as t (account_key, signature, is_writable, is_signer)
                ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn insert_transactions_for_block(
        &self,
        transactions: &Vec<BlockTransactionInfo>,
        slot: i64,
    ) -> anyhow::Result<()> {
        let temp_table = self.temp_table_tracker.get_new_temp_table();
        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            signature char(88),
            processed_slot BIGINT,
            is_successful BOOL,
            cu_requested BIGINT,
            cu_consumed BIGINT,
            prioritization_fees BIGINT,
            supp_infos text
        );",
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                signature,
                processed_slot,
                is_successful,
                cu_requested,
                cu_consumed,
                prioritization_fees,
                supp_infos
            ) FROM STDIN BINARY
        "#,
            temp_table
        );

        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::TEXT,
                Type::INT8,
                Type::BOOL,
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::TEXT,
            ],
        );
        pin_mut!(writer);
        for transaction in transactions {
            let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(7);
            args.push(&transaction.signature);
            args.push(&slot);
            args.push(&transaction.is_successful);
            args.push(&transaction.cu_requested);
            args.push(&transaction.cu_consumed);
            args.push(&transaction.prioritization_fees);
            args.push(&transaction.supp_infos);
            writer.as_mut().write(&args).await?;
        }
        writer.finish().await?;

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.transaction_infos
            (transaction_id, processed_slot, is_successful, cu_requested, cu_consumed, prioritization_fees, supp_infos)
                SELECT 
                    ( select transaction_id from banking_stage_results_2.transactions where signature = t.signature ),
                    t.processed_slot,
                    t.is_successful,
                    t.cu_requested,
                    t.cu_consumed,
                    t.prioritization_fees,
                    t.supp_infos
                FROM (
                    SELECT signature, processed_slot, is_successful, cu_requested, cu_consumed, prioritization_fees, supp_infos from {}
                )
                as t (signature, processed_slot, is_successful, cu_requested, cu_consumed, prioritization_fees, supp_infos)
                ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn save_account_usage_in_block(&self, block_info: &BlockInfo) -> anyhow::Result<()> {
        let temp_table = self.temp_table_tracker.get_new_temp_table();
        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            account_key CHAR(44),
            slot BIGINT,
            is_write_locked BOOL,
            total_cu_requested BIGINT,
            total_cu_consumed BIGINT,
            prioritization_fees_info text
        );",
                    temp_table
                )
                .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY {}(
                account_key,
                slot,
                is_write_locked,
                total_cu_requested,
                total_cu_consumed,
                prioritization_fees_info
            ) FROM STDIN BINARY
        "#,
            temp_table
        );

        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::TEXT,
                Type::INT8,
                Type::BOOL,
                Type::INT8,
                Type::INT8,
                Type::TEXT,
            ],
        );
        pin_mut!(writer);
        for account_usage in block_info.heavily_locked_accounts.iter() {
            let is_writable = account_usage.is_write_locked;
            let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(6);
            let pf_json = serde_json::to_string(&account_usage.prioritization_fee_data)?;
            args.push(&account_usage.key);
            args.push(&block_info.slot);
            args.push(&is_writable);
            args.push(&account_usage.cu_requested);
            args.push(&account_usage.cu_consumed);
            args.push(&pf_json);
            writer.as_mut().write(&args).await?;
        }
        writer.finish().await?;

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.accounts_map_blocks
            (   acc_id,
                slot,
                is_write_locked,
                total_cu_requested,
                total_cu_consumed,
                prioritization_fees_info
            )
                SELECT 
                    ( select acc_id from banking_stage_results_2.accounts where account_key = t.account_key ),
                    t.slot,
                    t.is_write_locked,
                    t.total_cu_requested,
                    t.total_cu_consumed,
                    t.prioritization_fees_info
                FROM (
                    SELECT account_key,
                    slot,
                    is_write_locked,
                    total_cu_requested,
                    total_cu_consumed,
                    prioritization_fees_info from {}
                )
                as t (account_key,
                    slot,
                    is_write_locked,
                    total_cu_requested,
                    total_cu_consumed,
                    prioritization_fees_info
                )
                ON CONFLICT DO NOTHING;
        "#,
            temp_table
        );
        self.client.execute(statement.as_str(), &[]).await?;

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn save_block_info(&self, block_info: &BlockInfo) -> anyhow::Result<()> {
        let statement = r#"
            INSERT INTO banking_stage_results_2.blocks (
                slot,
                block_hash,
                leader_identity,
                successful_transactions,
                processed_transactions,
                total_cu_used,
                total_cu_requested,
                supp_infos
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            "#;
        self.client
            .execute(
                statement,
                &[
                    &block_info.slot,
                    &block_info.block_hash,
                    &block_info.leader_identity.clone().unwrap_or_default(),
                    &block_info.successful_transactions,
                    &block_info.processed_transactions,
                    &block_info.total_cu_used,
                    &block_info.total_cu_requested,
                    &serde_json::to_string(&block_info.sup_info)?,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn save_banking_transaction_results(
        &self,
        txs: Vec<TransactionInfo>,
    ) -> anyhow::Result<()> {
        if txs.is_empty() {
            return Ok(());
        }
        // create transaction ids
        let signatures = txs
            .iter()
            .map(|transaction| transaction.signature.clone())
            .collect_vec();
        self.create_transaction_ids(signatures).await?;
        // create account ids
        let accounts = txs
            .iter()
            .map(|transaction| transaction.account_used.clone())
            .flatten()
            .map(|(acc, _)| acc)
            .collect_vec();
        self.create_accounts_for_transaction(accounts).await?;
        // add transaction in tx slot table
        self.insert_transaction_in_txslot_table(&txs.as_slice())
            .await?;
        let txs_accounts = txs
            .iter()
            .map(|tx| AccountsForTransaction {
                signature: tx.signature.clone(),
                accounts: tx
                    .account_used
                    .iter()
                    .map(|(key, is_writable)| AccountUsed {
                        key: key.clone(),
                        writable: *is_writable,
                        is_signer: false,
                    })
                    .collect(),
            })
            .collect_vec();
        // insert accounts for transaction
        self.insert_accounts_for_transaction(txs_accounts).await?;
        Ok(())
    }

    pub async fn copy_in(
        &self,
        statement: &str,
    ) -> Result<CopyInSink<bytes::Bytes>, tokio_postgres::error::Error> {
        // BinaryCopyInWriter
        // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
        self.client.copy_in(statement).await
    }

    pub async fn save_block(&self, block_info: BlockInfo) -> anyhow::Result<()> {
        // create transaction ids
        let signatures = block_info
            .transactions
            .iter()
            .map(|transaction| transaction.signature.clone())
            .collect_vec();
        self.create_transaction_ids(signatures).await?;
        // create account ids
        let accounts = block_info
            .heavily_locked_accounts
            .iter()
            .map(|acc| acc.key.clone())
            .collect_vec();
        self.create_accounts_for_transaction(accounts).await?;

        let txs_accounts = block_info
            .transactions
            .iter()
            .map(|tx| AccountsForTransaction {
                signature: tx.signature.clone(),
                accounts: tx
                    .accounts
                    .iter()
                    .map(|acc| AccountUsed {
                        key: acc.key.clone(),
                        writable: acc.is_writable,
                        is_signer: acc.is_signer,
                    })
                    .collect(),
            })
            .collect_vec();
        self.insert_accounts_for_transaction(txs_accounts).await?;

        // save transactions in block
        self.insert_transactions_for_block(&block_info.transactions, block_info.slot)
            .await?;

        // save account usage in blocks
        self.save_account_usage_in_block(&block_info).await?;
        self.save_block_info(&block_info).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Postgres {
    session: Arc<PostgresSession>,
}

impl Postgres {
    pub async fn new() -> Self {
        let session = PostgresSession::new().await.unwrap();
        Self {
            session: Arc::new(session),
        }
    }

    pub fn spawn_transaction_infos_saver(
        &self,
        map_of_transaction: Arc<DashMap<String, BTreeMap<u64, TransactionInfo>>>,
        slot: Arc<AtomicU64>,
    ) {
        let session = self.session.clone();
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let slot = slot.load(std::sync::atomic::Ordering::Relaxed);
                let mut txs_to_store = vec![];
                for tx in map_of_transaction.iter() {
                    let slot_map = tx.value();
                    let first_slot = slot_map.keys().next().cloned().unwrap_or_default();
                    if slot > first_slot + 300 {
                        txs_to_store.push(tx.key().clone());
                    }
                }

                if !txs_to_store.is_empty() {
                    debug!("saving transaction infos for {}", txs_to_store.len());
                    let data = txs_to_store
                        .iter()
                        .filter_map(|key| map_of_transaction.remove(key))
                        .map(|(_, tree)| tree.iter().map(|(_, info)| info).cloned().collect_vec())
                        .flatten()
                        .collect_vec();
                    let batches = data.chunks(1024).collect_vec();
                    for batch in batches {
                        if let Err(err) = session
                            .save_banking_transaction_results(batch.to_vec())
                            .await
                        {
                            panic!("Error saving transaction infos {}", err);
                        }
                    }
                }
            }
        });
    }

    pub async fn save_block_info(&self, block: BlockInfo) -> anyhow::Result<()> {
        self.session.save_block(block).await
    }
}

#[derive(Serialize, Clone)]
pub struct TransactionErrorData {
    error: TransactionError,
    count: usize,
}

pub struct AccountUsed {
    key: String,
    writable: bool,
    is_signer: bool,
}

pub struct AccountsForTransaction {
    pub signature: String,
    pub accounts: Vec<AccountUsed>,
}
