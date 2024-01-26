use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use anyhow::Context;
use base64::Engine;
use dashmap::DashMap;
use futures::pin_mut;
use itertools::Itertools;
use log::{debug, error, info, log, warn, Level};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use prometheus::{opts, register_int_gauge, IntGauge};
use serde::Serialize;
use solana_sdk::transaction::TransactionError;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
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

const BLOCK_WRITE_BUFFER_SIZE: usize = 5;
const LIMIT_LATEST_TXS_PER_ACCOUNT: i64 = 100;

lazy_static::lazy_static! {
    static ref ACCOUNTS_SAVING_QUEUE: IntGauge =
       register_int_gauge!(opts!("banking_stage_sidecar_accounts_save_queue", "Account in save queue")).unwrap();
}

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

#[derive(Clone)]
pub struct PostgresSession {
    client: Arc<Client>,
    temp_table_tracker: Arc<TempTableTracker>,
    accounts_for_transaction_sender:
        tokio::sync::mpsc::UnboundedSender<Vec<AccountsForTransaction>>,
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

        let (accounts_for_transaction_sender, accounts_for_transaction_reciever) =
            tokio::sync::mpsc::unbounded_channel();

        let instance = Self {
            client: Arc::new(client),
            temp_table_tracker: Arc::new(TempTableTracker::new()),
            accounts_for_transaction_sender,
        };
        Self::spawn_account_transaction_saving_task(
            instance.clone(),
            accounts_for_transaction_reciever,
        );
        Ok(instance)
    }

    fn spawn_account_transaction_saving_task(
        instance: PostgresSession,
        mut accounts_for_transaction_reciever: tokio::sync::mpsc::UnboundedReceiver<
            Vec<AccountsForTransaction>,
        >,
    ) {
        tokio::spawn(async move {
            while let Some(accounts_for_transaction) =
                accounts_for_transaction_reciever.recv().await
            {
                let instant: Instant = Instant::now();
                ACCOUNTS_SAVING_QUEUE.dec();
                if let Err(e) = instance
                    .insert_accounts_for_transaction(accounts_for_transaction)
                    .await
                {
                    error!("Error inserting accounts for transactions : {e:?}");
                }
                log::info!(
                    "Took {} ms to insert accounts for transaction",
                    instant.elapsed().as_millis()
                );
            }
            error!("account transaction saving task ");
            panic!("account transaction saving task");
        });
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
                std::process::exit(-1);
            }
            unreachable!("Postgres thread returned")
        });

        Ok(client)
    }

    pub async fn configure_work_mem(&self) {
        self.client
            .execute("SET work_mem TO '256MB'", &[])
            .await
            .unwrap();
        let work_mem: String = self
            .client
            .query_one("show work_mem", &[])
            .await
            .unwrap()
            .get("work_mem");
        info!("Configured work_mem={}", work_mem);
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
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
        pin_mut!(writer);
        for signature in signatures {
            writer.as_mut().write(&[&signature]).await?;
        }
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} signatures into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
        INSERT INTO banking_stage_results_2.transactions(signature) SELECT signature from {}
        ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        let num_rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} signatures in transactions table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

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
            key TEXT
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
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
        pin_mut!(writer);
        for account in accounts {
            writer.as_mut().write(&[&account]).await?;
        }
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} account keys into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
        INSERT INTO banking_stage_results_2.accounts(account_key) SELECT key from {}
        ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} account keys into accounts table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

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
        let started_at = Instant::now();
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
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} txs for tx_slot into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.transaction_slot(transaction_id, slot, error_code, count, utc_timestamp)
                SELECT ( select transaction_id from banking_stage_results_2.transactions where signature = t.sig ), t.slot, t.error_code, t.count, t.utc_timestamp
                FROM (
                    SELECT sig, slot, error_code, count, utc_timestamp from {}
                )
                as t (sig, slot, error_code, count, utc_timestamp) ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} txs into transaction_slot table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    /// add accounts for transaction to accounts_map_transaction table and update accounts_map_transaction_latest
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
            is_signer BOOL,
            is_atl BOOL
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
                account_key, signature, is_writable, is_signer, is_atl
            ) FROM STDIN BINARY
        "#,
            temp_table
        );
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = self.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[Type::TEXT, Type::TEXT, Type::BOOL, Type::BOOL, Type::BOOL],
        );
        pin_mut!(writer);
        for acc_tx in accounts_for_transaction {
            for acc in &acc_tx.accounts {
                let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(4);
                args.push(&acc.key);
                args.push(&acc_tx.signature);
                args.push(&acc.writable);
                args.push(&acc.is_signer);
                args.push(&acc.is_atl);
                writer.as_mut().write(&args).await?;
            }
        }
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} accounts for transaction into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        // merge data from temp table into accounts_map_transaction
        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.accounts_map_transaction(acc_id, transaction_id, is_writable, is_signer, is_atl)
                SELECT 
                    ( select acc_id from banking_stage_results_2.accounts where account_key = new_amt_data.account_key ),
                    ( select transaction_id from banking_stage_results_2.transactions where signature = new_amt_data.signature ),
                    new_amt_data.is_writable,
                    new_amt_data.is_signer,
                    new_amt_data.is_atl
                FROM {} AS new_amt_data
                ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        let rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} accounts into accounts_map_transaction in {}ms",
            rows,
            started_at.elapsed().as_millis()
        );

        // merge data from temp table into accounts_map_transaction_latest
        // note: query uses the array_dedup_append postgres function to deduplicate and limit the array size
        // example: array_dedup_append('{8,3,2,1}', '{5,3}', 4) -> {2,1,5,3}
        let temp_table_latest_agged = self.temp_table_tracker.get_new_temp_table();
        let statement = format!(
            r#"
            CREATE TEMP TABLE {temp_table_name} AS
            WITH amt_new AS (
                SELECT
                    acc_id, array_agg(transactions.transaction_id) AS tx_agged
                FROM {temp_table_newdata} AS newdata
                inner join banking_stage_results_2.accounts on accounts.account_key=newdata.account_key
                inner join banking_stage_results_2.transactions on transactions.signature=newdata.signature
                GROUP BY acc_id
            )
            SELECT
                acc_id,
                array_dedup_append(
                    (SELECT tx_ids FROM banking_stage_results_2.accounts_map_transaction_latest WHERE acc_id=amt_new.acc_id),
                    amt_new.tx_agged,
                    {limit}) AS tx_ids_agg
                FROM amt_new
        "#,
            temp_table_newdata = temp_table,
            temp_table_name = temp_table_latest_agged,
            limit = LIMIT_LATEST_TXS_PER_ACCOUNT
        );
        let started_at = Instant::now();
        let num_rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "merged new transactions into accounts_map_transaction_latest for {} accounts in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
            INSERT INTO banking_stage_results_2.accounts_map_transaction_latest(acc_id, tx_ids)
            SELECT acc_id, tx_ids_agg FROM {temp_table_name}
            ON CONFLICT (acc_id) DO UPDATE SET tx_ids = EXCLUDED.tx_ids
        "#,
            temp_table_name = temp_table_latest_agged
        );
        let started_at = Instant::now();
        let num_rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "upserted {} merged transaction arrays into accounts_map_transaction_latest in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        self.drop_temp_table(temp_table_latest_agged).await?;
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
        )",
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
        let started_at = Instant::now();
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
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} transactions for block into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

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
                FROM {} AS t
                ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        let num_rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} transactions for block into transaction_infos table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn save_account_usage_in_block(&self, block_info: &BlockInfo) -> anyhow::Result<()> {
        let temp_table = self.temp_table_tracker.get_new_temp_table();
        self.client
            .execute(
                format!(
                    "CREATE TEMP TABLE {}(
            account_key char(44),
            slot BIGINT,
            is_write_locked BOOL,
            total_cu_requested BIGINT,
            total_cu_consumed BIGINT,
            prioritization_fees_info text
        )",
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
        let started_at = Instant::now();
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
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} heavily_locked_accounts into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

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
                ON CONFLICT DO NOTHING
        "#,
            temp_table
        );
        let started_at = Instant::now();
        let num_rows = self.client.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} heavily_locked_accounts into accounts_map_blocks table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

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
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT DO NOTHING
            "#;
        let started_at = Instant::now();
        let num_rows = self
            .client
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
        debug!(
            "inserted {} block info into blocks table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        if num_rows == 0 {
            warn!("block_info already exists in blocks table - skipping insert");
        }

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
            .flat_map(|transaction| transaction.account_used.clone())
            .map(|(acc, _)| acc)
            .collect_vec();
        self.create_accounts_for_transaction(accounts).await?;
        // add transaction in tx slot table
        self.insert_transaction_in_txslot_table(txs.as_slice())
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
                        is_atl: false,
                    })
                    .collect(),
            })
            .collect_vec();
        // insert accounts for transaction
        ACCOUNTS_SAVING_QUEUE.inc();
        let _ = self.accounts_for_transaction_sender.send(txs_accounts);
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
                        key: acc.key.to_string(),
                        writable: acc.is_writable,
                        is_signer: acc.is_signer,
                        is_atl: acc.is_alt,
                    })
                    .collect(),
            })
            .collect_vec();

        ACCOUNTS_SAVING_QUEUE.inc();
        let _ = self.accounts_for_transaction_sender.send(txs_accounts);

        // save transactions in block
        self.insert_transactions_for_block(&block_info.transactions, block_info.slot)
            .await?;

        // save account usage in blocks
        self.save_account_usage_in_block(&block_info).await?;
        self.save_block_info(&block_info).await?;

        Ok(())
    }
}

impl PostgresSession {
    // "errors" -> lookup data
    // "blocks" -> keep, it its small
    // "accounts" -> keep, table is used for pkey lookup
    // "transactions" -> keep, table is used for pkey lookup
    // "accounts_map_blocks" -> delete rows slot before X
    // "accounts_map_transaction" -> delete rows with transaction_id before X
    // "transaction_infos" -> delete rows processed_slot before X
    // "transaction_slot" -> delete transaction with slot before X
    pub async fn cleanup_old_data(&self, slots_to_keep: i64, dry_run: bool) {
        // keep 1mio slots (apprx 4 days)
        info!(
            "{}Running cleanup job with slots_to_keep={}",
            if dry_run { "DRY-RUN: " } else { "" },
            slots_to_keep
        );

        self.configure_work_mem().await;

        {
            info!("Rows before cleanup:");
            self.log_rowcount(Level::Info, "blocks").await;
            self.log_rowcount(Level::Info, "accounts").await;
            self.log_rowcount(Level::Info, "transactions").await;
            self.log_rowcount(Level::Info, "accounts_map_blocks").await;
            self.log_rowcount(Level::Info, "accounts_map_transaction")
                .await;
            self.log_rowcount(Level::Info, "transaction_infos").await;
            self.log_rowcount(Level::Info, "transaction_slot").await;
        }

        // max slot from blocks table
        let latest_slot = self
            .client
            .query_one(
                "SELECT max(slot) as latest_slot FROM banking_stage_results_2.blocks",
                &[],
            )
            .await
            .unwrap();
        // assume not null
        let latest_slot: i64 = latest_slot.get("latest_slot");

        // do not delete cutoff_slot
        let cutoff_slot_excl = latest_slot - slots_to_keep;
        info!(
            "latest_slot={} from blocks table; keeping {} slots - i.e. slot {}",
            latest_slot, slots_to_keep, cutoff_slot_excl
        );

        let cutoff_transaction_incl: i64 = {
            let cutoff_transaction_from_txi_incl = self.client.query_one(
                &format!(
                    r"
                        SELECT max(transaction_id) as transaction_id FROM banking_stage_results_2.transaction_infos
                        WHERE processed_slot < {cutoff_slot}
                    ",
                    cutoff_slot = cutoff_slot_excl
                ),
                &[]).await.unwrap();
            let cutoff_transaction_from_txi_incl: Option<i64> =
                cutoff_transaction_from_txi_incl.get("transaction_id");

            let cutoff_transaction_from_txslot_incl = self.client.query_one(
                &format!(
                    r"
                        SELECT max(transaction_id) as transaction_id FROM banking_stage_results_2.transaction_slot
                        WHERE slot < {cutoff_slot}
                    ",
                    cutoff_slot = cutoff_slot_excl
                ),
                &[]).await.unwrap();
            let cutoff_transaction_from_txslot_incl: Option<i64> =
                cutoff_transaction_from_txslot_incl.get("transaction_id");

            debug!(
                "cutoff_transaction_from_txi_incl: {:?}",
                cutoff_transaction_from_txi_incl
            );
            debug!(
                "cutoff_transaction_from_txslot_incl: {:?}",
                cutoff_transaction_from_txslot_incl
            );

            let min_transaction_id: Option<i64> = vec![
                cutoff_transaction_from_txi_incl,
                cutoff_transaction_from_txslot_incl,
            ]
            .into_iter()
            .flatten()
            .min();

            if min_transaction_id.is_none() {
                info!("nothing to delete - abort");
                return;
            }

            min_transaction_id.unwrap()
        };

        info!(
            "delete slots but keep slots including and after {}",
            cutoff_slot_excl
        );
        info!(
            "should delete transactions with id =< {}",
            cutoff_transaction_incl
        );

        {
            let txs_to_delete = self
                .client
                .query_one(
                    &format!(
                        r"
                        SELECT count(*) as cnt_tx FROM banking_stage_results_2.transactions txs
                        WHERE txs.transaction_id <= {cutoff_transaction}
                    ",
                        cutoff_transaction = cutoff_transaction_incl
                    ),
                    &[],
                )
                .await
                .unwrap();

            let txs_to_delete: i64 = txs_to_delete.get("cnt_tx");

            info!("would delete transactions: {}", txs_to_delete);
        }

        {
            let amt_to_delete = self.client.query_one(
                &format!(
                    r"
                        SELECT count(*) as cnt_amt FROM banking_stage_results_2.accounts_map_transaction amt
                        WHERE amt.transaction_id <= {cutoff_transaction}
                    ",
                    cutoff_transaction = cutoff_transaction_incl
                ),
                &[]).await.unwrap();

            let amt_to_delete: i64 = amt_to_delete.get("cnt_amt");

            info!("would delete accounts_map_transaction: {}", amt_to_delete);
        }

        {
            let amb_to_delete = self.client.query_one(
                &format!(
                    r"
                        SELECT count(*) as cnt_amb FROM banking_stage_results_2.accounts_map_blocks amb
                        WHERE amb.slot < {cutoff_slot}
                    ",
                    cutoff_slot = cutoff_slot_excl
                ),
                &[]).await.unwrap();

            let amb_to_delete: i64 = amb_to_delete.get("cnt_amb");

            info!("would delete from accounts_map_blocks: {}", amb_to_delete);
        }

        {
            let txi_to_delete = self.client.query_one(
                &format!(
                    r"
                        SELECT count(*) as cnt_txis FROM banking_stage_results_2.transaction_infos txi
                        WHERE txi.processed_slot < {cutoff_slot}
                    ",
                    cutoff_slot = cutoff_slot_excl
                ),
                &[]).await.unwrap();

            let txi_to_delete: i64 = txi_to_delete.get("cnt_txis");

            info!("would delete from transaction_infos: {}", txi_to_delete);
        }

        {
            let txslot_to_delete = self.client.query_one(
                &format!(
                    r"
                        SELECT count(*) as cnt_txslots FROM banking_stage_results_2.transaction_slot tx_slot
                        WHERE tx_slot.slot < {cutoff_slot}
                    ",
                    cutoff_slot = cutoff_slot_excl
                ),
                &[]).await.unwrap();

            let txslot_to_delete: i64 = txslot_to_delete.get("cnt_txslots");

            info!("would delete from transaction_slot: {}", txslot_to_delete);
        }

        if dry_run {
            warn!("dry-run: stop now without changing anything");
            return;
        }

        {
            let started = Instant::now();
            let deleted_rows = self.client.execute(
                &format!(
                    r"
                    DELETE FROM banking_stage_results_2.transactions WHERE transaction_id <= {transaction_id}
                ", transaction_id = cutoff_transaction_incl
                ), &[]).await.unwrap();
            info!(
                "Deleted {} rows from transactions in {:.3}s",
                deleted_rows,
                started.elapsed().as_secs_f32()
            );
        }
        {
            let started = Instant::now();
            let deleted_rows = self.client.execute(
                &format!(
                    r"
                    DELETE FROM banking_stage_results_2.accounts_map_transaction WHERE transaction_id <= {transaction_id}
                ", transaction_id = cutoff_transaction_incl
                ), &[]).await.unwrap();
            info!(
                "Deleted {} rows from accounts_map_transaction in {:.3}s",
                deleted_rows,
                started.elapsed().as_secs_f32()
            );
        }
        {
            let started = Instant::now();
            let deleted_rows = self.client.execute(
                &format!(
                    r"
                    DELETE FROM banking_stage_results_2.accounts_map_blocks WHERE slot <= {cutoff_slot}
                ", cutoff_slot = cutoff_slot_excl
                ), &[]).await.unwrap();
            info!(
                "Deleted {} rows from accounts_map_blocks in {:.3}s",
                deleted_rows,
                started.elapsed().as_secs_f32()
            );
        }
        {
            let started = Instant::now();
            let deleted_rows = self.client.execute(
                &format!(
                    r"
                    DELETE FROM banking_stage_results_2.transaction_infos WHERE processed_slot < {cutoff_slot}
                ", cutoff_slot = cutoff_slot_excl
                ), &[]).await.unwrap();
            info!(
                "Deleted {} rows from transaction_infos in {:.3}s",
                deleted_rows,
                started.elapsed().as_secs_f32()
            );
        }
        {
            let started = Instant::now();
            let deleted_rows = self
                .client
                .execute(
                    &format!(
                        r"
                    DELETE FROM banking_stage_results_2.transaction_slot WHERE slot < {cutoff_slot}
                ",
                        cutoff_slot = cutoff_slot_excl
                    ),
                    &[],
                )
                .await
                .unwrap();
            info!(
                "Deleted {} rows from transaction_slot in {:.3}s",
                deleted_rows,
                started.elapsed().as_secs_f32()
            );
        }

        {
            info!("Rows after cleanup:");
            self.log_rowcount(Level::Info, "blocks").await;
            self.log_rowcount(Level::Info, "accounts").await;
            self.log_rowcount(Level::Info, "transactions").await;
            self.log_rowcount(Level::Info, "accounts_map_blocks").await;
            self.log_rowcount(Level::Info, "accounts_map_transaction")
                .await;
            self.log_rowcount(Level::Info, "transaction_infos").await;
            self.log_rowcount(Level::Info, "transaction_slot").await;
        }

        info!("Cleanup job completed.");
    }

    async fn log_rowcount(&self, level: Level, table: &str) {
        let count: i64 = self
            .client
            .query_one(
                &format!(
                    "SELECT count(*) as cnt FROM banking_stage_results_2.{tablename}",
                    tablename = table
                ),
                &[],
            )
            .await
            .unwrap()
            .get("cnt");
        log!(level, "- rows count in table <{}>: {}", table, count);
    }
}

#[derive(Clone)]
pub struct Postgres {
    session: Arc<PostgresSession>,
}

impl Postgres {
    pub async fn new_with_workmem() -> Self {
        let session = PostgresSession::new().await.unwrap();
        let session = Arc::new(session);
        session.configure_work_mem().await;
        Self { session }
    }

    pub fn spawn_block_saver(&self) -> Sender<BlockInfo> {
        let (block_sender, mut block_receiver) =
            tokio::sync::mpsc::channel::<BlockInfo>(BLOCK_WRITE_BUFFER_SIZE);

        let session = self.session.clone();
        tokio::spawn(async move {
            loop {
                match block_receiver.recv().await {
                    None => {
                        warn!("block_receiver closed - stopping thread");
                        return;
                    }

                    Some(block) => {
                        let slot = block.slot;
                        let instant = Instant::now();
                        match session.save_block(block).await {
                            Ok(_) => {
                                info!(
                                    "saving block {} took {} ms",
                                    slot,
                                    instant.elapsed().as_millis()
                                );
                            }
                            Err(err) => {
                                error!("saving block failed {}", err);
                            }
                        }
                    }
                };
            }
        });

        block_sender
    }

    pub fn spawn_transaction_infos_saver(
        &self,
        map_of_transaction: Arc<DashMap<(String, u64), TransactionInfo>>,
        slot: Arc<AtomicU64>,
    ) {
        let session = self.session.clone();
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let slot = slot.load(std::sync::atomic::Ordering::Relaxed);
                let mut txs_to_store = vec![];
                for tx in map_of_transaction.iter() {
                    if slot > tx.key().1 + 300 {
                        txs_to_store.push(tx.key().clone());
                    }
                }

                if !txs_to_store.is_empty() {
                    debug!("saving transaction infos for {}", txs_to_store.len());
                    let batches = txs_to_store
                        .iter()
                        .filter_map(|key| map_of_transaction.remove(key))
                        .map(|(_, trans)| trans)
                        .collect_vec();
                    if let Err(err) = session.save_banking_transaction_results(batches).await {
                        panic!("saving transaction infos failed {}", err);
                    }
                }
            }
        });
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
    is_atl: bool,
}

pub struct AccountsForTransaction {
    pub signature: String,
    pub accounts: Vec<AccountUsed>,
}

pub async fn send_block_info_to_buffer(
    block_sender_postgres: Sender<BlockInfo>,
    block_info: BlockInfo,
) -> anyhow::Result<()> {
    debug!(
        "block buffer capacity: {}",
        block_sender_postgres.capacity()
    );

    const WARNING_THRESHOLD: Duration = Duration::from_millis(3000);

    let started_at = Instant::now();
    if let Err(SendTimeoutError::Timeout(block)) = block_sender_postgres
        .send_timeout(block_info, WARNING_THRESHOLD)
        .await
    {
        let slot = block.slot;
        warn!(
            "Block {} was not buffered for {:.3}s - continue waiting",
            slot,
            WARNING_THRESHOLD.as_secs_f32()
        );
        block_sender_postgres.send(block).await?;
        info!(
            "Block {} was finally buffered after {:.3}s",
            slot,
            started_at.elapsed().as_secs_f32()
        );
    }

    Ok(())
}
