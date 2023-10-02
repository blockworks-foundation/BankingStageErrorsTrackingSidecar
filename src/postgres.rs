use std::sync::Arc;

use anyhow::Context;
use tokio::sync::RwLock;
use tokio_postgres::{Client, config::SslMode, NoTls, tls::MakeTlsConnect, Socket, types::ToSql};

use crate::transaction_info::TransactionInfo;


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
        let NUMBER_OF_ARGS = 8;

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());
        for tx in txs.iter() {
            args.push(&tx.signature);
            args.push(&tx.transaction_message);
            args.push(&tx.errors);
            args.push(&tx.is_executed);
            args.push(&tx.is_confirmed);
            args.push(&tx.first_notification_slot);
            args.push(&tx.cu_requested);
            args.push(&tx.prioritization_fees);
        }
        Ok(())
    }
}

pub struct Postgres {
    session: Arc<RwLock<PostgresSession>>,
}