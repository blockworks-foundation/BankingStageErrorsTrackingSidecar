use clap::Parser;
use grpc_banking_transactions_notifications::postgres;
use grpc_banking_transactions_notifications::postgres::PostgresSession;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {

    #[arg(short, long, help = "num of slots to keep relative to highest slot in blocks table")]
    pub num_slots_to_keep: i64,

    #[arg(short, long, default_value_t = false)]
    pub dry_run: bool,
}

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let Args {
        num_slots_to_keep, dry_run
    } = Args::parse();

    let session = PostgresSession::new().await.unwrap();

    session.cleanup_old_data(num_slots_to_keep, dry_run).await;

}
