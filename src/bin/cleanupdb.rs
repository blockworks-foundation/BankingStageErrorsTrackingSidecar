use grpc_banking_transactions_notifications::postgres;
use grpc_banking_transactions_notifications::postgres::PostgresSession;

#[tokio::main()]
async fn main() {
    tracing_subscriber::fmt::init();

    let session = PostgresSession::new().await.unwrap();

    session.cleanup_old_data(true).await;

}
