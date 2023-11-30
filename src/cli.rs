use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    pub grpc_address_to_fetch_blocks: String,

    #[arg(short='x', long)]
    pub grpc_x_token: Option<String>,

    #[arg(short, long, value_delimiter = ',')]
    pub banking_grpc_addresses: Vec<String>,

    /// enable metrics to prometheus at addr
    #[arg(short = 'm', long, default_value_t = String::from("[::]:9091"))]
    pub prometheus_addr: String,
}
