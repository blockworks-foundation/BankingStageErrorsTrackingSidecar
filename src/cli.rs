use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    pub rpc_url: String,

    #[arg(short, long)]
    pub grpc_address_to_fetch_blocks: Option<String>,

    // note: this only applies to grpc_address_to_fetch_blocks
    #[arg(short = 'x', long)]
    pub grpc_x_token: Option<String>,

    #[arg(short, long, value_delimiter = ',')]
    pub banking_grpc_addresses: Vec<String>,

    /// enable metrics to prometheus at addr
    #[arg(short = 'm', long, default_value_t = String::from("[::]:9091"))]
    pub prometheus_addr: String,

    #[arg(short = 'a', long, default_value_t = String::from("alts.txt"))]
    pub alts: String,
}
