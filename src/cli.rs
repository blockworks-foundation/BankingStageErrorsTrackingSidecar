use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    pub grpc_address: String,

    #[arg(long)]
    pub grpc_x_token: Option<String>,

    #[arg(short, long, default_value_t = String::from("http://127.0.0.1:8899"))]
    pub rpc_url: String,

    /// enable metrics to prometheus at addr
    #[arg(short = 'm', long, default_value_t = String::from("[::]:9091"))]
    pub prometheus_addr: String,
}
