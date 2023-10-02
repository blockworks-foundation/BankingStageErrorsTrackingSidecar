use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use futures::StreamExt;
use solana_sdk::signature::Signature;
use transaction_info::TransactionInfo;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterBlocks,
};

mod transaction_info;
mod postgres;

#[tokio::main()]
async fn main() {
    let grpc_addr = "http://127.0.0.1:10000";
    let mut client = GeyserGrpcClient::connect(grpc_addr, None::<&'static str>, None).unwrap();
    let map_of_infos = Arc::new(DashMap::<String, TransactionInfo>::new());

    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );
    let commitment_level = CommitmentLevel::Processed;

    let mut stream = client
        .subscribe_once(
            HashMap::new(),
            Default::default(),
            HashMap::new(),
            Default::default(),
            blocks_subs,
            Default::default(),
            Some(commitment_level),
            Default::default(),
            true,
        )
        .await
        .unwrap();
    while let Some(message) = stream.next().await {
        let message = message.unwrap();

        let Some(update) = message.update_oneof else {
                    continue;
                };

        match update {
            UpdateOneof::BankingTransactionErrors(transaction) => {
                if transaction.error.is_none() {
                    continue;
                }
                let sig = transaction.signature.to_string();
                match map_of_infos.get_mut(&sig) {
                    Some(mut x) => {
                        x.add_notification(&transaction);
                    }
                    None => {
                        map_of_infos.insert(
                            sig,
                            TransactionInfo::new(transaction.signature, transaction.slot),
                        );
                    }
                }
            }
            UpdateOneof::Block(block) => {
                for transaction in block.transactions {
                    let Some(tx) = &transaction.transaction else {
                        continue;
                    };
                    let signature = Signature::try_from(tx.signatures[0].clone()).unwrap();
                    if let Some(mut info) = map_of_infos.get_mut(&signature.to_string()) {
                        info.add_transaction(&transaction);
                    }
                }
            }
            _ => {
            }
        };
    }
}
