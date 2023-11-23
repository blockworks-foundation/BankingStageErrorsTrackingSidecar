use itertools::Itertools;
use serde::Serialize;
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    compute_budget::{self, ComputeBudgetInstruction},
    instruction::CompiledInstruction,
    message::{
        v0::{self, MessageAddressTableLookup},
        MessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
};
use std::collections::HashMap;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

#[derive(Serialize, Debug, Clone)]
pub struct AccountUsage {
    pub key: Pubkey,
    pub cu_requested: u64,
    pub cu_consumed: u64,
}

pub struct BlockInfo {
    pub block_hash: String,
    pub slot: i64,
    pub leader_identity: Option<String>,
    pub successful_transactions: i64,
    pub banking_stage_errors: Option<i64>,
    pub processed_transactions: i64,
    pub total_cu_used: i64,
    pub total_cu_requested: i64,
    pub heavily_writelocked_accounts: Vec<AccountUsage>,
    pub heavily_readlocked_accounts: Vec<AccountUsage>,
}

impl BlockInfo {
    pub fn new(block: &SubscribeUpdateBlock, banking_stage_error_count: Option<i64>) -> BlockInfo {
        let block_hash = block.blockhash.clone();
        let slot = block.slot;
        let leader_identity = block
            .rewards
            .as_ref()
            .map(|rewards| {
                rewards
                    .rewards
                    .iter()
                    .find(|x| x.reward_type == 1)
                    .map(|x| x.pubkey.clone())
            })
            .unwrap_or(None);
        let successful_transactions = block
            .transactions
            .iter()
            .filter(|x| x.meta.as_ref().map(|x| x.err.is_none()).unwrap_or(false))
            .count() as u64;
        let processed_transactions = block.transactions.len() as u64;

        let total_cu_used = block
            .transactions
            .iter()
            .map(|x| {
                x.meta
                    .as_ref()
                    .map(|x| x.compute_units_consumed.unwrap_or(0))
                    .unwrap_or(0)
            })
            .sum::<u64>() as i64;
        let mut writelocked_accounts: HashMap<Pubkey, AccountUsage> = HashMap::new();
        let mut readlocked_accounts: HashMap<Pubkey, AccountUsage> = HashMap::new();
        let mut total_cu_requested: u64 = 0;
        for transaction in &block.transactions {
            let Some(tx) = &transaction.transaction else {
                continue;
            };

            let Some(message) = &tx.message else {
                continue;
            };

            let Some(header) = &message.header else {
                continue;
            };

            let Some(meta) = &transaction.meta else {
                continue;
            };

            let message = VersionedMessage::V0(v0::Message {
                header: MessageHeader {
                    num_required_signatures: header.num_required_signatures as u8,
                    num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
                    num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
                },
                account_keys: message
                    .account_keys
                    .clone()
                    .into_iter()
                    .map(|key| {
                        let bytes: [u8; 32] =
                            key.try_into().unwrap_or(Pubkey::default().to_bytes());
                        Pubkey::new_from_array(bytes)
                    })
                    .collect(),
                recent_blockhash: solana_sdk::hash::Hash::new(&message.recent_blockhash),
                instructions: message
                    .instructions
                    .clone()
                    .into_iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: ix.program_id_index as u8,
                        accounts: ix.accounts,
                        data: ix.data,
                    })
                    .collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .clone()
                    .into_iter()
                    .map(|table| {
                        let bytes: [u8; 32] = table
                            .account_key
                            .try_into()
                            .unwrap_or(Pubkey::default().to_bytes());
                        MessageAddressTableLookup {
                            account_key: Pubkey::new_from_array(bytes),
                            writable_indexes: table.writable_indexes,
                            readonly_indexes: table.readonly_indexes,
                        }
                    })
                    .collect(),
            });

            let legacy_compute_budget: Option<(u32, Option<u64>)> =
                message.instructions().iter().find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                            units,
                            additional_fee,
                        }) = try_from_slice_unchecked(i.data.as_slice())
                        {
                            if additional_fee > 0 {
                                return Some((
                                    units,
                                    Some(
                                        ((units.saturating_mul(1000))
                                            .saturating_div(additional_fee))
                                            as u64,
                                    ),
                                ));
                            } else {
                                return Some((units, None));
                            }
                        }
                    }
                    None
                });

            let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);

            let cu_requested = message
                .instructions()
                .iter()
                .find_map(|i| {
                    if i.program_id(message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                            try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(limit);
                        }
                    }
                    None
                })
                .or(legacy_cu_requested);
            let cu_requested = cu_requested.unwrap_or(200000) as u64;
            let cu_consumed = meta.compute_units_consumed.unwrap_or(0);
            total_cu_requested = total_cu_requested + cu_requested;

            let accounts = message
                .static_account_keys()
                .iter()
                .enumerate()
                .map(|(index, account)| (message.is_maybe_writable(index), account.clone()))
                .collect_vec();
            for writable_account in accounts.iter().filter(|x| x.0 == true).map(|x| x.1) {
                match writelocked_accounts.get_mut(&writable_account) {
                    Some(x) => {
                        x.cu_requested += cu_requested;
                        x.cu_consumed += cu_consumed;
                    }
                    None => {
                        writelocked_accounts.insert(
                            writable_account,
                            AccountUsage {
                                key: writable_account,
                                cu_consumed,
                                cu_requested,
                            },
                        );
                    }
                }
            }

            for readable_account in accounts.iter().filter(|x| x.0 == false).map(|x| x.1) {
                match readlocked_accounts.get_mut(&readable_account) {
                    Some(x) => {
                        x.cu_requested += cu_requested;
                        x.cu_consumed += cu_consumed;
                    }
                    None => {
                        readlocked_accounts.insert(
                            readable_account,
                            AccountUsage {
                                key: readable_account,
                                cu_consumed,
                                cu_requested,
                            },
                        );
                    }
                }
            }
        }

        let mut heavily_writelocked_accounts = writelocked_accounts
            .iter()
            .filter(|(_, account)| account.cu_consumed > 1000000)
            .map(|x| x.1.clone())
            .collect_vec();
        heavily_writelocked_accounts.sort_by(|lhs, rhs| rhs.cu_consumed.cmp(&lhs.cu_consumed));

        let mut heavily_readlocked_accounts: Vec<_> = readlocked_accounts
            .iter()
            .filter(|(_, acc)| acc.cu_consumed > 1000000)
            .map(|x| x.1.clone())
            .collect();
        heavily_readlocked_accounts.sort_by(|lhs, rhs| rhs.cu_consumed.cmp(&lhs.cu_consumed));
        BlockInfo {
            block_hash,
            slot: slot as i64,
            leader_identity,
            successful_transactions: successful_transactions as i64,
            processed_transactions: processed_transactions as i64,
            banking_stage_errors: banking_stage_error_count,
            total_cu_used,
            total_cu_requested: total_cu_requested as i64,
            heavily_writelocked_accounts,
            heavily_readlocked_accounts,
        }
    }
}
