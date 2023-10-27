use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use itertools::Itertools;
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
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

pub struct BlockInfo {
    pub block_hash: String,
    pub slot: i64,
    pub leader_identity: Option<String>,
    pub successful_transactions: i64,
    pub banking_stage_errors: i64,
    pub processed_transactions: i64,
    pub total_cu_used: i64,
    pub total_cu_requested: i64,
    pub heavily_writelocked_accounts: Vec<String>,
}

impl BlockInfo {
    pub fn new(
        block: &SubscribeUpdateBlock,
        errors_by_slots: &Arc<DashMap<u64, u64>>,
    ) -> BlockInfo {
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
        let banking_stage_errors = errors_by_slots.get(&slot).map(|x| *x).unwrap_or_default();
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
        let mut writelocked_accounts: HashMap<Pubkey, (u64, u64)> = HashMap::new();
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
                                    Some(((units * 1000) / additional_fee) as u64),
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

            let writable_accounts = message
                .static_account_keys()
                .iter()
                .enumerate()
                .filter(|(index, _)| message.is_maybe_writable(*index))
                .map(|x| x.1.clone())
                .collect_vec();
            for writable_account in writable_accounts {
                match writelocked_accounts.get_mut(&writable_account) {
                    Some(x) => {
                        x.0 += cu_requested;
                        x.1 += cu_consumed;
                    }
                    None => {
                        writelocked_accounts.insert(writable_account, (cu_requested, cu_consumed));
                    }
                }
            }
        }

        let mut heavily_writelocked_accounts = writelocked_accounts
            .iter()
            .filter(|x| x.1 .1 > 1000000)
            .collect_vec();
        heavily_writelocked_accounts.sort_by(|lhs, rhs| (*rhs.1).cmp(lhs.1));
        let heavily_writelocked_accounts = heavily_writelocked_accounts
            .iter()
            .map(|(pubkey, (cu_req, cu_con))| {
                format!("(k:{}, cu_req:{}, cu_con:{})", **pubkey, *cu_req, *cu_con)
            })
            .collect_vec();
        BlockInfo {
            block_hash,
            slot: slot as i64,
            leader_identity,
            successful_transactions: successful_transactions as i64,
            processed_transactions: processed_transactions as i64,
            banking_stage_errors: banking_stage_errors as i64,
            total_cu_used,
            total_cu_requested: total_cu_requested as i64,
            heavily_writelocked_accounts,
        }
    }
}
