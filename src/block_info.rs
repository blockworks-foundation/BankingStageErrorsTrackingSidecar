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
    slot_history::Slot,
};
use solana_transaction_status::{RewardType, UiConfirmedBlock};
use std::collections::HashMap;

#[derive(Serialize, Debug, Clone)]
pub struct AccountUsage {
    pub key: String,
    pub cu_requested: u64,
    pub cu_consumed: u64,
    pub max_pf: u64,
    pub min_pf: u64,
    pub median_pf: u64,
}

pub struct AccountData {
    pub key: String,
    pub cu_requested: u64,
    pub cu_consumed: u64,
    pub vec_pf: Vec<u64>,
}

impl From<&AccountData> for AccountUsage {
    fn from(value: &AccountData) -> Self {
        let mut median = value.vec_pf.clone();
        median.sort();
        let mid = median.len() / 2;
        AccountUsage {
            key: value.key.clone(),
            cu_requested: value.cu_requested,
            cu_consumed: value.cu_consumed,
            max_pf: value.vec_pf.iter().max().cloned().unwrap_or_default(),
            min_pf: value.vec_pf.iter().min().cloned().unwrap_or_default(),
            median_pf: median[mid],
        }
    }
}

#[derive(Serialize, Debug)]
pub struct BlockSupplimentaryInfo {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,
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
    pub sup_info: Option<BlockSupplimentaryInfo>,
}

impl BlockInfo {
    pub fn process_versioned_message(
        message: &VersionedMessage,
        prio_fees_in_block: &mut Vec<u64>,
        writelocked_accounts: &mut HashMap<Pubkey, AccountData>,
        readlocked_accounts: &mut HashMap<Pubkey, AccountData>,
        cu_consumed: u64,
        total_cu_requested: &mut u64,
        is_vote: bool,
    ) {
        let (cu_requested, prio_fees, nb_ix_except_cb) = {
            let mut cu_request: Option<u64> = None;
            let mut prio_fees: Option<u64> = None;
            let mut nb_ix_except_cb: u64 = 0;

            for ix in message.instructions() {
                if ix
                    .program_id(message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    let ix_which =
                        try_from_slice_unchecked::<ComputeBudgetInstruction>(ix.data.as_slice());
                    if let Ok(ix_which) = ix_which {
                        match ix_which {
                            ComputeBudgetInstruction::RequestUnitsDeprecated {
                                units,
                                additional_fee,
                            } => {
                                cu_request = Some(units as u64);
                                if additional_fee > 0 {
                                    prio_fees = Some(
                                        (units as u64)
                                            .saturating_mul(1000)
                                            .saturating_div(additional_fee as u64),
                                    );
                                }
                            }
                            ComputeBudgetInstruction::SetComputeUnitLimit(units) => {
                                cu_request = Some(units as u64)
                            }
                            ComputeBudgetInstruction::SetComputeUnitPrice(price) => {
                                prio_fees = Some(price)
                            }
                            _ => {}
                        }
                    }
                } else {
                    nb_ix_except_cb += 1;
                }
            }

            (cu_request, prio_fees, nb_ix_except_cb)
        };
        let prioritization_fees = prio_fees.unwrap_or_default();
        prio_fees_in_block.push(prioritization_fees);
        let cu_requested =
            std::cmp::min(1_400_000, cu_requested.unwrap_or(200000 * nb_ix_except_cb));
        *total_cu_requested += cu_requested;
        if !is_vote {
            let accounts = message
                .static_account_keys()
                .iter()
                .enumerate()
                .map(|(index, account)| (message.is_maybe_writable(index), *account))
                .collect_vec();
            for writable_account in accounts.iter().filter(|x| x.0).map(|x| x.1) {
                match writelocked_accounts.get_mut(&writable_account) {
                    Some(x) => {
                        x.cu_requested += cu_requested;
                        x.cu_consumed += cu_consumed;
                        x.vec_pf.push(prioritization_fees);
                    }
                    None => {
                        writelocked_accounts.insert(
                            writable_account,
                            AccountData {
                                key: writable_account.to_string(),
                                cu_consumed,
                                cu_requested,
                                vec_pf: vec![prioritization_fees],
                            },
                        );
                    }
                }
            }

            for readable_account in accounts.iter().filter(|x| !x.0).map(|x| x.1) {
                match readlocked_accounts.get_mut(&readable_account) {
                    Some(x) => {
                        x.cu_requested += cu_requested;
                        x.cu_consumed += cu_consumed;
                        x.vec_pf.push(prioritization_fees);
                    }
                    None => {
                        readlocked_accounts.insert(
                            readable_account,
                            AccountData {
                                key: readable_account.to_string(),
                                cu_consumed,
                                cu_requested,
                                vec_pf: vec![prioritization_fees],
                            },
                        );
                    }
                }
            }
        }
    }

    pub fn calculate_account_usage(
        writelocked_accounts: &HashMap<Pubkey, AccountData>,
        readlocked_accounts: &HashMap<Pubkey, AccountData>,
    ) -> (Vec<AccountUsage>, Vec<AccountUsage>) {
        let mut heavily_writelocked_accounts = writelocked_accounts
            .iter()
            .map(|(_, data)| AccountUsage::from(data))
            .collect_vec();
        heavily_writelocked_accounts.sort_by(|lhs, rhs| rhs.cu_consumed.cmp(&lhs.cu_consumed));

        let mut heavily_readlocked_accounts: Vec<_> = readlocked_accounts
            .iter()
            .map(|(_, data)| AccountUsage::from(data))
            .collect();
        heavily_readlocked_accounts.sort_by(|lhs, rhs| rhs.cu_consumed.cmp(&lhs.cu_consumed));
        (heavily_writelocked_accounts, heavily_readlocked_accounts)
    }

    pub fn calculate_supp_info(
        prio_fees_in_block: &mut Vec<u64>,
    ) -> Option<BlockSupplimentaryInfo> {
        if !prio_fees_in_block.is_empty() {
            prio_fees_in_block.sort();
            let median_index = prio_fees_in_block.len() / 2;
            let p75_index = prio_fees_in_block.len() * 75 / 100;
            let p90_index = prio_fees_in_block.len() * 90 / 100;
            Some(BlockSupplimentaryInfo {
                p_min: prio_fees_in_block[0],
                p_median: prio_fees_in_block[median_index],
                p_75: prio_fees_in_block[p75_index],
                p_90: prio_fees_in_block[p90_index],
                p_max: prio_fees_in_block.last().cloned().unwrap_or_default(),
            })
        } else {
            None
        }
    }

    pub fn new(
        block: &yellowstone_grpc_proto_original::prelude::SubscribeUpdateBlock,
        banking_stage_errors: Option<i64>,
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
        let mut writelocked_accounts: HashMap<Pubkey, AccountData> = HashMap::new();
        let mut readlocked_accounts: HashMap<Pubkey, AccountData> = HashMap::new();
        let mut total_cu_requested: u64 = 0;
        let mut prio_fees_in_block = vec![];
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

            Self::process_versioned_message(
                &message,
                &mut prio_fees_in_block,
                &mut writelocked_accounts,
                &mut readlocked_accounts,
                meta.compute_units_consumed.unwrap_or(0),
                &mut total_cu_requested,
                transaction.is_vote,
            );
        }

        let (heavily_writelocked_accounts, heavily_readlocked_accounts) =
            Self::calculate_account_usage(&writelocked_accounts, &readlocked_accounts);

        let sup_info = Self::calculate_supp_info(&mut prio_fees_in_block);

        BlockInfo {
            block_hash,
            slot: slot as i64,
            leader_identity,
            successful_transactions: successful_transactions as i64,
            processed_transactions: processed_transactions as i64,
            banking_stage_errors,
            total_cu_used,
            total_cu_requested: total_cu_requested as i64,
            heavily_writelocked_accounts,
            heavily_readlocked_accounts,
            sup_info,
        }
    }

    pub fn _new_from_rpc_block(
        slot: Slot,
        block: &UiConfirmedBlock,
        banking_stage_errors_count: i64,
    ) -> Option<Self> {
        let block_hash = block.blockhash.clone();
        let leader_identity = block
            .rewards
            .as_ref()
            .map(|rewards| {
                rewards
                    .iter()
                    .find(|x| x.reward_type == Some(RewardType::Fee))
                    .map(|x| x.pubkey.clone())
            })
            .unwrap_or(None);
        let transactions = if let Some(transactions) = &block.transactions {
            transactions
        } else {
            return None;
        };

        let successful_transactions = transactions
            .iter()
            .filter(|x| x.meta.as_ref().map(|x| x.err.is_none()).unwrap_or(false))
            .count() as u64;
        let processed_transactions = transactions.len() as u64;

        let total_cu_used = transactions
            .iter()
            .map(|x| {
                x.meta
                    .as_ref()
                    .map(|x| match x.compute_units_consumed {
                        solana_transaction_status::option_serializer::OptionSerializer::Some(x) => {
                            x
                        }
                        solana_transaction_status::option_serializer::OptionSerializer::Skip => 0,
                        solana_transaction_status::option_serializer::OptionSerializer::None => 0,
                    })
                    .unwrap_or(0)
            })
            .sum::<u64>() as i64;
        let mut writelocked_accounts: HashMap<Pubkey, AccountData> = HashMap::new();
        let mut readlocked_accounts: HashMap<Pubkey, AccountData> = HashMap::new();
        let mut total_cu_requested: u64 = 0;
        let mut prio_fees_in_block = vec![];
        for transaction in transactions {
            let Some(tx) = transaction.transaction.decode() else {
                continue;
            };

            let message = &tx.message;

            let Some(meta) = &transaction.meta else {
                continue;
            };
            let is_vote = false;

            Self::process_versioned_message(
                message,
                &mut prio_fees_in_block,
                &mut writelocked_accounts,
                &mut readlocked_accounts,
                match meta.compute_units_consumed {
                    solana_transaction_status::option_serializer::OptionSerializer::None => 0,
                    solana_transaction_status::option_serializer::OptionSerializer::Skip => 0,
                    solana_transaction_status::option_serializer::OptionSerializer::Some(x) => x,
                },
                &mut total_cu_requested,
                is_vote,
            );
        }

        let (heavily_writelocked_accounts, heavily_readlocked_accounts) =
            Self::calculate_account_usage(&writelocked_accounts, &readlocked_accounts);

        let sup_info = Self::calculate_supp_info(&mut prio_fees_in_block);
        Some(BlockInfo {
            block_hash,
            slot: slot as i64,
            leader_identity,
            successful_transactions: successful_transactions as i64,
            processed_transactions: processed_transactions as i64,
            banking_stage_errors: Some(banking_stage_errors_count),
            total_cu_used,
            total_cu_requested: total_cu_requested as i64,
            heavily_writelocked_accounts,
            heavily_readlocked_accounts,
            sup_info,
        })
    }
}
