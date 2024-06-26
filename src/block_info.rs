use crate::alt_store::ALTStore;
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
    signature::Signature,
    slot_history::Slot,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use std::rc::Rc;

#[derive(Serialize, Debug, Clone)]
pub struct PrioFeeData {
    pub max: Option<u64>,
    pub min: Option<u64>,
    pub p75: Option<u64>,
    pub p90: Option<u64>,
    pub p95: Option<u64>,
    pub med: Option<u64>,
}

impl PrioFeeData {
    pub fn new(pf_vec: &Vec<u64>) -> Self {
        let mut vec = pf_vec.clone();
        vec.sort();
        Self {
            max: vec.last().cloned(),
            min: vec.first().cloned(),
            p75: (pf_vec.len() > 1).then(|| pf_vec[pf_vec.len() * 75 / 100]),
            p90: (pf_vec.len() > 1).then(|| pf_vec[pf_vec.len() * 90 / 100]),
            p95: (pf_vec.len() > 1).then(|| pf_vec[pf_vec.len() * 95 / 100]),
            med: (pf_vec.len() > 1).then(|| pf_vec[pf_vec.len() / 2]),
        }
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct AccountUsage {
    pub key: String,
    pub is_write_locked: bool,
    pub cu_requested: i64,
    pub cu_consumed: i64,
    pub prioritization_fee_data: PrioFeeData,
}

pub struct AccountData {
    pub key: String,
    pub cu_requested: u64,
    pub cu_consumed: u64,
    pub vec_pf: Vec<u64>,
}

impl From<(&AccountData, bool)> for AccountUsage {
    fn from(value: (&AccountData, bool)) -> Self {
        let (account_data, is_write_locked) = value;
        AccountUsage {
            key: account_data.key.clone(),
            cu_requested: account_data.cu_requested as i64,
            cu_consumed: account_data.cu_consumed as i64,
            is_write_locked,
            prioritization_fee_data: PrioFeeData::new(&account_data.vec_pf),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct PrioritizationFeesInfo {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,

    pub med_cu: Option<u64>,
    pub p75_cu: Option<u64>,
    pub p90_cu: Option<u64>,
    pub p95_cu: Option<u64>,
}

#[derive(Clone)]
pub struct TransactionAccount {
    pub key: Pubkey,
    pub is_writable: bool,
    pub is_signer: bool,
    pub is_alt: bool,
}

pub struct BlockTransactionInfo {
    pub signature: String,
    pub processed_slot: i64,
    pub is_successful: bool,
    pub cu_requested: i64,
    pub cu_consumed: i64,
    pub prioritization_fees: i64,
    pub supp_infos: String,
    pub accounts: Vec<TransactionAccount>,
}

pub struct BlockInfo {
    pub block_hash: String,
    pub slot: i64,
    pub leader_identity: Option<String>,
    pub successful_transactions: i64,
    pub processed_transactions: i64,
    pub total_cu_used: i64,
    pub total_cu_requested: i64,
    pub heavily_locked_accounts: Vec<AccountUsage>,
    pub sup_info: Option<PrioritizationFeesInfo>,
    pub transactions: Vec<BlockTransactionInfo>,
}

impl BlockInfo {
    pub async fn process_versioned_message(
        atl_store: &Arc<ALTStore>,
        signature: String,
        slot: Slot,
        message: &VersionedMessage,
        prio_fees_in_block: &mut Vec<(u64, u64)>,
        writelocked_accounts: &mut HashMap<Pubkey, AccountData>,
        readlocked_accounts: &mut HashMap<Pubkey, AccountData>,
        cu_consumed: u64,
        total_cu_requested: &mut u64,
        is_vote: bool,
        is_successful: bool,
    ) -> Option<BlockTransactionInfo> {
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
        prio_fees_in_block.push((prioritization_fees, cu_consumed));
        let cu_requested =
            std::cmp::min(1_400_000, cu_requested.unwrap_or(200000 * nb_ix_except_cb));
        *total_cu_requested += cu_requested;
        if !is_vote {
            let mut accounts = message
                .static_account_keys()
                .iter().cloned()
                .enumerate()
                .map(|(index, account_pk)| TransactionAccount {
                    key: account_pk,
                    is_writable: message.is_maybe_writable(index),
                    is_signer: message.is_signer(index),
                    is_alt: false,
                })
                .collect_vec();
            if let Some(atl_messages) = message.address_table_lookups() {
                for atl_message in atl_messages {
                    let atl_acc = atl_message.account_key;
                    let mut atl_accs = atl_store
                        .get_accounts(
                            &atl_acc,
                            &atl_message.writable_indexes,
                            &atl_message.readonly_indexes,
                        )
                        .await;
                    accounts.append(&mut atl_accs);
                }
            }

            for writable_account in accounts
                .iter()
                .filter(|x| x.is_writable)
                .map(|x| x.key)
            {
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
                                key: fd_bs58::encode_32(writable_account),
                                cu_consumed,
                                cu_requested,
                                vec_pf: vec![prioritization_fees],
                            },
                        );
                    }
                }
            }

            for readable_account in accounts
                .iter()
                .filter(|x| !x.is_writable)
                .map(|x| x.key)
            {
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
                                key: fd_bs58::encode_32(readable_account),
                                cu_consumed,
                                cu_requested,
                                vec_pf: vec![prioritization_fees],
                            },
                        );
                    }
                }
            }

            Some(BlockTransactionInfo {
                signature,
                processed_slot: slot as i64,
                is_successful,
                cu_requested: cu_requested as i64,
                cu_consumed: cu_consumed as i64,
                prioritization_fees: prioritization_fees as i64,
                supp_infos: String::new(),
                accounts,
            })
        } else {
            None
        }
    }

    pub fn calculate_account_usage(
        writelocked_accounts: &HashMap<Pubkey, AccountData>,
        readlocked_accounts: &HashMap<Pubkey, AccountData>,
    ) -> Vec<AccountUsage> {
        let mut accounts = writelocked_accounts
            .iter()
            .map(|(_, data)| AccountUsage::from((data, true)))
            .collect_vec();

        let mut heavily_readlocked_accounts = readlocked_accounts
            .iter()
            .map(|(_, data)| AccountUsage::from((data, false)))
            .collect_vec();
        accounts.append(&mut heavily_readlocked_accounts);
        accounts
    }

    pub fn calculate_supp_info(
        prio_fees_in_block: &mut Vec<(u64, u64)>,
    ) -> Option<PrioritizationFeesInfo> {
        if !prio_fees_in_block.is_empty() {
            // get stats by transaction
            prio_fees_in_block.sort_by(|a, b| a.0.cmp(&b.0));
            let median_index = prio_fees_in_block.len() / 2;
            let p75_index = prio_fees_in_block.len() * 75 / 100;
            let p90_index = prio_fees_in_block.len() * 90 / 100;
            let p_min = prio_fees_in_block[0].0;
            let p_median = prio_fees_in_block[median_index].0;
            let p_75 = prio_fees_in_block[p75_index].0;
            let p_90 = prio_fees_in_block[p90_index].0;
            let p_max = prio_fees_in_block.last().map(|x| x.0).unwrap_or_default();

            let mut med_cu = None;
            let mut p75_cu = None;
            let mut p90_cu = None;
            let mut p95_cu = None;

            // get stats by CU
            let cu_sum: u64 = prio_fees_in_block.iter().map(|x| x.1).sum();
            let mut agg: u64 = 0;
            for (prio, cu) in prio_fees_in_block {
                agg = agg + *cu;
                if med_cu.is_none() && agg > (cu_sum as f64 * 0.5) as u64 {
                    med_cu = Some(*prio);
                }
                if p75_cu.is_none() && agg > (cu_sum as f64 * 0.75) as u64 {
                    p75_cu = Some(*prio)
                }
                if p90_cu.is_none() && agg > (cu_sum as f64 * 0.9) as u64 {
                    p90_cu = Some(*prio);
                }
                if p95_cu.is_none() && agg > (cu_sum as f64 * 0.95) as u64 {
                    p95_cu = Some(*prio)
                }
            }

            Some(PrioritizationFeesInfo {
                p_min,
                p_median,
                p_75,
                p_90,
                p_max,
                med_cu,
                p75_cu,
                p90_cu,
                p95_cu,
            })
        } else {
            None
        }
    }

    pub async fn new(
        atl_store: Arc<ALTStore>,
        block: &yellowstone_grpc_proto_original::prelude::SubscribeUpdateBlock,
    ) -> BlockInfo {
        let block_hash = block.blockhash.clone();
        let _span = tracing::debug_span!("map_block_info", block_hash = block_hash);
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
        let mut lookup_tables = HashSet::new();
        let sigs_and_messages = block
            .transactions
            .iter()
            .filter_map(|transaction| {
                let Some(tx) = &transaction.transaction else {
                    return None;
                };

                let Some(message) = &tx.message else {
                    return None;
                };

                let Some(header) = &message.header else {
                    return None;
                };

                let Some(meta) = &transaction.meta else {
                    return None;
                };

                let signature = fd_bs58::encode_64(&tx.signatures[0]);

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
                            let account_key = Pubkey::new_from_array(bytes);
                            lookup_tables.insert(account_key);
                            MessageAddressTableLookup {
                                account_key,
                                writable_indexes: table.writable_indexes,
                                readonly_indexes: table.readonly_indexes,
                            }
                        })
                        .collect(),
                });
                Some((signature, message, meta, transaction.is_vote))
            })
            .collect_vec();

        atl_store
            .start_loading_missing_alts(&lookup_tables.iter().collect_vec())
            .await;

        let mut block_transactions = vec![];
        for (signature, message, meta, is_vote) in sigs_and_messages {
            let tx = Self::process_versioned_message(
                &atl_store,
                signature,
                slot,
                &message,
                &mut prio_fees_in_block,
                &mut writelocked_accounts,
                &mut readlocked_accounts,
                meta.compute_units_consumed.unwrap_or(0),
                &mut total_cu_requested,
                is_vote,
                meta.err.is_none(),
            )
            .await;
            if let Some(tx) = tx {
                block_transactions.push(tx);
            }
        }

        let heavily_locked_accounts =
            Self::calculate_account_usage(&writelocked_accounts, &readlocked_accounts);

        let sup_info = Self::calculate_supp_info(&mut prio_fees_in_block);

        BlockInfo {
            block_hash,
            slot: slot as i64,
            leader_identity,
            successful_transactions: successful_transactions as i64,
            processed_transactions: processed_transactions as i64,
            total_cu_used,
            total_cu_requested: total_cu_requested as i64,
            heavily_locked_accounts,
            sup_info,
            transactions: block_transactions,
        }
    }
}
