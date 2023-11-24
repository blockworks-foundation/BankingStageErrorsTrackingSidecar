use std::{collections::HashMap, hash::Hash, str::FromStr};

use chrono::{DateTime, Utc};
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
    transaction::{TransactionError, VersionedTransaction},
};
use yellowstone_grpc_proto::prelude::{
    SubscribeUpdateBankingTransactionResults, SubscribeUpdateTransactionInfo,
};

fn convert_transaction_error_into_int(error: &TransactionError) -> u8 {
    match error {
        TransactionError::AccountBorrowOutstanding => 0,
        TransactionError::AccountInUse => 1,
        TransactionError::AccountLoadedTwice => 2,
        TransactionError::AccountNotFound => 3,
        TransactionError::AddressLookupTableNotFound => 4,
        TransactionError::AlreadyProcessed => 5,
        TransactionError::BlockhashNotFound => 6,
        TransactionError::CallChainTooDeep => 7,
        TransactionError::ClusterMaintenance => 8,
        TransactionError::DuplicateInstruction(_) => 9,
        TransactionError::InstructionError(_, _) => 10,
        TransactionError::InsufficientFundsForFee => 11,
        TransactionError::InsufficientFundsForRent { .. } => 12,
        TransactionError::InvalidAccountForFee => 13,
        TransactionError::InvalidAccountIndex => 14,
        TransactionError::InvalidAddressLookupTableData => 15,
        TransactionError::InvalidAddressLookupTableIndex => 16,
        TransactionError::InvalidAddressLookupTableOwner => 17,
        TransactionError::InvalidLoadedAccountsDataSizeLimit => 18,
        TransactionError::InvalidProgramForExecution => 19,
        TransactionError::InvalidRentPayingAccount => 20,
        TransactionError::InvalidWritableAccount => 21,
        TransactionError::MaxLoadedAccountsDataSizeExceeded => 22,
        TransactionError::MissingSignatureForFee => 23,
        TransactionError::ProgramAccountNotFound => 24,
        TransactionError::ResanitizationNeeded => 25,
        TransactionError::SanitizeFailure => 26,
        TransactionError::SignatureFailure => 27,
        TransactionError::TooManyAccountLocks => 28,
        TransactionError::UnbalancedTransaction => 29,
        TransactionError::UnsupportedVersion => 30,
        TransactionError::WouldExceedAccountDataBlockLimit => 31,
        TransactionError::WouldExceedAccountDataTotalLimit => 32,
        TransactionError::WouldExceedMaxAccountCostLimit => 33,
        TransactionError::WouldExceedMaxBlockCostLimit => 34,
        TransactionError::WouldExceedMaxVoteCostLimit => 35,
    }
}

#[derive(Clone, PartialEq)]
pub struct ErrorKey {
    pub error: TransactionError,
    pub slot: Slot,
}

impl ToString for ErrorKey {
    fn to_string(&self) -> String {
        self.error.to_string() + "-" + self.slot.to_string().as_str()
    }
}

impl Hash for ErrorKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let tmp = convert_transaction_error_into_int(&self.error);
        tmp.hash(state);
        self.slot.hash(state);
    }
}

impl Eq for ErrorKey {}

#[derive(Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub errors: HashMap<ErrorKey, usize>,
    pub is_executed: bool,
    pub is_confirmed: bool,
    pub first_notification_slot: u64,
    pub cu_requested: Option<u64>,
    pub prioritization_fees: Option<u64>,
    pub utc_timestamp: DateTime<Utc>,
    pub account_used: HashMap<Pubkey, char>,
    pub processed_slot: Option<Slot>,
}

impl TransactionInfo {
    pub fn new(notification: &SubscribeUpdateBankingTransactionResults) -> Self {
        let mut errors = HashMap::new();
        let is_executed = notification.error.is_none();
        // Get time
        let utc_timestamp = Utc::now();

        match &notification.error {
            Some(e) => {
                let error: TransactionError = bincode::deserialize(&e.err).unwrap();
                let key = ErrorKey {
                    error,
                    slot: notification.slot,
                };
                errors.insert(key, 1);
            }
            None => {}
        };

        let account_used = notification
            .accounts
            .iter()
            .map(|x| {
                (
                    Pubkey::from_str(&x.account).unwrap(),
                    if x.is_writable { 'w' } else { 'r' },
                )
            })
            .collect();
        Self {
            signature: notification.signature.clone(),
            errors,
            is_executed,
            is_confirmed: false,
            first_notification_slot: notification.slot,
            cu_requested: None,
            prioritization_fees: None,
            utc_timestamp,
            account_used,
            processed_slot: None,
        }
    }

    pub fn add_notification(&mut self, notification: &SubscribeUpdateBankingTransactionResults) {
        match &notification.error {
            Some(error) => {
                let slot = notification.slot;
                let error: TransactionError = bincode::deserialize(&error.err).unwrap();
                let key = ErrorKey { error, slot };
                match self.errors.get_mut(&key) {
                    Some(x) => {
                        *x += 1;
                    }
                    None => {
                        self.errors.insert(key, 1);
                    }
                }
            }
            None => {}
        }
    }

    pub fn add_rpc_transaction(&mut self, slot: u64, transaction: &VersionedTransaction) {
        let message = &transaction.message;
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
                            return Some((units, Some(((units * 1000) / additional_fee) as u64)));
                        } else {
                            return Some((units, None));
                        }
                    }
                }
                None
            });

        let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);
        let legacy_prioritization_fees = legacy_compute_budget.map(|x| x.1).unwrap_or(None);

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

        let prioritization_fees = message
            .instructions()
            .iter()
            .find_map(|i| {
                if i.program_id(message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                        try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some(price);
                    }
                }

                None
            })
            .or(legacy_prioritization_fees);
        if let Some(cu_requested) = cu_requested {
            self.cu_requested = Some(cu_requested as u64);
        }

        if let Some(prioritization_fees) = prioritization_fees {
            self.prioritization_fees = Some(prioritization_fees);
        }
        self.is_confirmed = true;
        self.is_executed = true;
        self.processed_slot = Some(slot);
    }

    pub fn add_transaction(&mut self, transaction: &SubscribeUpdateTransactionInfo, slot: Slot) {
        let Some(transaction) = &transaction.transaction else {
            return;
        };

        let Some(message) = &transaction.message else {
            return;
        };

        let Some(header) = &message.header else {
            return;
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
                    let bytes: [u8; 32] = key.try_into().unwrap_or(Pubkey::default().to_bytes());
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
                            return Some((units, Some(((units * 1000) / additional_fee) as u64)));
                        } else {
                            return Some((units, None));
                        }
                    }
                }
                None
            });

        let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);
        let legacy_prioritization_fees = legacy_compute_budget.map(|x| x.1).unwrap_or(None);

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

        let prioritization_fees = message
            .instructions()
            .iter()
            .find_map(|i| {
                if i.program_id(message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                        try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some(price);
                    }
                }

                None
            })
            .or(legacy_prioritization_fees);
        if let Some(cu_requested) = cu_requested {
            self.cu_requested = Some(cu_requested as u64);
        }

        if let Some(prioritization_fees) = prioritization_fees {
            self.prioritization_fees = Some(prioritization_fees);
        }
        self.is_confirmed = true;
        self.is_executed = true;
        self.processed_slot = Some(slot);
    }
}
