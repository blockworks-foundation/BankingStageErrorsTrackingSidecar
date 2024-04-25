use std::{collections::HashMap, hash::Hash};
use std::rc::Rc;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use solana_sdk::transaction::TransactionError;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBankingTransactionResults;

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
}

impl ToString for ErrorKey {
    fn to_string(&self) -> String {
        self.error.to_string()
    }
}

impl Hash for ErrorKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let tmp = convert_transaction_error_into_int(&self.error);
        tmp.hash(state)
    }
}

impl ErrorKey {
    pub fn to_int(&self) -> i32 {
        convert_transaction_error_into_int(&self.error) as i32
    }
}

impl Eq for ErrorKey {}

#[derive(Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub errors: HashMap<ErrorKey, usize>,
    pub slot: u64,
    pub utc_timestamp: DateTime<Utc>,
    pub account_used: Vec<(String, bool)>,
    // local write_version used in lite-rpc
    pub write_version: u64,
}

impl TransactionInfo {
    pub fn new(notification: &SubscribeUpdateBankingTransactionResults, global_error_plugin_write_version: u64) -> Self {
        let mut errors = HashMap::new();
        // Get time
        let utc_timestamp = Utc::now();

        match &notification.error {
            Some(e) => {
                let error: TransactionError = bincode::deserialize(&e.err).unwrap();
                let key = ErrorKey { error };
                errors.insert(key, 1);
            }
            None => {}
        };

        let account_used = notification
            .accounts
            .iter()
            .map(|x| (x.account.clone(), x.is_writable))
            .collect_vec();

        Self {
            signature: notification.signature.clone().into(),
            errors,
            slot: notification.slot,
            utc_timestamp,
            account_used,
            write_version: global_error_plugin_write_version,
        }
    }

    pub fn add_notification(&mut self, notification: &SubscribeUpdateBankingTransactionResults) {
        match &notification.error {
            Some(error) => {
                let error: TransactionError = bincode::deserialize(&error.err).unwrap();
                let key = ErrorKey { error };
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
}
