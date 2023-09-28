use std::{collections::HashMap, hash::Hash};

use solana_sdk::{slot_history::Slot, transaction::Transaction, transaction::TransactionError};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBankingTransactionResults;

fn convert_transaction_error_into_int(error: &TransactionError)-> u8 {
    match error {
        TransactionError::AccountBorrowOutstanding=>0,
        TransactionError::AccountInUse=>1,
        TransactionError::AccountLoadedTwice=>2,
        TransactionError::AccountNotFound=>3,
        TransactionError::AddressLookupTableNotFound=>4,
        TransactionError::AlreadyProcessed=>5,
        TransactionError::BlockhashNotFound=>6,
        TransactionError::CallChainTooDeep=>7,
        TransactionError::ClusterMaintenance=>8,
        TransactionError::DuplicateInstruction(_)=>9,
        TransactionError::InstructionError(_, _) => 10,
        TransactionError::InsufficientFundsForFee=>11,
        TransactionError::InsufficientFundsForRent { .. } => 12,
        TransactionError::InvalidAccountForFee => 13,
        TransactionError::InvalidAccountIndex => 14,
        TransactionError::InvalidAddressLookupTableData=>15,
        TransactionError::InvalidAddressLookupTableIndex=>16,
        TransactionError::InvalidAddressLookupTableOwner=>17,
        TransactionError::InvalidLoadedAccountsDataSizeLimit=>18,
        TransactionError::InvalidProgramForExecution=>19,
        TransactionError::InvalidRentPayingAccount=>20,
        TransactionError::InvalidWritableAccount=>21,
        TransactionError::MaxLoadedAccountsDataSizeExceeded=>22,
        TransactionError::MissingSignatureForFee=>23,
        TransactionError::ProgramAccountNotFound=>24,
        TransactionError::ResanitizationNeeded=>25,
        TransactionError::SanitizeFailure=>26,
        TransactionError::SignatureFailure=>27,
        TransactionError::TooManyAccountLocks=>28,
        TransactionError::UnbalancedTransaction=>29,
        TransactionError::UnsupportedVersion=>30,
        TransactionError::WouldExceedAccountDataBlockLimit=>31,
        TransactionError::WouldExceedAccountDataTotalLimit=>32,
        TransactionError::WouldExceedMaxAccountCostLimit=>33,
        TransactionError::WouldExceedMaxBlockCostLimit=>34,
        TransactionError::WouldExceedMaxVoteCostLimit=>35,
    }
}

#[derive(Clone, PartialEq)]
pub struct ErrorKey {
    error : TransactionError,
    slot: Slot,
}

impl Hash for ErrorKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let tmp = convert_transaction_error_into_int(&self.error);
        tmp.hash(state);
        self.slot.hash(state);
    }
}

impl Eq for ErrorKey {

}

pub struct TransactionInfo {
    pub signature : String,
    pub transaction_message: Option<Transaction>,
    pub errors: HashMap<ErrorKey, usize>,
    pub is_executed: bool,
    pub is_confirmed : bool,
    pub block_height : Option<u64>,
    pub first_notification_slot: u64,
}

impl TransactionInfo {
    pub fn new(signature: String, first_notification_slot: Slot) -> Self {
        Self { 
            signature, 
            transaction_message: None,
            errors: HashMap::new(),
            is_executed: false,
            is_confirmed: false,
            first_notification_slot,
            block_height: None,
        }
    }

    pub fn add_notification(&mut self, notification: &SubscribeUpdateBankingTransactionResults) {
        match &notification.error {
            Some(error) => {
                let slot = notification.slot;
                let error: TransactionError = bincode::deserialize(&error.err).unwrap();
                let key = ErrorKey {
                    error,
                    slot,
                };
                match self.errors.get_mut(&key) {
                    Some(x) => {
                        *x = *x + 1;
                    },
                    None => {
                        self.errors.insert(key, 1);
                    }
                }
            },
            None => {
                self.is_executed = true;
            }
        }
    }
}