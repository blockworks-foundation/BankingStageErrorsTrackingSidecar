CREATE SCHEMA banking_stage_results_2;

CREATE TABLE banking_stage_results_2.transactions(
  signature char(88) primary key,
  transaction_id bigserial,
  UNIQUE(transaction_id)
);

CREATE TABLE banking_stage_results_2.transaction_infos (
  transaction_id BIGINT PRIMARY KEY,
  processed_slot BIGINT,
  is_successful BOOL,
  cu_requested BIGINT,
  cu_consumed BIGINT,
  prioritization_fees BIGINT,
  supp_infos text
);

CREATE TABLE banking_stage_results_2.transaction_slot (
  transaction_id BIGINT,
  slot BIGINT NOT NULL,
  error INT,
  count INT,
  utc_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (transaction_id, slot, error)
);

CREATE INDEX idx_transaction_slot_timestamp ON banking_stage_results_2.transaction_slot(utc_timestamp);

CREATE TABLE banking_stage_results_2.blocks (
  slot BIGINT PRIMARY KEY,
  block_hash CHAR(44),
  leader_identity CHAR(44),
  successful_transactions BIGINT,
  banking_stage_errors BIGINT,
  processed_transactions BIGINT,
  total_cu_used BIGINT,
  total_cu_requested BIGINT,
  supp_infos text
);

CREATE TABLE banking_stage_results_2.accounts(
	acc_id bigserial primary key,
	account_key text,
	UNIQUE (account_key)
);

CREATE INDEX idx_account_key ON banking_stage_results_2.accounts(account_key);

CREATE TABLE banking_stage_results_2.accounts_map_transaction(
  acc_id BIGINT,
  transaction_id BIGINT,
  is_writable BOOL,
  is_signer BOOL,
  PRIMARY KEY (acc_id, transaction_id)
);

CREATE TABLE banking_stage_results_2.accounts_map_blocks (
  acc_id BIGINT,
  slot BIGINT,
  is_write_locked BOOL,
  total_cu_consumed BIGINT,
  total_cu_requested BIGINT,
  prioritization_fees_info text,
  supp_infos text,
  PRIMARY KEY (acc_id, slot, is_writable)
);

CREATE TABLE banking_stage_results_2.errors (
  error_code int primary key,
  error text
);

insert into banking_stage_results_2.errors (error, error_code) VALUES 
        ('TransactionError::AccountBorrowOutstanding', 0),
        ('TransactionError::AccountInUse', 1),
        ('TransactionError::AccountLoadedTwice', 2),
        ('TransactionError::AccountNotFound', 3),
        ('TransactionError::AddressLookupTableNotFound', 4),
        ('TransactionError::AlreadyProcessed', 5),
        ('TransactionError::BlockhashNotFound', 6),
        ('TransactionError::CallChainTooDeep', 7),
        ('TransactionError::ClusterMaintenance', 8),
        ('TransactionError::DuplicateInstruction', 9),
        ('TransactionError::InstructionError', 10),
        ('TransactionError::InsufficientFundsForFee', 11),
        ('TransactionError::InsufficientFundsForRent', 12),
        ('TransactionError::InvalidAccountForFee', 13),
        ('TransactionError::InvalidAccountIndex', 14),
        ('TransactionError::InvalidAddressLookupTableData', 15),
        ('TransactionError::InvalidAddressLookupTableIndex', 16),
        ('TransactionError::InvalidAddressLookupTableOwner', 17),
        ('TransactionError::InvalidLoadedAccountsDataSizeLimit', 18),
        ('TransactionError::InvalidProgramForExecution', 19),
        ('TransactionError::InvalidRentPayingAccount', 20),
        ('TransactionError::InvalidWritableAccount', 21),
        ('TransactionError::MaxLoadedAccountsDataSizeExceeded', 22),
        ('TransactionError::MissingSignatureForFee', 23),
        ('TransactionError::ProgramAccountNotFound', 24),
        ('TransactionError::ResanitizationNeeded', 25),
        ('TransactionError::SanitizeFailure', 26),
        ('TransactionError::SignatureFailure', 27),
        ('TransactionError::TooManyAccountLocks', 28),
        ('TransactionError::UnbalancedTransaction', 29),
        ('TransactionError::UnsupportedVersion', 30),
        ('TransactionError::WouldExceedAccountDataBlockLimit', 31),
        ('TransactionError::WouldExceedAccountDataTotalLimit', 32),
        ('TransactionError::WouldExceedMaxAccountCostLimit', 33),
        ('TransactionError::WouldExceedMaxBlockCostLimit', 34),
        ('TransactionError::WouldExceedMaxVoteCostLimit', 35);

-- optional
CLUSTER banking_stage_results_2.blocks using blocks_pkey;
VACUUM FULL banking_stage_results_2.blocks;
-- optional
CLUSTER banking_stage_results_2.transaction_slot using idx_transaction_slot_timestamp;
VACUUM FULL banking_stage_results_2.transaction_slot;