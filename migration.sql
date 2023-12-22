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

CREATE TABLE banking_stage_results_2.errors (
  error_code int primary key,
  error_text text
);

CREATE TABLE banking_stage_results_2.transaction_slot (
  transaction_id BIGINT,
  slot BIGINT,
  error_code INT REFERENCES banking_stage_results_2.errors(error_code),
  count INT,
  utc_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (transaction_id, slot, error_code)
);

CREATE INDEX idx_transaction_slot_timestamp ON banking_stage_results_2.transaction_slot(utc_timestamp);
CREATE INDEX idx_transaction_slot_slot ON banking_stage_results_2.transaction_slot(slot);

CREATE TABLE banking_stage_results_2.blocks (
  slot BIGINT PRIMARY KEY,
  block_hash CHAR(44),
  leader_identity CHAR(44),
  successful_transactions BIGINT,
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

CREATE TABLE banking_stage_results_2.accounts_map_transaction(
  acc_id BIGINT,
  transaction_id BIGINT,
  is_writable BOOL,
  is_signer BOOL,
  PRIMARY KEY (acc_id, transaction_id)
);

ALTER TABLE banking_stage_results_2.accounts_map_transaction ADD COLUMN is_atl BOOL;

CREATE INDEX accounts_map_transaction_acc_id ON banking_stage_results_2.accounts_map_transaction(acc_id);
CREATE INDEX accounts_map_transaction_transaction_id ON banking_stage_results_2.accounts_map_transaction(transaction_id);

CREATE INDEX idx_blocks_block_hash ON banking_stage_results_2.blocks(block_hash);

CREATE TABLE banking_stage_results_2.accounts_map_blocks (
  acc_id BIGINT,
  slot BIGINT,
  is_write_locked BOOL,
  total_cu_consumed BIGINT,
  total_cu_requested BIGINT,
  prioritization_fees_info text,
  supp_infos text,
  PRIMARY KEY (acc_id, slot, is_write_locked)
);
CREATE INDEX idx_accounts_map_blocks_slot ON banking_stage_results_2.accounts_map_blocks(slot);

insert into banking_stage_results_2.errors (error_text, error_code) VALUES 
        ('AccountBorrowOutstanding', 0),
        ('AccountInUse', 1),
        ('AccountLoadedTwice', 2),
        ('AccountNotFound', 3),
        ('AddressLookupTableNotFound', 4),
        ('AlreadyProcessed', 5),
        ('BlockhashNotFound', 6),
        ('CallChainTooDeep', 7),
        ('ClusterMaintenance', 8),
        ('DuplicateInstruction', 9),
        ('InstructionError', 10),
        ('InsufficientFundsForFee', 11),
        ('InsufficientFundsForRent', 12),
        ('InvalidAccountForFee', 13),
        ('InvalidAccountIndex', 14),
        ('InvalidAddressLookupTableData', 15),
        ('InvalidAddressLookupTableIndex', 16),
        ('InvalidAddressLookupTableOwner', 17),
        ('InvalidLoadedAccountsDataSizeLimit', 18),
        ('InvalidProgramForExecution', 19),
        ('InvalidRentPayingAccount', 20),
        ('InvalidWritableAccount', 21),
        ('MaxLoadedAccountsDataSizeExceeded', 22),
        ('MissingSignatureForFee', 23),
        ('ProgramAccountNotFound', 24),
        ('ResanitizationNeeded', 25),
        ('SanitizeFailure', 26),
        ('SignatureFailure', 27),
        ('TooManyAccountLocks', 28),
        ('UnbalancedTransaction', 29),
        ('UnsupportedVersion', 30),
        ('WouldExceedAccountDataBlockLimit', 31),
        ('WouldExceedAccountDataTotalLimit', 32),
        ('WouldExceedMaxAccountCostLimit', 33),
        ('WouldExceedMaxBlockCostLimit', 34),
        ('WouldExceedMaxVoteCostLimit', 35);

CLUSTER banking_stage_results_2.blocks using blocks_pkey;
VACUUM FULL banking_stage_results_2.blocks;
-- optional
CLUSTER banking_stage_results_2.transaction_slot using idx_transaction_slot_timestamp;
VACUUM FULL banking_stage_results_2.transaction_slot;

ALTER TABLE banking_stage_results_2.transactions ALTER COLUMN signature TYPE TEXT;

CLUSTER banking_stage_results_2.accounts_map_transaction using accounts_map_transaction_pkey;

