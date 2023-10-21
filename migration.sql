CREATE SCHEMA banking_stage_results;

CREATE TYPE error AS (
    error   text,
    slot    BIGINT,
    count   BIGINT
);

CREATE TYPE ACCOUNT_USED AS (
  account CHAR(44),
  r_w     CHAR
);

CREATE TYPE ACCOUNT_USE_COUNT AS (
  account CHAR(44),
  cu     BIGINT
);

CREATE TABLE banking_stage_results.transaction_infos (
  signature CHAR(88) PRIMARY KEY,
  message text,
  errors ERROR[],
  is_executed BOOL,
  is_confirmed BOOL,
  first_notification_slot BIGINT NOT NULL,
  cu_requested BIGINT,
  prioritization_fees BIGINT,
  utc_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
  accounts_used ACCOUNT_USED[],
  processed_slot BIGINT
);

CREATE TABLE banking_stage_results.blocks (
  block_hash CHAR(44) PRIMARY KEY,
  slot BIGINT,
  leader_identity CHAR(44),
  successful_transactions BIGINT,
  banking_stage_errors BIGINT,
  processed_transactions BIGINT,
  total_cu_used BIGINT,
  total_cu_requested BIGINT,
  heavily_writelocked_accounts ACCOUNT_USE_COUNT[]
);

CREATE TABLE banking_stage_results.accounts (
  account CHAR(44) PRIMARY KEY,
  account_borrow_outstanding BIGINT,
  account_in_use_write BIGINT,
  account_in_use_read BIGINT,
  would_exceed_maximum_account_cost_limit BIGINT
);


CREATE INDEX idx_blocks_slot ON banking_stage_results.blocks(slot);
-- optional
CLUSTER banking_stage_results.blocks using idx_blocks_slot;
VACUUM FULL banking_stage_results.blocks;

CREATE INDEX idx_transaction_infos_timestamp ON banking_stage_results.transaction_infos(utc_timestamp);
-- optional
CLUSTER banking_stage_results.transaction_infos using idx_transaction_infos_timestamp;
VACUUM FULL banking_stage_results.transaction_infos;
