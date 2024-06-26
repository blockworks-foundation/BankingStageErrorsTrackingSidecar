
-- for initial database setup start with init-database.sql

-- note "STORAGE" option in column definition requires PostgreSQL 16 or later

CREATE SCHEMA banking_stage_results_2;

CREATE TABLE banking_stage_results_2.transactions(
	transaction_id bigserial,
	signature varchar(88) NOT NULL,
	PRIMARY KEY (transaction_id) INCLUDE(signature),
	UNIQUE(signature) INCLUDE (transaction_id)
);


CREATE TABLE banking_stage_results_2.transaction_infos (
                                                           transaction_id BIGINT PRIMARY KEY,
                                                           processed_slot BIGINT NOT NULL,
                                                           is_successful BOOL NOT NULL,
                                                           cu_requested BIGINT NOT NULL,
                                                           cu_consumed BIGINT NOT NULL,
                                                           prioritization_fees BIGINT NOT NULL,
                                                           supp_infos text
);
-- page layout: move supp_infos to toast; everything else should stay on main
ALTER TABLE banking_stage_results_2.transaction_infos SET (toast_tuple_target=128);
ALTER TABLE banking_stage_results_2.transaction_infos ALTER COLUMN supp_infos SET STORAGE extended;


CREATE TABLE banking_stage_results_2.errors (
                                                error_code int primary key,
                                                error_text text NOT NULL
);
-- page layout: keep everything on main
ALTER TABLE banking_stage_results_2.errors ALTER COLUMN error_text SET STORAGE main;


CREATE TABLE banking_stage_results_2.transaction_slot (
                                                          transaction_id BIGINT,
                                                          slot BIGINT NOT NULL,
                                                          error_code INT NOT NULL,
                                                          count INT NOT NULL,
                                                          utc_timestamp TIMESTAMP NOT NULL,
                                                          PRIMARY KEY (transaction_id, slot, error_code)
);
-- page layout: keep everything on main (note: TIMESTAMP uses storage plain)
ALTER TABLE banking_stage_results_2.transaction_slot SET (toast_tuple_target=128);
CREATE INDEX idx_transaction_slot_timestamp ON banking_stage_results_2.transaction_slot(utc_timestamp);
CREATE INDEX idx_transaction_slot_slot ON banking_stage_results_2.transaction_slot(slot);


CREATE TABLE banking_stage_results_2.blocks (
                                                slot BIGINT PRIMARY KEY,
                                                successful_transactions BIGINT NOT NULL,
                                                processed_transactions BIGINT NOT NULL,
                                                total_cu_used BIGINT NOT NULL,
                                                total_cu_requested BIGINT NOT NULL,
                                                block_hash varchar(44) NOT NULL,
                                                leader_identity varchar(44) NOT NULL,
                                                supp_infos text
);
-- page layout: blockhash is frequently used
ALTER TABLE banking_stage_results_2.blocks SET (toast_tuple_target=200);
ALTER TABLE banking_stage_results_2.blocks ALTER COLUMN block_hash SET STORAGE main;
CREATE INDEX idx_blocks_block_hash ON banking_stage_results_2.blocks(block_hash);


CREATE TABLE banking_stage_results_2.accounts(
                                                 acc_id bigserial PRIMARY KEY,
                                                 account_key varchar(44) NOT NULL,
                                                 UNIQUE (account_key)
);
-- page layout: rows are small and must store in main; compression is okey
ALTER TABLE banking_stage_results_2.accounts ALTER COLUMN account_key SET STORAGE main;
-- ALTER TABLE banking_stage_results_2.transactions SET (toast_tuple_target=4080);


CREATE TABLE banking_stage_results_2.accounts_map_transaction(
                                                                 transaction_id BIGINT NOT NULL,
                                                                 acc_id BIGINT NOT NULL,
                                                                 is_writable BOOL NOT NULL,
                                                                 is_signer BOOL NOT NULL,
                                                                 is_atl BOOL NOT NULL,
                                                                 PRIMARY KEY (transaction_id, acc_id)
);
-- page layout: very small rows, keep everything on main
ALTER TABLE banking_stage_results_2.accounts_map_transaction SET (toast_tuple_target=128);


CREATE TABLE banking_stage_results_2.accounts_map_blocks (
                                                             acc_id BIGINT NOT NULL,
                                                             slot BIGINT NOT NULL,
                                                             is_write_locked BOOL NOT NULL,
                                                             total_cu_consumed BIGINT NOT NULL,
                                                             total_cu_requested BIGINT NOT NULL,
                                                             prioritization_fees_info text NOT NULL,
                                                             supp_infos text,
                                                             PRIMARY KEY (acc_id, slot, is_write_locked)
);
-- page layout: move prioritization_fees_info and supp_infos to toast; everything else should stay on main
ALTER TABLE banking_stage_results_2.accounts_map_blocks SET (toast_tuple_target=128);
ALTER TABLE banking_stage_results_2.accounts_map_blocks ALTER COLUMN prioritization_fees_info SET STORAGE extended;
ALTER TABLE banking_stage_results_2.accounts_map_blocks ALTER COLUMN supp_infos SET STORAGE extended;
CREATE INDEX idx_accounts_map_blocks_slot ON banking_stage_results_2.accounts_map_blocks(slot);


CREATE UNLOGGED TABLE banking_stage_results_2.accounts_map_transaction_latest(
                                                                        acc_id BIGINT PRIMARY KEY,
    -- max 120 int8 ids (see LIMIT_LATEST_TXS_PER_ACCOUNT)
                                                                        tx_ids BIGINT[] NOT NULL
);
-- page layout: tx_ids is the only relevant data must remain in main; tx_ids size is intentionally limited to fit a single page; compression is okey
-- do not limit tuple size, it is not relevant
ALTER TABLE banking_stage_results_2.accounts_map_transaction_latest SET (toast_tuple_target=4080);
ALTER TABLE banking_stage_results_2.accounts_map_transaction_latest ALTER COLUMN tx_ids SET STORAGE main;


INSERT INTO banking_stage_results_2.errors (error_text, error_code) VALUES
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


CREATE OR REPLACE FUNCTION array_dedup_append(base bigint[], append bigint[], n_limit int)
    RETURNS bigint[]
AS $$
DECLARE
    tmplist  bigint[];
    el		bigint;
    len int;
BEGIN
    tmplist := base;
    FOREACH el IN ARRAY append LOOP
            tmplist := array_remove(tmplist, el);
            tmplist := tmplist || el;
        END LOOP;

    len := CARDINALITY(tmplist);
    RETURN  tmplist[(len + 1 - n_limit):];
END
$$ LANGUAGE plpgsql IMMUTABLE CALLED ON NULL INPUT;


ALTER TABLE banking_stage_results_2.accounts
    SET (
        autovacuum_vacuum_scale_factor=0,
        autovacuum_vacuum_threshold=100,
        autovacuum_vacuum_insert_scale_factor=0,
        autovacuum_vacuum_insert_threshold=100,
        autovacuum_analyze_scale_factor=0,
        autovacuum_analyze_threshold=100
        );

ALTER TABLE banking_stage_results_2.transactions
    SET (
        autovacuum_vacuum_scale_factor=0,
        autovacuum_vacuum_threshold=1000,
        autovacuum_vacuum_insert_scale_factor=0,
        autovacuum_vacuum_insert_threshold=1000,
        autovacuum_analyze_scale_factor=0,
        autovacuum_analyze_threshold=1000
        );

ALTER TABLE banking_stage_results_2.accounts_map_transaction
    SET (
        autovacuum_vacuum_scale_factor=0,
        autovacuum_vacuum_threshold=10000,
        autovacuum_vacuum_insert_scale_factor=0,
        autovacuum_vacuum_insert_threshold=10000,
        autovacuum_analyze_scale_factor=0,
        autovacuum_analyze_threshold=10000
        );

-- not written ATM
ALTER TABLE banking_stage_results_2.accounts_map_transaction_latest
    SET (
        autovacuum_vacuum_scale_factor=0,
        autovacuum_vacuum_threshold=100,
        autovacuum_vacuum_insert_scale_factor=0,
        autovacuum_vacuum_insert_threshold=100,
        autovacuum_analyze_scale_factor=0,
        autovacuum_analyze_threshold=100
        );

ALTER TABLE banking_stage_results_2.accounts_map_transaction_latest SET (FILLFACTOR=90);
ALTER INDEX banking_stage_results_2.accounts_map_transaction_latest_pkey SET (FILLFACTOR=50);

