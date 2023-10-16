CREATE SCHEMA banking_stage_results;

CREATE TABLE banking_stage_results.transaction_infos (
  signature CHAR(88) NOT NULL,
  message text,
  errors text,
  is_executed BOOL,
  is_confirmed BOOL,
  first_notification_slot BIGINT NOT NULL,
  cu_requested BIGINT,
  prioritization_fees BIGINT
);
