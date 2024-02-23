-- setup new postgresql database; tested with PostgreSQL 15

-- CREATE DATABASE the_banking_stage_db
-- run migration.sql

-- setup sidecar user
GRANT CONNECT ON DATABASE the_banking_stage_db TO bankingstage_sidecar;
ALTER USER bankingstage_sidecar CONNECTION LIMIT 10;
GRANT USAGE ON SCHEMA banking_stage_results_2 TO bankingstage_sidecar;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA banking_stage_results_2 TO bankingstage_sidecar;
ALTER DEFAULT PRIVILEGES IN SCHEMA banking_stage_results_2 GRANT ALL PRIVILEGES ON TABLES TO bankingstage_sidecar;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA banking_stage_results_2 TO bankingstage_sidecar;
ALTER DEFAULT PRIVILEGES IN SCHEMA banking_stage_results_2 GRANT USAGE ON SEQUENCES TO bankingstage_sidecar;


-- setup query_user
GRANT CONNECT ON DATABASE the_banking_stage_db TO query_user;
ALTER USER query_user CONNECTION LIMIT 5;
GRANT USAGE ON SCHEMA banking_stage_results_2 TO query_user;
GRANT SELECT ON ALL TABLES in SCHEMA banking_stage_results_2 TO query_user;


-- setup bankingstage_dashboard
GRANT CONNECT ON DATABASE the_banking_stage_db TO bankingstage_dashboard;
ALTER USER bankingstage_sidecar CONNECTION LIMIT 10;
GRANT USAGE ON SCHEMA banking_stage_results_2 TO bankingstage_dashboard;
GRANT SELECT ON ALL TABLES in SCHEMA banking_stage_results_2 TO bankingstage_dashboard;
