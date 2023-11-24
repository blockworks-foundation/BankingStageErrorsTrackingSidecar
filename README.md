# TO INSTALL POSTGRES SCHEMA AND DATABASE

sudo -u postgres psql postgres
###### in postgres
create data
create database mangolana;
grant all privileges on database mangolana to galactus;


psql -d mangolana < migration.sql

export PG_CONFIG="host=localhost dbname=mangolana user=galactus password=test sslmode=disable" 

### give rights to user

GRANT ALL PRIVILEGES ON DATABASE mangolana TO galactus;
GRANT ALL PRIVILEGES ON SCHEMA banking_stage_results TO galactus;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA banking_stage_results TO galactus;
ALTER DEFAULT PRIVILEGES IN SCHEMA banking_stage_results GRANT ALL PRIVILEGES  ON TABLES TO galactus;