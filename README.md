# BankingStage Sidecar
This is a sidecar application for the BankingStage project. It is responsible for importing data from the Solana blockchain into the PostgreSQL database.
Data is retrieved via Solana RPC and Geysers gRPC API.

## Database Configuration
### Database Roles
* `bankingstage_sidecar` - write access to the database for the sidecar importer
* `bankingstage_dashboard` - read-only access to the database for the dashboard web application
* `query_user` - group for read-only access to the database intended for human user interaction with database

```sql
CREATE USER some_user_in_group_query_user PASSWORD 'test';
GRANT query_user TO some_user_in_group_query_user;
```

### Configure sidecar PostgreSQL connection
export PG_CONFIG="host=localhost dbname=the_banking_stage_db user=some_user_in_group_query_user password=test sslmode=disable"

### Database Schema
The database schema is defined in the [migration.sql](migration.sql) file.
For new database installations start with the [init-database.sql](init-database.sql) file.
Required is a PostgreSQL database (tested version 15).

