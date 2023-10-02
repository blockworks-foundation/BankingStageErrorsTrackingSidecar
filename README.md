# TO INSTALL POSTGRES SCHEMA AND DATABASE

sudo -u postgres psql postgres
###### in postgres
create data
create database mangolana;
grant all privileges on database mangolana to galactus;


psql -d mangolana < migration.sql

export PG_CONFIG="host=localhost dbname=mangolana user=username password=password sslmode=disable" 
