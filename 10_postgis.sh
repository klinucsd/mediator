#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Create the 'template_postgis' template db
"${psql[@]}" <<- 'EOSQL'
CREATE DATABASE template_postgis IS_TEMPLATE true;
CREATE DATABASE mediator;
EOSQL

# Load PostGIS into both template_database and $POSTGRES_DB
for DB in template_postgis "$POSTGRES_DB" "mediator"; do
	echo "Loading PostGIS extensions into $DB"
	"${psql[@]}" --dbname="$DB" <<-'EOSQL'
		CREATE EXTENSION IF NOT EXISTS postgis;
		CREATE EXTENSION IF NOT EXISTS postgis_topology;
		CREATE EXTENSION IF NOT EXISTS postgis_raster;	
		-- Reconnect to update pg_setting.resetval
		-- See https://github.com/postgis/docker-postgis/issues/288
		\c
		CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
EOSQL

# Create tables for mediator
"${psql[@]}" --dbname="mediator" <<-'EOSQL' 
CREATE TABLE IF NOT EXISTS md_data_status(
    data_id SERIAL primary key,
    url VARCHAR(256) NOT NULL,
    table_name VARCHAR(36) NOT NULL ,
    status VARCHAR(16) NOT NULL,
    fetch_requested_user VARCHAR(32) NOT NULL ,
    fetch_requested_time timestamp NOT NULL default now(),
    status_updated_time timestamp default now() NOT NULL,
    last_used_time timestamp,
    notes TEXT
);

CREATE OR REPLACE VIEW md_v_data_status AS
    SELECT url, status, fetch_requested_user, fetch_requested_time, status_updated_time
      FROM md_data_status;
EOSQL


"${psql[@]}" --dbname="mediator" <<-'EOSQL' 
CREATE OR REPLACE FUNCTION md_fetch_data(url VARCHAR)
RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION 'Please invoke this function using the syntax: SELECT md_fetch_data(''<URL>'') only.';
END;
$$ LANGUAGE plpgsql;
EOSQL



"${psql[@]}" --dbname="mediator" <<-'EOSQL' 
CREATE OR REPLACE FUNCTION md_remove_data(input_string VARCHAR)
    RETURNS BOOLEAN AS $$
DECLARE
    table_to_drop VARCHAR;
BEGIN
    -- Step 1: Query md_data_status to get the table name
    BEGIN
        SELECT INTO table_to_drop table_name FROM md_data_status WHERE url = input_string;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Handle the case where no data is found
            -- RAISE EXCEPTION 'No table found for the given URL: %', input_string;
            RETURN FALSE;
    END;

    -- Step 2: Drop the table
    IF table_to_drop IS NOT NULL THEN
        EXECUTE 'DROP TABLE IF EXISTS ' || table_to_drop;
    ELSE
        RAISE EXCEPTION 'No table found for the given URL: %', input_string;
    END IF;

    -- Step 3: Delete the corresponding entry from md_data_status
    DELETE FROM md_data_status WHERE url = input_string;

    -- Step 4: Return True
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION md_mediator_error(error_message TEXT)
    RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION '%', error_message;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION md_list_data_loaders(url VARCHAR)
    RETURNS VOID AS $$
BEGIN
    RAISE EXCEPTION 'Please invoke this function using the syntax "SELECT md_list_data_loaders()" only.';
END;
$$ LANGUAGE plpgsql;


EOSQL
done