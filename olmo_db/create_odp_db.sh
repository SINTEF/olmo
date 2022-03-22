#!/bin/bash

# Create the database and its retention policies
influx -execute 'CREATE DATABASE "share_odp"'
influx -execute 'CREATE RETENTION POLICY "one_hour" ON "share_odp" DURATION 1h REPLICATION 1'
influx -execute 'CREATE RETENTION POLICY "one_day" ON "share_odp" DURATION 1d REPLICATION 1'
influx -execute 'CREATE RETENTION POLICY "one_week" ON "share_odp" DURATION 1w REPLICATION 1'
influx -execute 'ALTER RETENTION POLICY one_week ON share_odp DEFAULT'

# Create the users

# Fetch the passwords from the secrets folder
source /home/Secrets/influx_odp_credentials
influx -execute "CREATE USER '${USER}' WITH PASSWORD '${PWD}'"
influx -execute "GRANT READ ON share_odp TO '${USER}'"
influx -execute "SHOW GRANTS FOR '${USER}'"

source /home/Secrets/influx_tdv_credentials
influx -execute "CREATE USER '${USER}' WITH PASSWORD '${PWD}'"
influx -execute "GRANT READ ON share_odp TO '${USER}'"
influx -execute "SHOW GRANTS FOR '${USER}'"

source /home/Secrets/influx_node2_credentials
influx -execute "CREATE USER '${USER}' WITH PASSWORD '${PWD}'"
influx -execute "GRANT READ ON share_odp TO '${USER}'"
influx -execute "SHOW GRANTS FOR '${USER}'"
