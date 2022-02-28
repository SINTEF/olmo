#!/bin/bash

# Fetch the passwords from the secrets folder
source /home/Secrets/influx_odp_credentials

# Create the database and its retention policies
influx -execute 'CREATE DATABASE "share_odp"'
influx -execute 'CREATE RETENTION POLICY "one_hour" ON "share_odp" DURATION 1h REPLICATION 1'
influx -execute 'CREATE RETENTION POLICY "one_day" ON "share_odp" DURATION 1d REPLICATION 1'
influx -execute 'CREATE RETENTION POLICY "one_week" ON "share_odp" DURATION 1w REPLICATION 1'
influx -execute 'ALTER RETENTION POLICY one_week ON share_odp DEFAULT'

# Create the user
influx -execute "CREATE USER '${ODP_USER}' WITH PASSWORD '${ODP_PWD}'"
influx -execute "GRANT READ ON share_odp TO '${ODP_USER}'"
influx -execute "SHOW GRANTS FOR '${ODP_USER}'"

# Create the continuous query
# influx -execute 'CREATE CONTINUOUS QUERY odp_db_update_10m ON example BEGIN SELECT temperature_digital INTO share_odp.one_hour.loggernet_public FROM loggernet_public GROUP BY time(10m) END'
