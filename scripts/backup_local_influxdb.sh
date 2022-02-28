#!/bin/bash

# NOTE: This file replace by the python version: backup_influx_to_az.py


home_dir="/home/user"
timestamp=$(date +"%Y%m%d")
backupfolder=$"${home_dir}/OceanLab/backups/influxbackup_$timestamp"
zippedfile=$"${home_dir}/OceanLab/backups/influxbackup_$timestamp.zip"
azfile=$"influx_backups/influxbackup_testing_$timestamp.zip"
token=$(cat ${home_dir}/OceanLab/azure_token_datalake)

echo "=========="
echo "Starting backup to file $azfile"

# Create a folder
mkdir $backupfolder

# Backup influx into that folder and then .zip the folder
influxd backup -portable $backupfolder
zip -rm $zippedfile $backupfolder

# Upload that folder to the azure datalake
az storage fs file upload --source $zippedfile -p $azfile -f oceanlabdlcontainer --account-name oceanlabdlstorage --sas-token $token
echo "----------"
echo "Finished running backup_local_influxdb.sh."