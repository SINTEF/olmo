# OLMO influx database help


# Starting up docker container

Log onto `Grafana-vm1` (on azure). Assuming that you need to restart then (if starting from nothing you can skip steps `stop` and `prune -a`).

  1. Check what is running: `docker ps`
  2. Stop either `influxdb` or `grafana` if they are running (`docker stop influxdb grafana`)
  3. Clean up old images: `docker system prune -a`
  4. Go to folder with the olmo repository (and check you are on `main`)
  5. Run `docker-compose up -d`
  6. Go into the influx docker container: `docker exec -it influxdb bash`
  7. Start the cron deamon: `service cron start`
  8. Finally once grafana is up and running you need to update the token (alerting->notification channels)
  9. Also, if you stopped the ingestion, restart that.


# Backup and restore

## Backup

Backup should be done simply by running the `backup_influx_to_az.py` file. Note that this
contains references to file paths from the config file, these will probably fail if not running
on either of Torfinn2 or the grafana VM in azure.

## Restore

The simplest is to just build and start the docker container again. Then run a resotre from that. You will
need to

  1. Download from Azure the correct backup.
  2. Unzip the backup (as they are uploaded as .zip files)
  3. Move the unzipped folder into `./backups/` (so docker sees it)
  4. Go into the container (`docker exec -it influxdb bash`)
  5. Restore: `influxd restore -portable /backups/influxbackup_20220110/`

# Some manual operations

These could almost be considered 'issues' with the repo, although they may never be addressed.

 * Build up of backup files in portal.azure.com (oceanlabdlcontaier/influx_backups). I just delete 'exponentially' older ones.
 * Build up of files on torfinn2. Older ones are deleted (as the data should be in influx/azure)
 * Build up of log files on tofrinn2 and Grafana_vm1. Occasionally put into a folder with a final date of iles in that folder.

This can remove all files in a folder older then 7 days:
`find * -mtime +7 -exec rm {} \;`