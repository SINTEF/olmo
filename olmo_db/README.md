# OLMO influx database help


# Starting up docker container

Log onto `Grafana-vm1` (on azure). The simplest (in the case that there is nothing "wrong" but it just stopped):

  1. Go to folder for the olmo repository.
  2. Run `docker-compose up -d`
  3. Go into the influx docker container: `docker exec -it influxdb bash`
  4. Start the cron deamon: `service cron start`
  5. Finally once grafana is up and running you need to update the token (alerting->notification channels)

A More complete restart would be:

  1. Check what is running: `docker ps`
  2. Stop either `influxdb` or `grafana` if they are running (`docker stop influxdb grafana`)
  3. Clean up old images (this step can help if the image has been changed): `docker system prune -a`
  4. You can start and run through the steps given above (Go to correct directory and start docker compose).


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
