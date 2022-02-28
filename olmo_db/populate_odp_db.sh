#!/bin/bash

# Fetch the passwords from the secrets folder
source /home/Secrets/influx_admin_credentials

# Set influx authentification
export INFLUX_USERNAME=$USER
export INFLUX_PASSWORD=$PWD

influx -execute 'SELECT Latitude_decimal INTO share_odp..Latitude_decimal FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT Longitude_decimal INTO share_odp..Longitude_decimal FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT temperature_digital INTO share_odp..temperature_digital FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT wind_speed_digital INTO share_odp..wind_speed_digital FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT wind_direction_digital INTO share_odp..wind_direction_digital FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT pressure_digital INTO share_odp..pressure_digital FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT humidity_digital INTO share_odp..humidity_digital FROM example..loggernet_public WHERE time >= now() - 120m GROUP BY *'

now=$(date +"%T")
echo "Finished at $now"
