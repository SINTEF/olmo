#!/bin/bash

# Fetch the passwords from the secrets folder
source /home/Secrets/influx_admin_credentials

# Set influx authentification
export INFLUX_USERNAME=$USER
export INFLUX_PASSWORD=$PWD

influx -execute 'SELECT latitude AS Latitude_decimal INTO share_odp..Latitude_decimal FROM oceanlab..meteo_position_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT longitude AS Longitude_decimal INTO share_odp..Longitude_decimal FROM oceanlab..meteo_position_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT temperature AS temperature_digital INTO share_odp..temperature_digital FROM oceanlab..meteo_temperature_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT wind_speed AS wind_speed_digital INTO share_odp..wind_speed_digital FROM oceanlab..meteo_wind_speed_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT wind_direction AS wind_direction_digital INTO share_odp..wind_direction_digital FROM oceanlab..meteo_wind_direction_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT atmospheric_pressure AS pressure_digital INTO share_odp..pressure_digital FROM oceanlab..meteo_atmospheric_pressure_munkholmen WHERE time >= now() - 120m GROUP BY *'
influx -execute 'SELECT humidity AS humidity_digital INTO share_odp..humidity_digital FROM oceanlab..meteo_humidity_munkholmen WHERE time >= now() - 120m GROUP BY *'
# Node 2 data added to share_odp:
influx -execute 'SELECT wind_speed INTO share_odp..wind_speed_brattora01 FROM oceanlab..wind_speed_brattora01 WHERE time >= now() - 24h GROUP BY *'
influx -execute 'SELECT wind_direction INTO share_odp..wind_direction_brattora01 FROM oceanlab..wind_direction_brattora01 WHERE time >= now() - 24h GROUP BY *'
influx -execute 'SELECT gust_speed INTO share_odp..gust_speed_brattora01 FROM oceanlab..gust_speed_brattora01 WHERE time >= now() - 24h GROUP BY *'
influx -execute 'SELECT wind_speed INTO share_odp..wind_speed_brattora02 FROM oceanlab..wind_speed_brattora02 WHERE time >= now() - 24h GROUP BY *'
influx -execute 'SELECT wind_direction INTO share_odp..wind_direction_brattora02 FROM oceanlab..wind_direction_brattora02 WHERE time >= now() - 24h GROUP BY *'
influx -execute 'SELECT gust_speed INTO share_odp..gust_speed_brattora02 FROM oceanlab..gust_speed_brattora02 WHERE time >= now() - 24h GROUP BY *'

# Populate share_bistro DB:
influx -execute 'SELECT * INTO share_bistro..ctd_conductivity_munkholmen FROM oceanlab..ctd_conductivity_munkholmen WHERE time >= now() - 120m GROUP BY *'

now=$(date +"%T")
echo "Finished at $now"
