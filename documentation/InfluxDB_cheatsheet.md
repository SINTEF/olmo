# InfluxDB Cheatsheet

## Managine the influxdb service/daemon:
    
    $ sudo service influxdb start
    $ sudo service influxdb stop
    $ sudo service influxdb restart
    $ sudo service influxdb status

## Connect to InfluxDB using the commandline:

    $ influx

## Basic commands from within the cl client:

Create a database foo:

    CREATE DATABASE foo

List the databases:

    SHOW DATABASES

Select the a db to 'work with':

    USE foo

List measurements:

    SHOW MEASUREMENTS

Add a data point:

    INSERT meaurement_name,key=5 value=12


Show measurements for name: meaurement_name:

    SELECT * FROM meaurement_name LIMIT 3

Drop meaurement_name measurements:

    DROP MEASUREMENT meaurement_name

Show field keys:

    SHOW FIELD KEYS FROM "meaurement_name-A6"
    
Get power records from measurement with tag and time range:

    SELECT "power" FROM "drilling" WHERE ("module_id"='rover') AND time >= now() - 9h

Show series:
    
    SHOW SERIES
    
Drop all series for tag:

    DROP SERIES FROM "drilling" WHERE ("module_id" = 'oppy')

Drop a datapoint need to happen by filtering on time:

    DELETE FROM "drilling" WHERE time > '2022-06-05 15:10:42' and time < '2022-06-07 03:20:42'
