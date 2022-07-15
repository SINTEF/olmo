import os
import time
import pandas as pd
import datetime
from influxdb import InfluxDBClient
import seawater
import xmltodict

import config
import ingest
import util

# File to take data from some tables, process it and put that
# processed data into a new table.

# Databases:
admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
read_client = InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab')
write_clients = [
    InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'example'),
    InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
]

# List of measurements and variables which will be retrieved from
# influx from which the processed data should be collected.
input_data = [
    ['ctd_salinity_munkholmen', 'salinity'],
    ['ctd_temperature_munkholmen', 'temperature'],
    ['ctd_pressure_munkholmen', 'pressure'],
]
# Time period:
start_time = '2022-06-01T00:00:00Z'
end_time = '2022-06-30T15:00:00Z'


with open('19-8154.xmlcon', 'r') as f:
    calibfile = xmltodict.parse(f.read())
sensors = calibfile['SBE_InstrumentConfiguration']['Instrument']['SensorArray']['Sensor']


def calcpH(temp,pHvout):
    phslope=float(sensors[3]['pH_Sensor']['Slope'])
    phoffset=float(sensors[3]['pH_Sensor']['Offset'])
    
    abszero=273.15
    ktemp=abszero+temp
    const=1.98416E-4
    ph=7+(pHvout-phoffset)/(phslope*ktemp*const)
    return(ph)


def main():

    print("Starting running add_processed_data.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Must break down the timeslice smaller periods:
    start_time_ = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
    end_time_ = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
    total_time_delta = end_time_ - start_time_
    periods = []
    # Assuming breaking down into days:
    d = 0
    while d < total_time_delta.days:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            (start_time_ + datetime.timedelta(days=d + 1)).strftime('%Y-%m-%dT%H:%M:%SZ')))
        d += 1
    if total_time_delta.seconds > 0:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time_.strftime('%Y-%m-%dT%H:%M:%SZ')))

    for p in periods:
        timeslice = f"time > '{p[0]}' AND time < '{p[1]}'"

        # =================== Get the data:
        dfs = []
        for d in input_data:
            dfs.append(util.query_influxdb(read_client, d[0], d[1], timeslice, False, approved='all'))
        df_all = dfs[0]
        for t in dfs[1:]:
            df_all = pd.merge(df_all, t, how='left', on='time')
        df_all = df_all.set_index('time').tz_convert('UTC')  # Should be in correct TZ as comes from DB

        # =================== Processing to create the new data to be ingested:
        MUNKHOLMEN_LATITUDE = 63.456314
        df_all['density'] = seawater.eos80.dens0(df_all['salinity'], df_all['temperature'])
        df_all['depth'] = seawater.eos80.dpth(df_all['pressure'], MUNKHOLMEN_LATITUDE)
        print(df_all.head(1))

        # =================== Now actually ingest the data:
        tag_values = {'tag_sensor': 'ctd',
                      'tag_edge_device': 'munkholmen_topside_pi',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'no',
                      'tag_unit': 'none'}

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_depth_munkholmen'
        field_keys = {"depth": 'depth'}
        tag_values['tag_unit'] = 'metres'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_density_munkholmen'
        field_keys = {"density": 'density'}
        tag_values['tag_unit'] = 'kilograms_per_cubic_metre'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        print(f"Finished ingesting timeslice {timeslice} at "
              + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    print("Finished all at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    t = time.time()
    main()
    print(f"time taken: {time.time() - t}")
