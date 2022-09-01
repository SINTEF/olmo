import os
import time
import pandas as pd
import datetime
from influxdb import InfluxDBClient
import seawater
import xmltodict
import numpy as np

from ctd import CTD
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
    ['ctd_voltages_munkholmen', 'volt0'],
    ['ctd_voltages_munkholmen', 'volt1'],
    ['ctd_voltages_munkholmen', 'volt2'],
    ['ctd_voltages_munkholmen', 'volt5'],
    ['ctd_voltages_munkholmen', 'volt4'],
    ['ctd_sbe63_munkholmen', 'sbe63_temperature_voltage'],
    ['ctd_sbe63_munkholmen', 'sbe63']
]
# Time period:
start_time = '2022-07-15T00:00:00Z'
end_time = '2022-07-22T08:00:00Z'


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

    # Get ctd object and load the calibration file.
    ctd = CTD()
    ctd.load_calibration()

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
        df_all['density'] = seawater.eos80.dens0(df_all['salinity'], df_all['temperature'])
        df_all['depth'] = seawater.eos80.dpth(df_all['pressure'], ctd.MUNKHOLMEN_LATITUDE)

        df_all['ph'] = ctd.calcpH(df_all['temperature'], df_all['volt0'])
        df_all['cdom'] = ctd.calcCDOM(df_all['volt1'])
        df_all['par'] = ctd.calcPAR(df_all['volt2'])
        df_all['chl'] = ctd.calcchl(df_all['volt4'])
        df_all['ntu'] = ctd.calcNTU(df_all['volt5'])
        df_all['dissolved_oxygen_temperature'] = ctd.calcDO_T(df_all['sbe63_temperature_voltage'])
        df_all['dissolved_oxygen'] = ctd.calcDO(df_all['sbe63'], df_all['dissolved_oxygen_temperature'],
                                                df_all['salinity'], df_all['pressure'])

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

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_ph_munkholmen'
        field_keys = {"ph": 'ph'}
        tag_values['tag_unit'] = 'none'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_cdom_munkholmen'
        field_keys = {"cdom": 'cdom'}
        tag_values['tag_unit'] = 'ppb'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_par_munkholmen'
        field_keys = {"par": 'par'}
        tag_values['tag_unit'] = 'micro_mol_photons_per_metre_squared_per_second'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_chl_munkholmen'
        field_keys = {"chl": 'chl'}
        tag_values['tag_unit'] = 'micro_grams_per_litre'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_ntu_munkholmen'
        field_keys = {"ntu": 'ntu'}
        tag_values['tag_unit'] = 'ntu'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_dissolved_oxygen_munkholmen'
        field_keys = {"dissolved_oxygen": 'dissolved_oxygen'}
        tag_values['tag_unit'] = 'millilitres_per_litre'
        df = util.filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, write_clients)

        # ------------------------------------------------------------ #
        measurement_name = 'ctd_dissolved_oxygen_temperature_munkholmen'
        field_keys = {"dissolved_oxygen_temperature": 'dissolved_oxygen_temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'
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
