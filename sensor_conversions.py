import os
import logging
import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
from datetime import datetime, timezone

import config
import util_file

logger = logging.getLogger('olmo.sensor_conversions')


admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
client = InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'example')
client = InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab')
# client_df = DataFrameClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'example')
# client_az = DataFrameClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'example')


def float_col_fix(df, float_cols):
    '''Avoid problem where float cols give error if they "round to zero"'''
    for i, col in enumerate(df.columns):
        if col in float_cols:
            df[col] = df[col].astype(np.float64)
    return df


# def ingest_ctd(txt_filename='~/Downloads/CTD_example.txt'):

#     ctd_df = pd.read_csv(txt_filename, skiprows=8, delimiter=';')

#     client.create_database('ctd_demo')

#     def make_json_body(df):
#         timestring = df['Date'] + ' ' + df['Time']

#         date_format = '%d/%m/%Y %H:%M:%S'
#         time_for_influx = datetime.strptime(timestring, date_format).astimezone(timezone.utc)

#         json_body = [{
#             "measurement": "ctd_demo",
#             "time": time_for_influx,
#             "fields": {
#                 "temperature": df.Temp,
#                 "conductivity": df['Cond.'],
#                 "pressure": df.Press
#             }
#         }]
#         return json_body

#     logger.info('Synthetic CTD: looping through timestamps')
#     for i in range(len(ctd_df)):
#         logger.info(i / len(ctd_df) * 100)
#         json_body = make_json_body(ctd_df.iloc[i])
#         client.write_points(json_body)

#     logger.info('ok')


# def ingest_silc_sim(filename, storage_location):

#     # Get time, this is for all measurements relating to this file.
#     # Note, the filename has the transfer timestamp still appended
#     filename_orig = util_file.remove_timestring(filename)
#     timestring = filename_orig[-15:]
#     date_format = '%Y%m%d-%H%M%S'
#     time_for_influx = datetime.strptime(timestring, date_format).astimezone(timezone.utc)
#     print(filename)
#     print(time_for_influx)

#     client.create_database('silcam_sim')

#     # Write the location to the database.
#     json_body = [{
#         "measurement": "silcam_datafile",
#         "time": time_for_influx,
#         "fields": {
#             "storage_location": storage_location
#         }
#     }]
#     client.write_points(json_body)

#     # Normally here have to open and process silc. file.
#     df = pd.read_csv(filename, delimiter=',')

#     def make_json_body(df):

#         json_body = [{
#             "measurement": "silcam_particle_count",
#             "time": time_for_influx,
#             "fields": {
#                 "synthetic": df.iloc[1]
#             }
#         }]
#         return json_body

#     # Note in this case there is only a single measurement.
#     logger.info('Ingestion: Particle count')
#     logger.info(range(len(df)))
#     for i in range(len(df)):
#         # print(f"{i/len(df)*100:.0f}", end=' ')
#         json_body = make_json_body(df.iloc[i])
#         client.write_points(json_body)

#     logger.info('OK')


# def lisst200_csv_to_df(csv_filename):
#     '''take a LISST-200 .CSV file and returns a pandas DataFrame'''

#     c = 36  # number of size bins of LISST-200x
#     column_names = []
#     for size_bin in range(c):
#         name = f'size_bin_{size_bin+1:02}'
#         column_names += [name]

#     column_names += ['Laser transmission Sensor']
#     column_names += ['Supply voltage in [V]']
#     column_names += ['External analog input 1 [V]']
#     column_names += ['Laser Reference sensor [mW]']
#     column_names += ['Depth in [m of sea water]']
#     column_names += ['Temperature [C]']
#     column_names += ['Year']
#     column_names += ['Month']
#     column_names += ['Day']
#     column_names += ['Hour']
#     column_names += ['Minute']
#     column_names += ['Second']
#     column_names += ['External analog input 2 [V]']
#     column_names += ['Mean Diameter [μm]']
#     column_names += ['Total Volume Concentration [PPM]']
#     column_names += ['Relative Humidity [%]']
#     column_names += ['Accelerometer X [not presently calibrated or used]']
#     column_names += ['Accelerometer Y [not presently calibrated or used]']
#     column_names += ['Accelerometer Z [not presently calibrated or used]']
#     column_names += ['Raw pressure [most significant bit]']
#     column_names += ['Raw pressure [least significant 16 bits]']
#     column_names += ['Ambient Light [counts – not calibrated]']
#     column_names += ['Not used (set to zero)']
#     column_names += ['Computed optical transmission over path [dimensionless]']
#     column_names += ['Beam-attenuation (c) [m-1]']

#     df = pd.read_csv(csv_filename, names=column_names)

#     df['date'] = pd.to_datetime(dict(year=df.Year,
#                                      month=df.Month,
#                                      day=df.Day,
#                                      hour=df.Hour,
#                                      minute=df.Minute,
#                                      second=df.Second))

#     return df


# def ingest_lisst_200(csv_filename):

#     df = lisst200_csv_to_df(csv_filename)

#     # print(df)

#     float_cols = [col for col in df.columns if col != 'date']
#     # print(float_cols)
#     df = float_col_fix(df, float_cols)
#     df = df.set_index('date').tz_localize('CET', ambiguous='infer')

#     # print(df.index)

#     logger.info('Ingesting file to lisst_200.')
#     pd.set_option('precision', 6)
#     client_df.write_points(df, 'lisst_200')
#     logger.info('OK.')


# def lisst_vd_data(datafile):
#     '''LISST-100'''

#     data = pd.read_csv(datafile, delimiter=' ', header=None)

#     # vd = data.as_matrix(columns=data.columns[0:32])
#     vd = data.iloc[:, 0:32].values
#     # depth = data.as_matrix(columns=data.columns[36:37])/100
#     depth = data.iloc[:, 36:37].values / 100

#     # data3940_ = data.as_matrix(columns=data.columns[38:40])
#     data3940_ = data.iloc[:, 38:40].values

#     ts = pd.DataFrame()
#     for i in range(len(data3940_)):
#         data3940 = data3940_[i, :]
#         day = np.floor(data3940[0] / 100)
#         hour = data3940[0] - 100 * day
#         minute = np.floor(data3940[1] / 100)
#         second = data3940[1] - 100 * minute

#         YEAR = 2020

#         ts_ = pd.to_datetime([str(YEAR) + '-' + str(int(day)) + '-' + str(int(hour))
#                               + '-' + str(int(minute)) + '-' + str(int(second))],
#                              format='%Y-%j-%H-%M-%S')
#         tmp = pd.DataFrame(columns=['Time'])
#         tmp['Time'] = ts_
#         ts = ts.append(tmp)

#     # time = ts.as_matrix().flatten()
#     time = ts.values.flatten()

#     return time, depth, vd


# def ingest_lisst(datafile='~/Downloads/OneDrive_1_5-7-2021/L0082314.asc'):  # TODO Should have a generic path!
#     '''LISST-100 asc files'''

#     logger.info('loading lisst data')
#     time, depth, vd = lisst_vd_data(datafile)
#     logger.info('  ok.')

#     client.create_database('lisst_demo')

#     r, c = np.shape(vd)

#     def make_json_body(time, depth, vd):
#         timestring = str(time)[:19]  # manually remove the decimal seconds from this string!

#         date_format = '%Y-%m-%dT%H:%M:%S'
#         time_for_influx = datetime.strptime(timestring, date_format).astimezone(timezone.utc)

#         fieldstr = '{"depth": depth,'
#         for size_bin in range(c):
#             name = f'size_bin_{size_bin+1:02}'
#             value = str(vd[size_bin])
#             fieldstr += '"' + name + '": ' + value + ','
#         fieldstr += '}'
#         json_body = [{
#             "measurement": "lisst_demo",
#             "time": time_for_influx,
#             "fields":
#                 eval(fieldstr),
#         }]
#         return json_body

#     logger.info('looping through timestamps')
#     for i, t in enumerate(time):
#         logger.info(i / len(time) * 100)
#         d = depth[i]
#         v = vd[i, :]
#         json_body = make_json_body(t, d, v)
#         client.write_points(json_body)

#     logger.info('ok')

#     r, c = np.shape(vd)
#     query = 'SELECT '
#     for size_bin in range(c):
#         name = f'size_bin_{size_bin+1:02}'
#         query += 'mean("' + name + '") AS "bin ' + str(size_bin + 1) + '",'
#     query = query[:-1] + ' FROM "lisst_demo" WHERE $timeFilter GROUP BY time($__interval) fill(null)'
#     print('Suggested query for grafana heatmap using timeseries buckets:')
#     print(query)


def add_custom_data_directory(df):

    logger.info('Adding custom data directory.')
    client_df.create_database('custom_datasets')
    client_df.write_points(df, 'datasets')
    logger.info('OK.')


# def ingest_loggernet(df):

#     logger.info('Ingesting LoggerNet df.')
#     pd.set_option('precision', 6)
#     client_df.create_database('munkholmen_loggernet')
#     client_df.write_points(df, 'loggernet_public')
#     logger.info('OK.')


def ingest_loggernet_file(file_path, file_type):
    '''Ingest loggernet files.

    NOTE: All cols that aren't strings should be floats, even if you think
    it will always be an int.

    Parameters
    ----------
    file_path : string
    file_type : string
        The 'basename' of the file, should correspond to one of those in the config.
    '''

    def ingest_and_log(file_path, data_cols, float_cols, measurement_name, time_col="TMSTAMP"):

        pd.set_option('precision', 6)
        df = pd.read_csv(file_path, sep=',', skiprows=0, header=1)
        df['date'] = pd.to_datetime(df[time_col], format='%Y-%m-%d %H:%M:%S')
        df = df[['date'] + data_cols]

        # Force cols to have time np.float64
        df = float_col_fix(df, float_cols)
        # Loggernet data is in CET, but all influx data will be utc.
        df = df.set_index('date').tz_localize('CET', ambiguous='infer').tz_convert('UTC')

        logger.info(f'Ingesting file to sintefpc: {measurement_name}.')
        client_df.write_points(df, measurement_name)
        logger.info(f'Ingesting file to azure: {measurement_name}.')
        try:
            client_az.write_points(df, measurement_name)
        except Exception as e:
            print(e)
        logger.info('OK.')

    if file_type == 'CR6_EOL2,0_meteo_ais_':
        data_cols = [
            "distance", "Latitude_decimal", "Longitude_decimal", "temperature_digital",
            "pressure_digital", "humidity_digital", "dew_point", "wind_speed_digital", "wind_direction_digital"
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_meteo')

    if file_type == 'CR6_EOL2p0_Public_':
        data_cols = [
            'RECNBR', 'manu', 'mode_auto', 'operational_state', 'Latitude_decimal',
            'Longitude_decimal', 'wind_speed_digital', 'wind_direction_digital', 'temperature_digital',
            'pressure_digital', 'humidity_digital', 'battery_level', 'battery_voltage', 'batt_volt',
            # 'H24_Panels_energy', 'H24_User_energy', 'WaveHeight', 'WaveDirection', 'WavePeriod',
            'H24_User_energy', 'WaveHeight', 'WaveDirection', 'WavePeriod',
        ]
        float_cols = [
            'manu', 'mode_auto', 'Latitude_decimal',
            'Longitude_decimal', 'wind_speed_digital', 'temperature_digital',
            'pressure_digital', 'battery_voltage', 'batt_volt',
            'WaveHeight', 'WaveDirection', 'WavePeriod',
        ]
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_public')

    if file_type == 'CR6_EOL2p0_Meteo_':
        data_cols = [
            "temperature_digital_Avg", "pressure_digital_Avg", "humidity_digital_Avg",
            "wind_speed_digital", "wind_direction_digital", "wind_speed_digital_WVc"
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_meteo')

    if file_type == 'CR6_EOL2p0_Position_':
        data_cols = [
            "distance", "Latitude_decimal", "Longitude_decimal"
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_position')

    if file_type == 'CR6_EOL2p0_Seaview_':
        data_cols = [
            "heading", "Hs", "Period", "Hmax", "direction"
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_seaview')

    if file_type == 'CR6_EOL2p0_Status_':
        data_cols = [
            "Battery", "LithiumBattery",
            "MemorySize", "MemoryFree",
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_status')

    if file_type == 'CR6_EOL2p0_Power_':
        data_cols = [
            "battery_level", "battery_voltage", "H1_Panels_Current",
            "H24_Panels_energy", "H24_User_energy"
        ]
        float_cols = data_cols
        ingest_and_log(file_path, data_cols, float_cols, 'loggernet_power')
