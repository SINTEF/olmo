import os
import logging
import pandas as pd
from influxdb import InfluxDBClient

import config
import util
import ingest

'''
File equivelent to a specific 'sensor' file, but for the loggernet data.
Gives details of how the loggernet files are to be decoded and ingested
into the database.
'''

logger = logging.getLogger('olmo.loggernet')

admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
client = InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'test')


def filter_df(df, col, lower=None, upper=None):

    assert ((lower is not None) or (upper is not None)), "You must filter either upper or lower bounds"
    # Assume that all data points will be approved or not (not none)
    df.loc[df['tag_approved'] == 'none', 'tag_approved'] = 'yes'
    if upper is not None:
        df.loc[df[col] > upper, 'tag_approved'] = 'no'
    if lower is not None:
        df.loc[df[col] < lower, 'tag_approved'] = 'no'

    return df


def add_tags(df, tag_values):
    for (k, v) in tag_values.items():
        df[k] = v
    return df


def filter_and_tag_df(df_all, field_keys, tag_values):
    '''Returns a df with tag_values and field_key values'''
    df = df_all.loc[:, [k for k in field_keys.keys()]]
    df = df.rename(columns=field_keys)
    df = add_tags(df, tag_values)
    return df


def ingest_df(measurement, df, client):

    all_cols = df.columns
    tag_cols = [c for c in all_cols if c[:4] == 'tag_']
    field_cols = [c for c in all_cols if c not in tag_cols]

    data = []
    for index, row in df.iterrows():
        data.append({
            'measurement': measurement,
            'time': index,
            'tags': {t[4:]: row[t] for t in tag_cols},
            'fields': {f: row[f] for f in field_cols},
        })

    client.write_points(data)
    # print("data written!!!")


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

    def load_data(file_path, data_cols, float_cols, rows_to_skip=None, time_col="TIMESTAMP"):

        pd.set_option('precision', 6)
        df = pd.read_csv(file_path, sep=',', skiprows=0, header=1)
        if rows_to_skip is not None:
            df = df.iloc[rows_to_skip:]

        df['date'] = pd.to_datetime(df[time_col], format='%Y-%m-%d %H:%M:%S')
        df = df[['date'] + data_cols]

        # Force cols to have time np.float64
        df = ingest.float_col_fix(df, float_cols)
        # Loggernet data is in CET, but all influx data should be utc.
        df = df.set_index('date').tz_localize('CET', ambiguous='infer').tz_convert('UTC')

        return df

    # def get_tag_values(
    #         tag_sensor='loggernet',
    #         tag_platform='munkholmen',
    #         tag_data_level='raw',
    #         tag_approved='none',
    #         tag_unit='none'):
    #     '''Function to return the tag_values dict, helps with default values.'''
    #     tag_values = {
    #         'tag_sensor': tag_sensor,
    #         'tag_platform': tag_platform,
    #         'tag_data_level': tag_data_level,
    #         'tag_approved': tag_approved,
    #         'tag_unit': tag_unit,
    #     }
    #     return tag_values

    # ==================================================================== #
    if file_type == 'CR6_EOL2,0_meteo_ais_':

        data_cols = [
            "distance", "Latitude_decimal", "Longitude_decimal", "temperature_digital",
            "pressure_digital", "humidity_digital", "dew_point", "wind_speed_digital",
            "wind_direction_digital"
        ]
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols, rows_to_skip=2)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_position_munkholmen'
        field_keys = {"Latitude_decimal": 'latitude',
                      "Longitude_decimal": 'longitude'}
        tag_values = {'tag_sensor': 'gps',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        # df = filter_df(df, 'latitude', lower=62.5, upper=64)
        # df = filter_df(df, 'longitude', lower=10, upper=11)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_position_displacement_munkholmen'
        field_keys = {"distance": 'position_displacement'}
        tag_values = {'tag_sensor': 'gps',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'metres'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        # df = filter_df(df, 'displacement', lower=0, upper=100)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_temperature_munkholmen'
        field_keys = {"temperature_digital": 'temperature'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees_celsius'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = filter_df(df, 'temperature', lower=-50, upper=100)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_atmospheric_pressure_munkholmen'
        field_keys = {"pressure_digital": 'atmospheric_pressure'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'hecto_pascal'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = filter_df(df, 'atmospheric_pressure', lower=500, upper=1500)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_humidity_munkholmen'
        field_keys = {"humidity_digital": 'humidity'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'percent'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = filter_df(df, 'humidity', lower=0, upper=100)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_dew_point_munkholmen'
        field_keys = {"dew_point": 'dew_point'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees_celsius'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        # df = filter_df(df, 'dew_point', lower=-50, upper=100)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_speed_munkholmen'
        field_keys = {"wind_speed_digital": 'wind_speed'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'metres_per_second'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = filter_df(df, 'wind_speed', lower=0, upper=140)
        ingest_df(measurement_name, df, client)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_direction_munkholmen'
        field_keys = {"wind_direction_digital": 'wind_direction'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = filter_df(df, 'wind_direction', lower=0, upper=360)
        ingest_df(measurement_name, df, client)

    # # ==================================================================== #
    # if file_type == 'CR6_EOL2,0_meteo_ais_':
    #     print("in FT: 'CR6_EOL2,0_meteo_ais_'")

    #     data_cols = [
    #         "distance", "Latitude_decimal", "Longitude_decimal", "temperature_digital",
    #         "pressure_digital", "humidity_digital", "dew_point", "wind_speed_digital",
    #         "wind_direction_digital"
    #     ]
    #     float_cols = data_cols

    #     df_all = load_data(file_path, data_cols, float_cols, rows_to_skip=2)

    #     # ---------------------------------------------------------------- #
    #     measurement_name = 'meteo_position_munkholmen'
    #     field_keys = {"Latitude_decimal": 'latitude',
    #                   "Longitude_decimal": 'longitude'}
    #     tag_values = {'tag_sensor': 'loggernet',
                    #   'tag_edge_device': 'cr6',
    #                   'tag_platform': 'munkholmen',
    #                   'tag_data_level': 'raw',  
    #                   'tag_approved': 'none',
    #                   'tag_unit': 'degrees'}
    #     df = filter_and_tag_df(df_all, field_keys, tag_values)
    #     # Data processing:
    #     # df = filter_df(df, 'latitude', lower=62.5, upper=64)
    #     # df = filter_df(df, 'longitude', lower=10, upper=11)
    #     print(df.head())
    #     ingest_df(measurement_name, df, client)