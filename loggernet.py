import logging
import pandas as pd

import ingest
import processing

'''
File equivelent to a specific 'sensor' file, but for the loggernet data.
Gives details of how the loggernet files are to be decoded and ingested
into the database.
'''

logger = logging.getLogger('olmo.loggernet')


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


def ingest_loggernet_file(file_path, file_type, clients):
    '''Ingest loggernet files.

    NOTE: All cols that aren't strings should be floats, even if you think
    it will always be an int.

    Parameters
    ----------
    file_path : string
    file_type : string
        The 'basename' of the file, should correspond to one of those in the config.
    '''

    def load_data(file_path, data_cols, float_cols, rows_to_skip=None, time_col="TMSTAMP"):

        pd.set_option('precision', 6)
        df = pd.read_csv(file_path, sep=',', skiprows=0, header=1)
        if rows_to_skip is not None:
            df = df.iloc[rows_to_skip:]

        df['date'] = pd.to_datetime(df[time_col], format='%Y-%m-%d %H:%M:%S')
        df = df[['date'] + data_cols]

        # There can be strings inserted as "NAN", set these to the -7999 nan.
        for col in df.columns:
            if df[col].dtypes == 'object':
                df.loc[df[col].str.match('NAN'), col] = -7999

        # Force cols to have time np.float64
        df = ingest.float_col_fix(df, float_cols)
        # Loggernet data is in CET, but all influx data should be utc.
        df = df.set_index('date').tz_localize('CET', ambiguous='infer').tz_convert('UTC')

        return df

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_meteo_ais_':

        data_cols = [
            "distance", "Latitude_decimal", "Longitude_decimal", "temperature_digital",
            "pressure_digital", "humidity_digital", "dew_point", "wind_speed_digital",
            "wind_direction_digital"
        ]
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols)

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
        df = processing.constant_val_filter(df, 'latitude', lower=62.5, upper=64)
        df = processing.constant_val_filter(df, 'longitude', lower=10, upper=11)
        ingest.ingest_df(measurement_name, df, clients)

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
        # df = processing.constant_val_filter(df, 'displacement', lower=0, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

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
        df = processing.constant_val_filter(df, 'temperature', lower=-50, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

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
        df = processing.constant_val_filter(df, 'atmospheric_pressure', lower=500, upper=1500)
        ingest.ingest_df(measurement_name, df, clients)

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
        df = processing.constant_val_filter(df, 'humidity', lower=0, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

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
        # df = processing.constant_val_filter(df, 'dew_point', lower=-50, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

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
        df = processing.constant_val_filter(df, 'wind_speed', lower=0, upper=140)
        ingest.ingest_df(measurement_name, df, clients)

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
        df = processing.constant_val_filter(df, 'wind_direction', lower=1, upper=360)
        ingest.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Power_':

        data_cols = [
            "battery_voltage", "PV_Voltage1",
            "Total_of_battery", "PV1_current", "Input_current",
            "Total_charge_current_of_battery", "Load_current",
            "Total_discharge_of_battery", "Solar_reg_temperature",
            "error", "Load_output", "AUX1", "AUX2",
            "Energy_input_24H", "Energy_input_total", "Energy_output_24H",
            "Energy_output_total", "Derating", "Tarom_checksum"
        ]
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_voltage_munkholmen'
        field_keys = {"battery_voltage": 'battery_voltage',
                      "PV_Voltage1": "pv_voltage1"}
        tag_values = {'tag_sensor': 'solar_regulator',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'volts'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'battery_voltage', lower=0, upper=50)
        df = processing.constant_val_filter(df, 'pv_voltage1', lower=0, upper=50)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_current_munkholmen'
        field_keys = {"PV1_current": "input_current",
                      "Load_current": "load_current"}
        tag_values = {'tag_sensor': 'solar_regulator',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'amps'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'input_current', lower=0, upper=100)
        df = processing.constant_val_filter(df, 'load_current', lower=0, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_energy_use_munkholmen'
        field_keys = {"Energy_input_24H": "energy_input_24h",
                      "Energy_input_total": "energy_input_total",
                      "Energy_output_24H": "energy_output_24h",
                      "Energy_output_total": "energy_output_total"}
        tag_values = {'tag_sensor': 'solar_regulator',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'amp_hours'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_aux_munkholmen'
        field_keys = {"error": "error",
                      "AUX1": "aux1",
                      "AUX2": "aux2",
                      "Derating": "derating",
                      "Tarom_checksum": "tarom_checksum",
                      "Total_discharge_of_battery": "total_discharge_of_battery",
                      "Total_charge_current_of_battery": "total_charge_current_of_battery",
                      "Load_output": "load_output",
                      "Total_of_battery": "total_of_battery"}
        tag_values = {'tag_sensor': 'solar_regulator',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'none'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'solar_regulator_power_munkholmen'
        field_keys = {"Solar_reg_temperature": "Solar_reg_temperature"}
        tag_values = {'tag_sensor': 'solar_regulator',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees_celsius'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'Solar_reg_temperature', lower=-50, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Meteo_avgd_':

        data_cols = [
            "temperature_digital_Avg", "pressure_digital_Avg",
            "humidity_digital_Avg", "wind_speed_digital", "wind_direction_digital"
        ]
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_temperature_avg_munkholmen'
        field_keys = {"temperature_digital_Avg": 'temperature_avg'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees_celsius'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'temperature_avg', lower=-50, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_atmospheric_pressure_avg_munkholmen'
        field_keys = {"pressure_digital_Avg": 'atmospheric_pressure_avg'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'hecto_pascal'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'atmospheric_pressure_avg', lower=500, upper=1500)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_humidity_avg_munkholmen'
        field_keys = {"humidity_digital_Avg": 'humidity_avg'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'percent'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'humidity_avg', lower=0, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_speed_avg_munkholmen'
        field_keys = {"wind_speed_digital": 'wind_speed_avg'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'metres_per_second'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'wind_speed_avg', lower=0, upper=140)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_direction_avg_munkholmen'
        field_keys = {"wind_direction_digital": 'wind_direction_avg'}
        tag_values = {'tag_sensor': 'gill_weatherstation',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'processed',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'wind_direction_avg', lower=1, upper=360)
        ingest.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Current_':
        df = pd.read_csv(file_path, skiprows=1)
        data_cols = list(df.columns[2:])
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols)
        print(df_all.head())

        '''

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_current_speed_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('current_speed')}
        tag_values = {'tag_sensor': 'signature_100',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'cm_per_second'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_current_direction_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('current_direction')}
        tag_values = {'tag_sensor': 'signature_100',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_amplitude_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('amplitude')}
        tag_values = {'tag_sensor': 'signature_100',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'db'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_correlation_munkholmen'
        field_keys = {i:i for i in data_cols if i.startswith('correlation')}
        tag_values = {'tag_sensor': 'signature_100',
                    'tag_edge_device': 'cr6',
                    'tag_platform': 'munkholmen',
                    'tag_data_level': 'raw',
                    'tag_approved': 'none',
                    'tag_unit': 'none'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_velocity_munkholmen'
        field_keys = {i:i for i in data_cols if i.startswith('velocity')}
        tag_values = {'tag_sensor': 'signature_100',
                    'tag_edge_device': 'cr6',
                    'tag_platform': 'munkholmen',
                    'tag_data_level': 'raw',
                    'tag_approved': 'none',
                    'tag_unit': 'cm_per_second'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        ingest.ingest_df(measurement_name, df, clients)

        '''

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Wave_sensor_':

        data_cols = [
            "heading", "Hs", "Period", "Hmax", "direction"
        ]
        float_cols = data_cols

        df_all = load_data(file_path, data_cols, float_cols)  # , rows_to_skip=2, time_col="TIMESTAMP"

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_heading_munkholmen'
        field_keys = {"heading": 'heading'}
        tag_values = {'tag_sensor': 'seaview',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'heading', lower=0, upper=360)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hs_munkholmen'
        field_keys = {"Hs": 'hs'}
        tag_values = {'tag_sensor': 'seaview',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'metres'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'hs', lower=-100, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_period_munkholmen'
        field_keys = {"Period": 'period'}
        tag_values = {'tag_sensor': 'seaview',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'seconds'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'period', lower=0, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hmax_munkholmen'
        field_keys = {"Hmax": 'hmax'}
        tag_values = {'tag_sensor': 'seaview',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'metres'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'hmax', lower=-100, upper=100)
        ingest.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_direction_munkholmen'
        field_keys = {"direction": 'direction'}
        tag_values = {'tag_sensor': 'seaview',
                      'tag_edge_device': 'cr6',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'none',
                      'tag_unit': 'degrees'}
        df = filter_and_tag_df(df_all, field_keys, tag_values)
        # Data processing:
        df = processing.constant_val_filter(df, 'direction', lower=0, upper=360)
        ingest.ingest_df(measurement_name, df, clients)
