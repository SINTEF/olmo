import logging
import numpy as np
import pandas as pd

import util_db
import processing

'''
File equivelent to a specific 'sensor' file, but for the loggernet data.
Gives details of how the loggernet files are to be decoded and ingested
into the database.
'''

logger = logging.getLogger('olmo.loggernet')


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

    def load_data(
            file_path, data_cols, float_cols, str_cols=[],
            timezone='CET', rows_to_skip=None, time_col="TMSTAMP"):

        pd.set_option('precision', 6)
        str_cols_dict = {c: str for c in str_cols} if str_cols else None
        df = pd.read_csv(file_path, sep=',', skiprows=0, header=1, dtype=str_cols_dict)
        if rows_to_skip is not None:
            df = df.iloc[rows_to_skip:]

        df['date'] = pd.to_datetime(df[time_col], format='%Y-%m-%d %H:%M:%S')
        df = df[['date'] + data_cols]

        # There can be strings inserted as "NAN", set these to the -7999 nan.
        for col in df.columns:
            if col in float_cols:
                if df[col].dtypes == 'object':
                    df.loc[df[col].str.match('NAN'), col] = -7999

        # Force cols to have time np.float64
        df = util_db.force_float_cols(df, float_cols=float_cols)
        # Loggernet data is in CET, but all influx data should be utc.
        df = df.set_index('date').tz_localize(timezone, ambiguous='infer').tz_convert('UTC')

        return df

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_meteo_ais_':

        data_cols = [
            "distance", "Latitude_decimal", "Longitude_decimal", "temperature_digital",
            "pressure_digital", "humidity_digital", "dew_point", "wind_speed_digital",
            "wind_direction_digital"
        ]
        float_cols = data_cols
        df_all = load_data(file_path, data_cols, float_cols, timezone='CET')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'gill_weatherstation',
            'tag_edge_device': 'cr6',
            'tag_platform': 'munkholmen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_position_munkholmen'
        field_keys = {"Latitude_decimal": 'latitude',
                      "Longitude_decimal": 'longitude'}
        tag_values['tag_sensor'] = 'gps'
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'latitude', lower=62.5, upper=64)
        df = processing.constant_val_filter(df, 'longitude', lower=10, upper=11)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_position_displacement_munkholmen'
        field_keys = {"distance": 'position_displacement'}
        tag_values['tag_sensor'] = 'gps'
        tag_values['tag_unit'] = 'metres'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # df = processing.constant_val_filter(df, 'displacement', lower=0, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_temperature_munkholmen'
        field_keys = {"temperature_digital": 'temperature'}
        tag_values['tag_unit'] = 'degrees_celsius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'temperature', lower=-50, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_atmospheric_pressure_munkholmen'
        field_keys = {"pressure_digital": 'atmospheric_pressure'}
        tag_values['tag_unit'] = 'hecto_pascal'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'atmospheric_pressure', lower=500, upper=1500)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_humidity_munkholmen'
        field_keys = {"humidity_digital": 'humidity'}
        tag_values['tag_unit'] = 'percent'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'humidity', lower=0, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_dew_point_munkholmen'
        field_keys = {"dew_point": 'dew_point'}
        tag_values['tag_unit'] = 'degrees_celsius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # df = processing.constant_val_filter(df, 'dew_point', lower=-50, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_speed_munkholmen'
        field_keys = {"wind_speed_digital": 'wind_speed'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'wind_speed', lower=0, upper=140)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_direction_munkholmen'
        field_keys = {"wind_direction_digital": 'wind_direction'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'wind_direction', lower=1, upper=360)
        util_db.ingest_df(measurement_name, df, clients)

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

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'solar_regulator',
            'tag_edge_device': 'cr6',
            'tag_platform': 'munkholmen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}
        # ---------------------------------------------------------------- #
        measurement_name = 'power_voltage_munkholmen'
        field_keys = {"battery_voltage": 'battery_voltage',
                      "PV_Voltage1": "pv_voltage1"}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'battery_voltage', lower=0, upper=50)
        df = processing.constant_val_filter(df, 'pv_voltage1', lower=0, upper=50)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_current_munkholmen'
        field_keys = {"PV1_current": "input_current",
                      "Load_current": "load_current"}
        tag_values['tag_unit'] = 'amperes'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'input_current', lower=0, upper=100)
        df = processing.constant_val_filter(df, 'load_current', lower=0, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_energy_use_munkholmen'
        field_keys = {"Energy_input_24H": "energy_input_24h",
                      "Energy_input_total": "energy_input_total",
                      "Energy_output_24H": "energy_output_24h",
                      "Energy_output_total": "energy_output_total"}
        tag_values['tag_unit'] = 'amp_hours'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

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
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'solar_regulator_power_munkholmen'
        field_keys = {"Solar_reg_temperature": "Solar_reg_temperature"}
        tag_values['tag_unit'] = 'degrees_celsius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'Solar_reg_temperature', lower=-50, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Meteo_avgd_':

        data_cols = [
            "temperature_digital_Avg", "pressure_digital_Avg",
            "humidity_digital_Avg", "wind_speed_digital", "wind_direction_digital"
        ]
        float_cols = data_cols
        df_all = load_data(file_path, data_cols, float_cols)

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'gill_weatherstation',
            'tag_edge_device': 'cr6',
            'tag_platform': 'munkholmen',
            'tag_data_level': 'processed',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_temperature_avg_munkholmen'
        field_keys = {"temperature_digital_Avg": 'temperature_avg'}
        tag_values['tag_unit'] = 'degrees_celsius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'temperature_avg', lower=-50, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_atmospheric_pressure_avg_munkholmen'
        field_keys = {"pressure_digital_Avg": 'atmospheric_pressure_avg'}
        tag_values['tag_unit'] = 'hecto_pascal'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'atmospheric_pressure_avg', lower=500, upper=1500)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_humidity_avg_munkholmen'
        field_keys = {"humidity_digital_Avg": 'humidity_avg'}
        tag_values['tag_unit'] = 'percent'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'humidity_avg', lower=0, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_speed_avg_munkholmen'
        field_keys = {"wind_speed_digital": 'wind_speed_avg'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'wind_speed_avg', lower=0, upper=140)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_direction_avg_munkholmen'
        field_keys = {"wind_direction_digital": 'wind_direction_avg'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'wind_direction_avg', lower=1, upper=360)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Current_':
        df = pd.read_csv(file_path, skiprows=1)
        data_cols = list(df.columns[2:])
        float_cols = [d for d in data_cols if d not in ['ADCP_status_code', 'data_adcp(1)']]
        df_all = load_data(file_path, data_cols, float_cols)

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'signature_100',
            'tag_edge_device': 'cr6',
            'tag_platform': 'munkholmen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_current_speed_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('current_speed')}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_current_direction_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('current_direction')}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_amplitude_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('amplitude')}
        tag_values['tag_unit'] = 'db'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_correlation_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('correlation')}
        tag_values['tag_unit'] = 'percent'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_velocity_munkholmen'
        field_keys = {i: i for i in data_cols if i.startswith('velocity')}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_battery_voltage_munkholmen'
        field_keys = {'ADCP_battery_voltage': 'battery_voltage'}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_error_code_munkholmen'
        field_keys = {'ADCP_error_code': 'error_code'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_heading_munkholmen'
        field_keys = {'ADCP_heading': 'heading'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_pitch_munkholmen'
        field_keys = {'ADCP_pitch': 'pitch'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_pressure_munkholmen'
        field_keys = {'ADCP_pressure': 'pressure'}
        tag_values['tag_unit'] = 'dbar'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_roll_munkholmen'
        field_keys = {'ADCP_Roll': 'roll'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_sound_speed_munkholmen'
        field_keys = {'ADCP_sound_speed': 'sound_speed'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_temperature_munkholmen'
        field_keys = {'ADCP_temperature': 'temperature'}
        tag_values['tag_unit'] = 'degrees_celsius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_configuration_munkholmen'
        tag_values['tag_unit'] = 'none'
        df = pd.DataFrame(columns=['identifier', 'instrument_type_id', 'num_beams', 'num_cells', 'checksum'])
        for i in range(df_all.shape[0]):
            d = df_all.iloc[i, -1]
            indexes = [1, 2, 3, 4, 7]
            df.loc[i, :] = [d.split(',')[j] for j in indexes]
        df.index = df_all.index
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_depth_config_munkholmen'
        tag_values['tag_unit'] = 'metres'
        df = pd.DataFrame(columns=['blanking', 'cell_size'])
        for i in range(df_all.shape[0]):
            d = df_all.iloc[i, -1]
            indexes = [5, 6]
            df.loc[i, :] = [d.split(',')[j] for j in indexes]
        df.index = df_all.index
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'CR6_EOL2p0_Wave_sensor_':

        data_cols = [
            "heading", "Hs", "Period", "Hmax", "direction"
        ]
        float_cols = data_cols
        df_all = load_data(file_path, data_cols, float_cols)  # , rows_to_skip=2, time_col="TIMESTAMP"

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'seaview',
            'tag_edge_device': 'cr6',
            'tag_platform': 'munkholmen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_heading_munkholmen'
        field_keys = {"heading": 'heading'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'heading', lower=0, upper=360)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hs_munkholmen'
        field_keys = {"Hs": 'hs'}
        tag_values['tag_unit'] = 'metres'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'hs', lower=-100, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_period_munkholmen'
        field_keys = {"Period": 'period'}
        tag_values['tag_unit'] = 'seconds'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'period', lower=0, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hmax_munkholmen'
        field_keys = {"Hmax": 'hmax'}
        tag_values['tag_unit'] = 'metres'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'hmax', lower=-100, upper=100)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_direction_munkholmen'
        field_keys = {"direction": 'direction'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        df = processing.constant_val_filter(df, 'direction', lower=0, upper=360)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_System_':

        data_cols = [
            "systemTemperature", "systemAirPressure", "systemRelHumidity",
            "victron_BattVolts", "victron_ChargeCurr", "victron_PanelVolts",
            "LoggerVoltage", "LoggerTemperature"
        ]
        float_cols = data_cols
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'none',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'system_temperature_ingdalen'
        field_keys = {"systemTemperature": 'system_temperature'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'system_atmospheric_pressure_ingdalen'
        field_keys = {"systemAirPressure": 'atmospheric_pressure'}
        tag_values['tag_unit'] = 'hecto_pascal'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'system_humidity_ingdalen'
        field_keys = {"systemRelHumidity": 'humidity'}
        tag_values['tag_unit'] = 'percent'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_battery_voltage_ingdalen'
        field_keys = {"victron_BattVolts": 'battery_voltage'}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_charge_current_ingdalen'
        field_keys = {"victron_ChargeCurr": 'charge_current'}
        tag_values['tag_unit'] = 'amperes'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_panel_voltage_ingdalen'
        field_keys = {"victron_PanelVolts": 'panel_voltage'}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_logger_voltage_ingdalen'
        field_keys = {"LoggerVoltage": 'logger_voltage'}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_logger_temperature_ingdalen'
        field_keys = {"LoggerTemperature": 'logger_temperature'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_victron_':

        # "TMSTAMP","RECNBR","victron_Device","victron_SER","victron_FW","victron_BattVolts","victron_ChargeCurr","victron_PanelVolts","victron_PanelPower","victron_State","victron_ERR"
        # "2022-05-31 06:30:00",346,"BlueSolar MPPT 100|30 rev2","HQ2102P4VGZ ",147,12.53,7.4,19.84,96,"Bulk Charging","Device OK"

        data_cols = [
            "victron_Device", "victron_SER", "victron_FW", "victron_BattVolts",
            "victron_ChargeCurr", "victron_PanelVolts", "victron_PanelPower",
            "victron_State", "victron_ERR"
        ]
        float_cols = [c for c in data_cols if c not in ['victron_Device', 'victron_SER', "victron_State", "victron_ERR"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_fw_ingdalen'
        field_keys = {"victron_Device": 'tag_sensor',
                      "victron_SER": 'tag_serial',
                      "victron_FW": 'fw'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # The below three vars (victron_BattVolts, victron_PanelVolts, victron_ChargeCurr)
        # are also included in the system table.
        # # ---------------------------------------------------------------- #
        # measurement_name = 'victron_voltages_ingdalen'
        # field_keys = {"victron_Device": 'tag_sensor',
        #               "victron_SER": 'tag_serial',
        #               "victron_BattVolts": 'battery_voltage',
        #               "victron_PanelVolts": 'panel_voltage'}
        # tag_values['tag_unit'] = 'volts'
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # # ---------------------------------------------------------------- #
        # measurement_name = 'victron_charge_current_ingdalen'
        # field_keys = {"victron_Device": 'tag_sensor',
        #               "victron_SER": 'tag_serial',
        #               "victron_ChargeCurr": 'charge_current'}
        # tag_values['tag_unit'] = 'amperes'
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_panel_power_ingdalen'
        field_keys = {"victron_Device": 'tag_sensor',
                      "victron_SER": 'tag_serial',
                      "victron_PanelPower": 'panel_power'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'victron_messages_ingdalen'
        field_keys = {"victron_Device": 'tag_sensor',
                      "victron_SER": 'tag_serial',
                      "victron_State": 'state',
                      "victron_ERR": 'error'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_SUNA_':

        data_cols = [
            "sunaSerial", "sunaNitrateMicroMol", "sunaNitrateMilliGrams",
            "sunaInternalHumidity", "sunaTemperatureHousing"
        ]
        float_cols = [c for c in data_cols if c not in ["sunaSerial"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'suna',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'suna_nitrate_micromol_ingdalen'
        field_keys = {"sunaSerial": 'tag_serial',
                      "sunaNitrateMicroMol": 'nitrate_micromol'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'suna_nitrate_milligram_ingdalen'
        field_keys = {"sunaSerial": 'tag_serial',
                      "sunaNitrateMilliGrams": 'nitrate_milligram'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'suna_internal_humidity_ingdalen'
        field_keys = {"sunaSerial": 'tag_serial',
                      "sunaInternalHumidity": 'humidity'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'suna_housing_temperature_ingdalen'
        field_keys = {"sunaSerial": 'tag_serial',
                      "sunaTemperatureHousing": 'temperature'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_signatureRecord_':

        data_cols = [
            "signatureDataType", "signatureDataTypeString", "signatureSerialNumber",
            "signatureConfiguration", "signatureSoundVelocity", "signatureTemperature",
            "signaturePressure", "signatureHeading", "signaturePitch", "signatureRoll",
            "signatureError", "signatureStatus0", "signatureStatus", "signatureCells",
            "signatureBeams", "signatureCellSize", "signatureBlanking", "signatureBattery",
            "signatureNominalCorrelation", "signatureAmbiguityVelocity", "signatureEchoFrequency"
        ]
        float_cols = [c for c in data_cols if c not in ["signatureDataTypeString"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'signature_100',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_meta_data_ingdalen'
        field_keys = {"signatureDataType": 'data_type',
                      "signatureDataTypeString": 'data_type_string',
                      "signatureSerialNumber": 'tag_serial_number',
                      "signatureConfiguration": 'configuration'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_sound_velocity_ingdalen'
        field_keys = {"signatureSoundVelocity": 'sound_velocity'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_temperature_ingdalen'
        field_keys = {"signatureTemperature": 'temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_pressure_ingdalen'
        field_keys = {"signaturePressure": 'pressure'}
        tag_values['tag_unit'] = 'none'  # atmospheres
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_heading_ingdalen'
        field_keys = {"signatureHeading": 'heading'}
        tag_values['tag_unit'] = 'none'  # degrees
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_pitch_ingdalen'
        field_keys = {"signaturePitch": 'pitch'}
        tag_values['tag_unit'] = 'none'  # degrees
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_roll_ingdalen'
        field_keys = {"signatureRoll": 'roll'}
        tag_values['tag_unit'] = 'none'  # degrees
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_status_ingdalen'
        field_keys = {"signatureError": 'error',
                      "signatureStatus0": 'status0',
                      "signatureStatus": 'status'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_cells_ingdalen'
        field_keys = {"signatureCells": 'cells'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_beams_ingdalen'
        field_keys = {"signatureBeams": 'beams'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_cell_size_ingdalen'
        field_keys = {"signatureCellSize": 'cell_size'}
        tag_values['tag_unit'] = 'none'  # metres 5
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_blanking_ingdalen'
        field_keys = {"signatureBlanking": 'blanking'}
        tag_values['tag_unit'] = 'none'  # metres 2
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_battery_ingdalen'
        field_keys = {"signatureBattery": 'battery'}
        tag_values['tag_unit'] = 'none'  # volts 23.4
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_battery_ingdalen'
        field_keys = {"signatureBattery": 'battery'}
        tag_values['tag_unit'] = 'none'  # volts 23.4
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_nominal_correlation_ingdalen'
        field_keys = {"signatureNominalCorrelation": 'nominal_correlation'}
        tag_values['tag_unit'] = 'none'  # percent? 82
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_ambiguity_velocity_ingdalen'
        field_keys = {"signatureAmbiguityVelocity": 'ambiguity_velocity'}
        tag_values['tag_unit'] = 'none'  # metres_per_second 10.39
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_echo_frequency_ingdalen'
        field_keys = {"signatureEchoFrequency": 'echo_frequency'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # # ==================================================================== #
    # if file_type == 'IngdalenCR6_signatureCurrentProf_':

    # # This is in a weird format with new series each file (check RECNBR)

    #     data_cols = [
    #         "signatureCellDistProfile",
    #         "signatureVelocityProfile(1)", "signatureVelocityProfile(2)", "signatureVelocityProfile(3)", "signatureVelocityProfile(4)",
    #         "signatureAmplitudeProfile(1)", "signatureAmplitudeProfile(2)", "signatureAmplitudeProfile(3)", "signatureAmplitudeProfile(4)",
    #         "signatureCorrelationProfile(1)", "signatureCorrelationProfile(2)", "signatureCorrelationProfile(3)", "signatureCorrelationProfile(4)"
    #     ]
    #     float_cols = [c for c in data_cols if c not in ["signatureDataTypeString"]]
    #     df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

    #     # Set a 'default' set of tags for this file:
    #     tag_values = {
    #         'tag_sensor': 'signature_100',
    #         'tag_edge_device': 'cr6_ingdalen',
    #         'tag_platform': 'ingdalen',
    #         'tag_data_level': 'raw',
    #         'tag_approved': 'none',
    #         'tag_unit': 'none'}

    # ==================================================================== #
    if file_type == 'IngdalenCR6_Seabird_':

        data_cols = [
            "seabirdDevice", "seabirdSerial", "seabirdBattery", "seabirdTemperature", "seabirdConductivity",
            "seabirdPressure", "seabirdDissOxygen", "seabirdSalinity", "seabirdSoundVel", "seabirdSpecCond"
        ]
        float_cols = [c for c in data_cols if c not in ["seabirdDevice"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            # 'tag_sensor': 'seabird',  # this will be the 'seabirdDevice'
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_battery_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdBattery": 'battery'}
        tag_values['tag_unit'] = 'none'  # volts 13.63
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_temperature_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdTemperature": 'temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'  # 8.5882
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_conductivity_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdConductivity": 'conductivity'}
        tag_values['tag_unit'] = 'none'  # ? 31.6308
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_pressure_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdPressure": 'pressure'}
        tag_values['tag_unit'] = 'none'  # ? 10.431
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_dissolved_oxygen_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdDissOxygen": 'dissolved_oxygen'}
        tag_values['tag_unit'] = 'none'  # ? 9.087
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_salinity_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdSalinity": 'salinity'}
        tag_values['tag_unit'] = 'none'  # ? 29.6189
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_sound_velocity_ingdalen'
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdSoundVel": 'sound_velocity'}
        tag_values['tag_unit'] = 'metres_per_second'  # ? 1478.221
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'seabird_spec_cond_ingdalen'  # ???
        field_keys = {"seabirdDevice": 'tag_sensor',
                      "seabirdSerial": 'tag_serial',
                      "seabirdSpecCond": 'spec_cond'}
        tag_values['tag_unit'] = 'none'  # ? 47.0862
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_Power_':

        data_cols = [
            "powerState(1)", "powerState(2)", "powerState(3)", "powerState(4)", "powerState(5)", "powerState(6)", "powerState(7)", "powerState(8)", "powerState(9)", "powerState(10)", "powerState(11)",
            "powerVoltage(1)", "powerVoltage(2)", "powerVoltage(3)", "powerVoltage(4)", "powerVoltage(5)", "powerVoltage(6)", "powerVoltage(7)", "powerVoltage(8)", "powerVoltage(9)", "powerVoltage(10)", "powerVoltage(11)",
            "powerCurrent(1)", "powerCurrent(2)", "powerCurrent(3)", "powerCurrent(4)", "powerCurrent(5)", "powerCurrent(6)", "powerCurrent(7)", "powerCurrent(8)", "powerCurrent(9)", "powerCurrent(10)", "powerCurrent(11)",
            "muxVoltage(1)", "muxVoltage(2)", "muxVoltage(3)",
            "muxCurrent(1)", "muxCurrent(2)", "muxCurrent(3)",
            "muxTemperature(1)", "muxTemperature(2)", "muxTemperature(3)"
        ]
        float_cols = [c for c in data_cols if c not in ["seabirdDevice"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'none',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'power_state_ingdalen'
        field_keys = {f'powerState({i})': f'power_state_{i}' for i in range(1, 12)}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_voltage_ingdalen'
        field_keys = {f'powerVoltage({i})': f'power_voltage_{i}' for i in range(1, 12)}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'power_current_ingdalen'
        field_keys = {f'powerCurrent({i})': f'power_current_{i}' for i in range(1, 12)}
        tag_values['tag_unit'] = 'amperes'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'mux_voltage_ingdalen'
        field_keys = {f'muxVoltage({i})': f'mux_voltage_{i}' for i in range(1, 4)}
        tag_values['tag_unit'] = 'volts'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'mux_current_ingdalen'
        field_keys = {f'muxCurrent({i})': f'mux_current_{i}' for i in range(1, 4)}
        tag_values['tag_unit'] = 'amperes'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'mux_temperature_ingdalen'
        field_keys = {f'muxTemperature({i})': f'mux_temperature_{i}' for i in range(1, 4)}
        tag_values['tag_unit'] = 'degrees_celcius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_PAR_':

        data_cols = [
            "parSrfSerial", "parSubSerial",
            "parSrfLive", "parSrfAvg", "parSrfPitch", "parSrfRoll", "parSrfTemp",
            "parSubLive", "parSubAvg", "parSubPitch", "parSubRoll", "parSubTemp"
        ]
        float_cols = [c for c in data_cols if c not in ["parSrfSerial", "parSubSerial"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'par',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # # ---------------------------------------------------------------- #
        # measurement_name = 'par_surface_serial_ingdalen'
        # field_keys = {"parSrfSerial": 'surface_serial'}
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # # ---------------------------------------------------------------- #
        # measurement_name = 'par_subsea_serial_ingdalen'
        # field_keys = {"parSubSerial": 'subsea_serial'}
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_surface_live_ingdalen'
        field_keys = {"parSrfSerial": 'tag_serial',
                      "parSrfLive": 'par_surface_live'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_subsea_live_ingdalen'
        field_keys = {"parSubSerial": 'tag_serial',
                      "parSubLive": 'par_subsea_live'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_surface_average_ingdalen'
        field_keys = {"parSrfSerial": 'tag_serial',
                      "parSrfAvg": 'par_surface_average'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_subsea_average_ingdalen'
        field_keys = {"parSubSerial": 'tag_serial',
                      "parSubAvg": 'par_subsea_average'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_surface_orientation_ingdalen'
        field_keys = {"parSrfSerial": 'tag_serial',
                      "parSrfPitch": 'par_surface_pitch',
                      "parSrfRoll": 'par_surface_roll'}
        tag_values['tag_unit'] = 'none'  # ? 0.6
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_subsea_orientation_ingdalen'
        field_keys = {"parSubSerial": 'tag_serial',
                      "parSubPitch": 'par_subsea_pitch',
                      "parSubRoll": 'par_subsea_roll'}
        tag_values['tag_unit'] = 'none'  # ? -1.5
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_surface_temperature_ingdalen'
        field_keys = {"parSrfSerial": 'tag_serial',
                      "parSrfTemp": 'par_surface_temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'  # ? 15.3
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'par_subsea_temperature_ingdalen'
        field_keys = {"parSubSerial": 'tag_serial',
                      "parSubTemp": 'par_subsea_temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'  # ? 13.7
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_MetData_':

        data_cols = [
            "avgWindSpeed", "avgWindDir", "gustWindSpeed", "gustWindDir",
            "maximetTemperature", "maximetPressure", "maximetHumidity", "maximetSolar"
        ]
        float_cols = [c for c in data_cols if c not in ["parSrfSerial", "parSubSerial"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'none',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_speed_ingdalen'
        field_keys = {"avgWindSpeed": 'wind_speed'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_wind_direction_ingdalen'
        field_keys = {"avgWindDir": 'wind_direction'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_gust_speed_ingdalen'
        field_keys = {"gustWindSpeed": 'gust_speed'}
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_gust_direction_ingdalen'
        field_keys = {"gustWindDir": 'gust_direction'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_maximet_temperature_ingdalen'
        field_keys = {"maximetTemperature": 'maximet_temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_maximet_pressure_ingdalen'
        field_keys = {"maximetPressure": 'maximet_pressure'}
        tag_values['tag_unit'] = 'hecto_pascal'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_maximet_humidity_ingdalen'
        field_keys = {"maximetHumidity": 'maximet_humidity'}
        tag_values['tag_unit'] = 'percent'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'meteo_maximet_solar_ingdalen'
        field_keys = {"maximetSolar": 'maximet_solar'}
        tag_values['tag_unit'] = 'none'  # ? 896
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_Hydrocat_':

        data_cols = [
            "hydrocatSerial", "hydrocatTemperature", "hydrocatConductivity", "hydrocatPressure",
            "hydrocatDissOxygen", "hydrocatSalinity", "hydrocatSoundVel", "hydrocatSpecCond",
            "hydrocatFluorescence", "hydrocatTurbidity", "hydrocatPH", "hydrocatOxygenSaturation"
        ]
        float_cols = [c for c in data_cols if c not in ["hydrocatSerial"]]
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'hydrocat',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # # ---------------------------------------------------------------- #
        # measurement_name = 'hydrocat_serial_ingdalen'
        # field_keys = {"hydrocatSerial": 'serial'}
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_temperature_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatTemperature": 'temperature'}
        tag_values['tag_unit'] = 'degrees_celcius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_conductivity_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatConductivity": 'conductivity'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_pressure_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatPressure": 'pressure'}
        tag_values['tag_unit'] = 'atmospheres'  # 0.968 ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_dissolved_oxygen_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatDissOxygen": 'dissolved_oxygen'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_salinity_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatSalinity": 'salinity'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_sound_velocity_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatSoundVel": 'sound_velocity'}
        tag_values['tag_unit'] = 'degrees_celcius'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_spec_cond_ingdalen'  # ?
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatSpecCond": 'spec_cond'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_fluorescence_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatFluorescence": 'fluorescence'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_turbidity_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatTurbidity": 'turbidity'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_ph_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatPH": 'ph'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'hydrocat_oxygen_saturation_ingdalen'
        field_keys = {"hydrocatSerial": 'tag_serial',
                      "hydrocatOxygenSaturation": 'oxygen_saturation'}
        tag_values['tag_unit'] = 'none'  # ?
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_GPSData_':

        data_cols = [
            "Latitude", "Longitude", "DOP", "Sats", "PositionDev"
        ]
        float_cols = data_cols
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'none',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'gps_position_ingdalen'
        field_keys = {"Latitude": 'latitude',
                      "Longitude": 'longitude'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'gps_dop_ingdalen'
        field_keys = {"DOP": 'dop'}
        tag_values['tag_unit'] = 'none'  # ? 1.2
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'gps_sats_ingdalen'
        field_keys = {"Sats": 'sats'}
        tag_values['tag_unit'] = 'none'  # ? 7
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'gps_position_displacement_ingdalen'
        field_keys = {"PositionDev": 'position_displacement'}
        tag_values['tag_unit'] = 'metres'  # ? 100.0999
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_Debug_':

        data_cols = [
            "debugMessage"
        ]
        float_cols = []
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'none',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'debug_log_ingdalen'
        field_keys = {"debugMessage": 'debug_log'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_CFluor_':

        data_cols = [
            "CFluor_Model", "CFluor_Serial", "CFluor_CDOM"
        ]
        # Needed to explicitly state that the str cols listed were such as they
        # can be empty, and then they are read as floats by pandas and this causes
        # as error (as they are np.nans then).
        df_all = load_data(
            file_path, data_cols, ["CFluor_CDOM"],
            str_cols=["CFluor_Model", "CFluor_Serial"], timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            # 'tag_sensor': 'none',  # Replaced by the 'CFlour_Model' var.
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # # ---------------------------------------------------------------- #
        # measurement_name = 'cflour_model_ingdalen'
        # field_keys = {"CFluor_Model": 'cflour_model'}
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # # ---------------------------------------------------------------- #
        # measurement_name = 'cflour_serial_ingdalen'
        # field_keys = {"CFluor_Serial": 'cflour_serial'}
        # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        # util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'cflour_cdom_ingdalen'
        field_keys = {"CFluor_Model": 'tag_sensor',
                      "CFluor_Serial": 'tag_serial',
                      "CFluor_CDOM": 'cflour_cdom'}
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_Wave_':

        data_cols = [
            "Hs", "DominantPeriodFW", "WaveDirectionFW", "MeanWaveDirection", "Hmax", "PavgTE", "maxAccX", "maxAccY", "maxAccZ"
        ]
        float_cols = data_cols
        df_all = load_data(
            file_path, data_cols, float_cols, timezone='UTC')

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'ingdalen_wave_sensor',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hs_ingdalen'
        field_keys = {"Hs": 'hs'}
        tag_values['tag_unit'] = 'metres'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_hmax_ingdalen'
        field_keys = {"Hmax": 'hmax'}
        tag_values['tag_unit'] = 'metres'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_period_ingdalen'
        field_keys = {"DominantPeriodFW": 'dominant_period_fw',
                      "PavgTE": 'p_avg_te'}
        tag_values['tag_unit'] = 'seconds'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_direction_ingdalen'
        field_keys = {"WaveDirectionFW": 'wave_direction_fw',
                      "MeanWaveDirection": 'mean_wave_direction'}
        tag_values['tag_unit'] = 'degrees'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'wave_acceleration_ingdalen'
        field_keys = {"maxAccX": 'max_acc_x',
                      "maxAccY": 'max_acc_y',
                      "maxAccZ": 'max_acc_z'}
        tag_values['tag_unit'] = 'none'
        df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

    # ==================================================================== #
    if file_type == 'IngdalenCR6_signatureCurrentProf_':

        data_cols = [
            "signatureCellDistProfile",
            "signatureVelocityProfile(1)", "signatureVelocityProfile(2)", "signatureVelocityProfile(3)", "signatureVelocityProfile(4)",
            "signatureAmplitudeProfile(1)", "signatureAmplitudeProfile(2)", "signatureAmplitudeProfile(3)", "signatureAmplitudeProfile(4)",
            "signatureCorrelationProfile(1)", "signatureCorrelationProfile(2)", "signatureCorrelationProfile(3)", "signatureCorrelationProfile(4)"
        ]
        float_cols = data_cols
        # This file will now be "2D" with many rows, each being a different depth. Note also
        # this can't ever contain more than one time point.
        df_all = load_data(file_path, data_cols, float_cols, timezone='UTC')
        n_bins = df_all.shape[0]
        idx = df_all.index[0]  # All vals assumed to be the same.

        # Set a 'default' set of tags for this file:
        tag_values = {
            'tag_sensor': 'signature_100',
            'tag_edge_device': 'cr6_ingdalen',
            'tag_platform': 'ingdalen',
            'tag_data_level': 'raw',
            'tag_approved': 'none',
            'tag_unit': 'none'}

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_depth_ingdalen'
        cols = []
        for i in range(n_bins):
            cols.append('depth_' + str(i).zfill(3))
        data = df_all.loc[:, 'signatureCellDistProfile'].values.reshape((1, n_bins))
        df = pd.DataFrame(data=data, index=[idx], columns=cols)
        tag_values['tag_unit'] = 'metres'
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_velocity_ingdalen'
        cols = []
        data = np.zeros((1, n_bins * 4))
        for i in range(4):
            for j in range(n_bins):
                cols.append(f'velocity{i + 1}_' + str(j).zfill(3))
        for i in range(4):
            data[0, i * n_bins: (i + 1) * n_bins] = df_all.loc[:, f'signatureVelocityProfile({i + 1})'].values.reshape((1, n_bins))
        df = pd.DataFrame(data=data, index=[idx], columns=cols)
        tag_values['tag_unit'] = 'metres_per_second'
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_amplitude_ingdalen'
        cols = []
        data = np.zeros((1, n_bins * 4))
        for i in range(4):
            for j in range(n_bins):
                cols.append(f'amplitude{i + 1}_' + str(j).zfill(3))
        for i in range(4):
            data[0, i * n_bins: (i + 1) * n_bins] = df_all.loc[:, f'signatureAmplitudeProfile({i + 1})'].values.reshape((1, n_bins))
        df = pd.DataFrame(data=data, index=[idx], columns=cols)
        tag_values['tag_unit'] = 'none'
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)

        # ---------------------------------------------------------------- #
        measurement_name = 'signature_100_correlation_ingdalen'
        cols = []
        data = np.zeros((1, n_bins * 4))
        for i in range(4):
            for j in range(n_bins):
                cols.append(f'correlation{i + 1}_' + str(j).zfill(3))
        for i in range(4):
            data[0, i * n_bins: (i + 1) * n_bins] = df_all.loc[:, f'signatureCorrelationProfile({i + 1})'].values.reshape((1, n_bins))
        df = pd.DataFrame(data=data, index=[idx], columns=cols)
        tag_values['tag_unit'] = 'none'
        df = util_db.add_tags(df, tag_values)
        util_db.ingest_df(measurement_name, df, clients)
