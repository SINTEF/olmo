import os
import time
import pandas as pd
import datetime
from influxdb import InfluxDBClient
import seawater
import xmltodict
import numpy as np

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


with open('19-8154.xmlcon', 'r') as f:
    calibfile = xmltodict.parse(f.read())
sensors = calibfile['SBE_InstrumentConfiguration']['Instrument']['SensorArray']['Sensor']


def calcpH(temp, pHvout):
    phslope = float(sensors[3]['pH_Sensor']['Slope'])
    phoffset = float(sensors[3]['pH_Sensor']['Offset'])

    abszero = 273.15
    ktemp = abszero + temp
    const = 1.98416e-4
    ph = 7 + (pHvout - phoffset) / (phslope * ktemp * const)
    return(ph)


def calcCDOM(CDOMvout):
    scalefactor = float(sensors[4]['FluoroWetlabCDOM_Sensor']['ScaleFactor'])
    vblank = float(sensors[4]['FluoroWetlabCDOM_Sensor']['Vblank'])
    CDOM = scalefactor * (CDOMvout - vblank)
    return CDOM


def calcPAR(PARvout):
    PAR_a0 = float(sensors[5]['PARLog_SatlanticSensor']['a0'])
    PAR_a1 = float(sensors[5]['PARLog_SatlanticSensor']['a1'])
    Im = float(sensors[5]['PARLog_SatlanticSensor']['Im'])
    PAR = Im * 10**((PARvout - PAR_a0) / PAR_a1)
    return PAR


def calcchl(chlvout):
    scalefactor = float(sensors[6]['FluoroWetlabECO_AFL_FL_Sensor']['ScaleFactor'])
    vblank = float(sensors[6]['FluoroWetlabECO_AFL_FL_Sensor']['Vblank'])
    chl = scalefactor * (chlvout - vblank)
    return chl


def calcNTU(NTUvout):
    scalefactor = float(sensors[7]['TurbidityMeter']['ScaleFactor'])
    vblank = float((sensors[7]['TurbidityMeter']['DarkVoltage']))
    NTU = scalefactor * (NTUvout - vblank)
    return NTU


def calcDO_T(V):
    TA0 = float(sensors[8]['OxygenSensor']['TA0'])
    TA1 = float(sensors[8]['OxygenSensor']['TA1'])
    TA2 = float(sensors[8]['OxygenSensor']['TA2'])
    TA3 = float(sensors[8]['OxygenSensor']['TA3'])

    def calcL(V):
        L = np.log((100000 * V) / (3.3 - V))
        return L

    L = calcL(V)
    T = 1 / (TA0 + (TA1 * L) + (TA2 * L**2) + (TA3 * L**3)) - 273.15
    return T


def calcDO(DOphase, T, S, P):
    # manual-53_011 p47
    A0 = float(sensors[8]['OxygenSensor']['A0'])
    A1 = float(sensors[8]['OxygenSensor']['A1'])
    A2 = float(sensors[8]['OxygenSensor']['A2'])
    B0 = float(sensors[8]['OxygenSensor']['B0'])
    B1 = float(sensors[8]['OxygenSensor']['B1'])
    C0 = float(sensors[8]['OxygenSensor']['C0'])
    C1 = float(sensors[8]['OxygenSensor']['C1'])
    C2 = float(sensors[8]['OxygenSensor']['C2'])

    def calcSalcorr(T, S):
        Ts = np.log((298.15 - T) / (273.15 + T))
        SolB0 = -6.24523e-3
        SolB1 = -7.37614e-3
        SolB2 = -1.03410e-2
        SolB3 = -8.17083e-3
        SolC0 = -4.88682e-7
        Scorr = np.exp(S * (SolB0 + SolB1 * Ts + SolB2 * Ts**2 + SolB3 * Ts**3) + SolC0 * S**2)
        return Scorr

    def calcPcorr(T, P):
        E = 0.011
        K = 273.15 + T
        Pcorr = np.exp(E * P / K)
        return Pcorr

    # Divide the phase delay output (μsec) by 39.457071 to get output in volts, and use the output in volts in the calibration equation.
    V = DOphase / 39.457071

    Pcorr = calcPcorr(T, P)
    Scorr = calcSalcorr(T, S)

    Ksv = (C0 + C1 * T + C2 * T**2)

    DO = (((A0 + A1 * T + A2 * V**2) / (B0 + B1 * V) - 1) / Ksv) * Scorr * Pcorr
    return DO


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

        df_all['ph'] = calcpH(df_all['temperature'], df_all['volt0'])
        df_all['cdom'] = calcCDOM(df_all['volt1'])
        df_all['par'] = calcPAR(df_all['volt2'])
        df_all['chl'] = calcchl(df_all['volt4'])
        df_all['ntu'] = calcNTU(df_all['volt5'])
        df_all['dissolved_oxygen_temperature'] = calcDO_T(df_all['sbe63_temperature_voltage'])
        df_all['dissolved_oxygen'] = calcDO(df_all['sbe63'], df_all['dissolved_oxygen_temperature'],
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
