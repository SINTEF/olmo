import os
import numpy as np
import pandas as pd
import seawater
import xmltodict

import sensor
import config
import util_db
import util_file

logger = util_file.init_logger(config.main_logfile, name='olmo.ctd')


class CTD(sensor.Sensor):
    def __init__(self, influx_clients=None):
        # Init the Sensor() class: This sets some defaults.
        super(CTD, self).__init__()
        self.influx_clients = influx_clients
        self.data_dir = f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA'
        self.file_search_l0 = r"ready_ctd_(\d{14})\.csv"
        self.drop_recent_files_l0 = 0
        self.remove_remote_files_l0 = True
        self.max_files_l0 = None

        # Some constants needed for calculations:
        self.MUNKHOLMEN_LATITUDE = 63.456314
        self.ABSZERO = 273.15
        self.PH_CONSTANT = 1.98416e-4

    def load_calibration(self, path=os.path.join(config.base_dir, 'olmo', 'sensor_calibration', '19-8154.xmlcon')):
        with open(path, 'r') as f:
            calibfile = xmltodict.parse(f.read())
        self.calibration = calibfile['SBE_InstrumentConfiguration']['Instrument']['SensorArray']['Sensor']

    def calcpH(self, temp, pHvout):
        phslope = float(self.calibration[3]['pH_Sensor']['Slope'])
        phoffset = float(self.calibration[3]['pH_Sensor']['Offset'])
        ktemp = self.ABSZERO + temp
        ph = 7 + (pHvout - phoffset) / (phslope * ktemp * self.PH_CONSTANT)
        return ph

    def calcCDOM(self, CDOMvout):
        scalefactor = float(self.calibration[4]['FluoroWetlabCDOM_Sensor']['ScaleFactor'])
        vblank = float(self.calibration[4]['FluoroWetlabCDOM_Sensor']['Vblank'])
        CDOM = scalefactor * (CDOMvout - vblank)
        return CDOM

    def calcPAR(self, PARvout):
        PAR_a0 = float(self.calibration[5]['PARLog_SatlanticSensor']['a0'])
        PAR_a1 = float(self.calibration[5]['PARLog_SatlanticSensor']['a1'])
        Im = float(self.calibration[5]['PARLog_SatlanticSensor']['Im'])
        PAR = Im * 10 ** ((PARvout - PAR_a0) / PAR_a1)
        return PAR

    def calcchl(self, chlvout):
        scalefactor = float(self.calibration[6]['FluoroWetlabECO_AFL_FL_Sensor']['ScaleFactor'])
        vblank = float(self.calibration[6]['FluoroWetlabECO_AFL_FL_Sensor']['Vblank'])
        chl = scalefactor * (chlvout - vblank)
        return chl

    def calcNTU(self, NTUvout):
        scalefactor = float(self.calibration[7]['TurbidityMeter']['ScaleFactor'])
        vblank = float((self.calibration[7]['TurbidityMeter']['DarkVoltage']))
        NTU = scalefactor * (NTUvout - vblank)
        return NTU

    def calcDO_T(self, V):
        TA0 = float(self.calibration[8]['OxygenSensor']['TA0'])
        TA1 = float(self.calibration[8]['OxygenSensor']['TA1'])
        TA2 = float(self.calibration[8]['OxygenSensor']['TA2'])
        TA3 = float(self.calibration[8]['OxygenSensor']['TA3'])

        def calcL(V):
            L = np.log((100000 * V) / (3.3 - V))
            return L

        L = calcL(V)
        T = 1 / (TA0 + (TA1 * L) + (TA2 * L**2) + (TA3 * L**3)) - self.ABSZERO
        return T

    def calcDO(self, DOphase, T, S, P):
        # manual-53_011 p47
        A0 = float(self.calibration[8]['OxygenSensor']['A0'])
        A1 = float(self.calibration[8]['OxygenSensor']['A1'])
        A2 = float(self.calibration[8]['OxygenSensor']['A2'])
        B0 = float(self.calibration[8]['OxygenSensor']['B0'])
        B1 = float(self.calibration[8]['OxygenSensor']['B1'])
        C0 = float(self.calibration[8]['OxygenSensor']['C0'])
        C1 = float(self.calibration[8]['OxygenSensor']['C1'])
        C2 = float(self.calibration[8]['OxygenSensor']['C2'])

        def calcSalcorr(T, S):
            Ts = np.log((298.15 - T) / (self.ABSZERO + T))
            SolB0 = -6.24523e-3
            SolB1 = -7.37614e-3
            SolB2 = -1.03410e-2
            SolB3 = -8.17083e-3
            SolC0 = -4.88682e-7
            Scorr = np.exp(S * (SolB0 + SolB1 * Ts + SolB2 * Ts**2 + SolB3 * Ts**3) + SolC0 * S**2)
            return Scorr

        def calcPcorr(T, P):
            E = 0.011
            K = self.ABSZERO + T
            Pcorr = np.exp(E * P / K)
            return Pcorr

        # Divide the phase delay output (μsec) by 39.457071 to get output in volts, and use the output in volts in the calibration equation.
        V = DOphase / 39.457071

        Pcorr = calcPcorr(T, P)
        Scorr = calcSalcorr(T, S)

        Ksv = (C0 + C1 * T + C2 * T**2)

        DO = (((A0 + A1 * T + A2 * V**2) / (B0 + B1 * V) - 1) / Ksv) * Scorr * Pcorr
        return DO

    def ingest_l0(self, files):

        for f in files:
            df_all = pd.read_csv(f, sep=',')

            time_col = 'Timestamp'
            df_all = util_db.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
            df_all[time_col] = pd.to_datetime(df_all[time_col], format='%Y-%m-%d %H:%M:%S')
            df_all = df_all.set_index(time_col).tz_localize('CET', ambiguous='infer').tz_convert('UTC')

            df_all['density'] = seawater.eos80.dens0(df_all['Salinity'], df_all['Temperature'])
            df_all['depth'] = seawater.eos80.dpth(df_all['Pressure'], self.MUNKHOLMEN_LATITUDE)

            tag_values = {'tag_sensor': 'ctd',
                          'tag_edge_device': 'munkholmen_topside_pi',
                          'tag_platform': 'munkholmen',
                          'tag_data_level': 'raw',
                          'tag_approved': 'no',
                          'tag_unit': 'none'}

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_temperature_munkholmen'
            field_keys = {"Temperature": 'temperature'}
            tag_values['tag_unit'] = 'degrees_celcius'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_conductivity_munkholmen'
            field_keys = {"Conductivity": 'conductivity'}
            tag_values['tag_unit'] = 'siemens_per_metre'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_pressure_munkholmen'
            field_keys = {"Pressure": 'pressure'}
            tag_values['tag_unit'] = 'none'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # -------------drop----------------------------------------------- #
            measurement_name = 'ctd_sbe63_munkholmen'
            field_keys = {"SBE63": 'sbe63',
                          "SBE63Temperature": 'sbe63_temperature_voltage'}
            tag_values['tag_unit'] = 'none'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_salinity_munkholmen'
            field_keys = {"Salinity": 'salinity'}
            tag_values['tag_unit'] = 'none'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_voltages_munkholmen'
            field_keys = {"Volt0": 'volt0',
                          "Volt1": 'volt1',
                          "Volt2": 'volt2',
                          "Volt4": 'volt4',
                          "Volt5": 'volt5'}
            tag_values['tag_unit'] = 'none'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_depth_munkholmen'
            field_keys = {"depth": 'depth'}
            tag_values['tag_unit'] = 'metres'
            tag_values['tag_data_level'] = 'processed'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_density_munkholmen'
            field_keys = {"density": 'density'}
            tag_values['tag_unit'] = 'kilograms_per_cubic_metre'
            tag_values['tag_data_level'] = 'processed'
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            logger.info(f'File {f} ingested.')

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('ctd.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])

        logger.info('ctd.rsync_and_ingest() finished.')
