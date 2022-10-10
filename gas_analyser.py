import os
import numpy as np
import pandas as pd
import seawater
import xmltodict

import sensor
import config
import util_db
import util_file

logger = util_file.init_logger(config.main_logfile, name='olmo.gasanalyser')


class GasAnalyser(sensor.Sensor):
    def __init__(
            self,
            data_dir=f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA/gas',
            recursive_file_search_l0=True,
            file_search_l0='gga_????-??-??_f????.txt',
            drop_recent_files_l0=0,
            remove_remote_files_l0=False,  # True
            max_files_l0=1,  # None
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(GasAnalyser, self).__init__()
        self.data_dir = data_dir
        self.recursive_file_search_l0 = recursive_file_search_l0
        self.file_search_l0 = file_search_l0
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.max_files_l0 = max_files_l0
        self.influx_clients = influx_clients

    def ingest_l0(self, files):

        for f in files:
            print(f)
            df_all = pd.read_csv(f, sep=',', skiprows=1)

            for c in df_all.columns:
                print("'" + c + "'")
            # print(df_all.head())

            time_col = '                     Time'
            df_all = util_db.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
            df_all[time_col] = pd.to_datetime(df_all[time_col], format='  %d/%m/%Y %H:%M:%S.%f')
            df_all = df_all.set_index(time_col).tz_localize('CET', ambiguous='infer').tz_convert('UTC')

            print(df_all.head())

            # tag_values = {'tag_sensor': 'ctd',
            #               'tag_serial': '12345'
            #               'tag_edge_device': 'munkholmen_topside_pi',
            #               'tag_platform': 'munkholmen',
            #               'tag_data_level': 'raw',
            #               'tag_approved': 'no',
            #               'tag_unit': 'none'}

            # # ------------------------------------------------------------ #
            # measurement_name = 'ctd_temperature_munkholmen'
            # field_keys = {"Temperature": 'temperature'}
            # tag_values['tag_unit'] = 'degrees_celcius'
            # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            # # util_db.ingest_df(measurement_name, df, self.influx_clients)

            # # ------------------------------------------------------------ #
            # measurement_name = 'ctd_conductivity_munkholmen'
            # field_keys = {"Conductivity": 'conductivity'}
            # tag_values['tag_unit'] = 'siemens_per_metre'
            # df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            # # util_db.ingest_df(measurement_name, df, self.influx_clients)

            # logger.info(f'File {f} ingested.')

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('ctd.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])

        logger.info('ctd.rsync_and_ingest() finished.')
