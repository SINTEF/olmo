import pandas as pd
import os
from glob import glob

import sensor
import config
import util_db
import util_file

logger = util_file.init_logger(config.main_logfile, name='olmo.munkholmen_silcam')


class Munkholmen_silcam(sensor.Sensor):
    def __init__(self, influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(Munkholmen_silcam, self).__init__()
        self.data_dir = f'/mnt/raid/iliad_silcam/processed_silcam_data'
        self.file_search_l0 = r"*-TIMESERIES.csv"
        self.drop_recent_files_l0 = 0
        self.remove_remote_files_l0 = False  # not used
        self.max_files_l0 = 0  # not used
        self.influx_clients = influx_clients

    def ingest_l0(self, files):

        print('not implemented!')
        pass

        for f in files:
            df_all = pd.read_csv(f, sep=',')
            print(df_all)

            time_col = 'Time'
            df_all = util_db.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
            df_all[time_col] = pd.to_datetime(df_all[time_col], format='%Y-%m-%d %H:%M:%S')
            df_all = df_all.set_index(time_col).tz_localize('UTC', ambiguous='infer').tz_convert('UTC')

            tag_values = {'tag_sensor': 'silcam',
                          'tag_edge_device': 'munkholmen_lattepanda',
                          'tag_platform': 'munkholmen',
                          'tag_data_level': 'processed',
                          'tag_approved': 'no',
                          'tag_unit': 'micrometre'}

            # ------------------------------------------------------------ #
            measurement_name = 'silcam_d50_munkholmen'
            field_keys = {"D50": 'd50'}
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            logger.info(f'File {f} ingested.')

    def ingest(self):

        #  files = self.fetch_files_list(self.file_search_l0, recursive_file_search, self.drop_recent_files_l0)
        files = glob(os.path.join(self.data_dir, self.file_search_l0))

        print(files)

        logger.info('silcam.rsync() finished.')

        if files is not None:
            self.ingest_l0(files)

        logger.info('silcam.timeseries_ingest() finished.')
