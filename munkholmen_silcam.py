import pandas as pd
import os
from glob import glob

import sensor
import config
import util_db
import util_file
import util_az

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
                          'tag_unit': 'micrometres'}

            # ------------------------------------------------------------ #
            measurement_name = 'silcam_total_d50_munkholmen'
            field_keys = {"D50": 'd50'}
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            tag_values = {'tag_sensor': 'silcam',
                          'tag_edge_device': 'munkholmen_lattepanda',
                          'tag_platform': 'munkholmen',
                          'tag_data_level': 'processed',
                          'tag_approved': 'no',
                          'tag_unit': 'microlitres_per_litre'}

            # ------------------------------------------------------------ #
            measurement_name = 'silcam_total_volume_concentration_munkholmen'
            data_cols = list(df_all.columns[0:52])
            field_keys = {i: i for i in data_cols}
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            logger.info(f'File {f} ingested.')

    def ingest(self):

        #  files = self.fetch_files_list(self.file_search_l0, recursive_file_search, self.drop_recent_files_l0)
        files = glob(os.path.join(self.data_dir, self.file_search_l0))

        logger.info('silcam.rsync() finished.')

        if files is not None:
            self.ingest_l0(files)

        logger.info('silcam.timeseries_ingest() finished.')

    def save_to_azure(self):

        raw_files = glob(os.path.join(self.data_dir, 'done_files/*.silc'))

        batch_directory = os.path.join('munkholmen_silcam',
                                       os.path.basename(raw_files[0])[:16])
        print('batch_directory', batch_directory)

        tag_values = {'tag_sensor': 'silcam',
                      'tag_edge_device': 'munkholmen_lattepanda',
                      'tag_platform': 'munkholmen',
                      'tag_data_level': 'raw',
                      'tag_approved': 'no',
                      'tag_unit': 'none'}
        measurement_name = 'silcam_raw_data_munkholmen'

        for i, file in enumerate(raw_files):
            print('copying file ', i, ' of ', len(raw_files))

            azure_file = os.path.join(batch_directory, os.path.basename(file))
            print('uploading to', azure_file)

            timestamp = pd.to_datetime(os.path.splitext(os.path.basename(file))[0][1:])
            print(timestamp)
            df_all = pd.DataFrame(data=[[batch_directory, timestamp]],
                                  columns=['batch_directory', 'timestamp'])
            time_col = 'timestamp'
            field_keys = {'batch_directory': 'batch_directory'}
            df_all = util_db.force_float_cols(df_all, not_float_cols=['batch_directory', time_col],
                                              error_to_nan=True)
            df_all[time_col] = pd.to_datetime(df_all[time_col], format='%Y-%m-%d %H:%M:%S')
            df_all = df_all.set_index(time_col).tz_localize('UTC', ambiguous='infer').tz_convert('UTC')
            print(df_all)
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            util_az.upload_file(file, azure_file,
                                'oceanlabdlcontainer', content_type='text/plain',
                                token_file='azure_token_silcam')
            print('rename')
            print(self.data_dir)
            try:
                os.mkdir(self.data_dir)
            except OSError as error:
                print(error)
                print('continue anyway')
            try:
                os.mkdir(os.path.join(self.data_dir, os.path.split(batch_directory)[0]))
            except OSError as error:
                print(error)
                print('continue anyway')
            try:
                os.mkdir(os.path.join(self.data_dir, batch_directory))
            except OSError as error:
                print(error)
                print('continue anyway')
            try:
                os.rename(file, os.path.join(self.data_dir, batch_directory, os.path.split(file)[-1]))
            except OSError as error:
                print(error)
                print('continue anyway')

        print('save_to_azure done')
