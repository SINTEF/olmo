import pandas as pd

import sensor
import config
import util_db
import util_file

logger = util_file.init_logger(config.main_logfile, name='olmo.munkholmen_pi')


class Munkholmen_Pi(sensor.Sensor):
    '''Class for rsyncing and ingesting the munkholmen raspberry pi status data.'''
    def __init__(
            self,
            data_dir=f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA',
            file_regex_l0=r"status.csv",
            drop_recent_files_l0=0,
            remove_remote_files_l0=False,
            max_files_l0=None,
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(Munkholmen_Pi, self).__init__()
        self.data_dir = data_dir
        self.file_regex_l0 = file_regex_l0
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.max_files_l0 = max_files_l0
        self.influx_clients = influx_clients

    def ingest_l0(self, files):

        for f in files:
            df_all = pd.read_csv(f, sep=',')

            time_col = 'timestamp'
            df_all = util_db.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
            df_all[time_col] = pd.to_datetime(df_all[time_col], format='%Y-%m-%d %H:%M:%S')
            df_all = df_all.set_index(time_col).tz_localize('CET', ambiguous='infer').tz_convert('UTC')

            tag_values = {'tag_sensor': 'munkholmen_pi',
                          'tag_edge_device': 'munkholmen_topside_pi',
                          'tag_platform': 'munkholmen',
                          'tag_data_level': 'raw',
                          'tag_approved': 'no',
                          'tag_unit': 'none'}

            # ------------------------------------------------------------ #
            measurement_name = 'pi_status_munkholmen'
            field_keys = {"uptime_seconds": 'uptime_seconds',
                          "loadavg_1min": "loadavg_1min",
                          "relay_0_1_status": "relay_0_1_status",
                          "relay_0_2_status": "relay_0_2_status",
                          "relay_0_3_status": "relay_0_3_status",
                          "relay_0_4_status": "relay_0_4_status",
                          "relay_1_1_status": "relay_1_1_status",
                          "relay_1_2_status": "relay_1_2_status",
                          "relay_1_3_status": "relay_1_3_status",
                          "relay_1_4_status": "relay_1_4_status",
                          "ready_ctd_files": "ready_ctd_files",
                          "logging_ctd_files": "logging_ctd_files"
                          }
            df = util_db.filter_and_tag_df(df_all, field_keys, tag_values)
            util_db.ingest_df(measurement_name, df, self.influx_clients)

            logger.info(f'File {f} ingested.')

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('pi.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])

        logger.info('pi.rsync_and_ingest() finished.')
