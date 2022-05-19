import os
import subprocess
import datetime
import pandas as pd
from influxdb import DataFrameClient

import sensor
import config
import util
import ingest

logger = util.init_logger(config.main_logfile, name='olmo.lisst_200')


class Lisst_200(sensor.Sensor):
    '''Class for rsyncing and ingesting the Lisst_200 data.'''
    def __init__(
            self,
            data_dir=f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA',
            db_name='example',
            file_regex_l0=r"lisst_L(\d{7})\.RBN",
            drop_recent_files_l0=0,
            remove_remote_files_l0=True,
            max_files_l0=10,
            measurement_name_l0='munk_lisst-200_l0',
            file_regex_l1=r"ready_lisst_I(\d{7})\.CSV",
            drop_recent_files_l1=0,
            remove_remote_files_l1=True,
            max_files_l1=None,
            measurement_name_l1='lisst_200',
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(Lisst_200, self).__init__()
        self.data_dir = data_dir
        self.db_name = db_name
        self.file_regex_l0 = file_regex_l0
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.max_files_l0 = max_files_l0
        self.measurement_name_l0 = measurement_name_l0
        self.file_regex_l1 = file_regex_l1
        self.drop_recent_files_l1 = drop_recent_files_l1
        self.remove_remote_files_l1 = remove_remote_files_l1
        self.max_files_l1 = max_files_l1
        self.measurement_name_l1 = measurement_name_l1
        self.influx_clients = influx_clients

    def lisst200_csv_to_df(self, csv_filename):
        '''Take a LISST-200 .CSV file and returns a pandas DataFrame'''

        c = 36  # number of size bins of LISST-200x
        column_names = []
        for size_bin in range(c):
            name = f'size_bin_{size_bin+1:02}'
            column_names += [name]

        column_names += ['Laser transmission Sensor']
        column_names += ['Supply voltage in [V]']
        column_names += ['External analog input 1 [V]']
        column_names += ['Laser Reference sensor [mW]']
        column_names += ['Depth in [m of sea water]']
        column_names += ['Temperature [C]']
        column_names += ['Year']
        column_names += ['Month']
        column_names += ['Day']
        column_names += ['Hour']
        column_names += ['Minute']
        column_names += ['Second']
        column_names += ['External analog input 2 [V]']
        column_names += ['Mean Diameter [μm]']
        column_names += ['Total Volume Concentration [PPM]']
        column_names += ['Relative Humidity [%]']
        column_names += ['Accelerometer X [not presently calibrated or used]']
        column_names += ['Accelerometer Y [not presently calibrated or used]']
        column_names += ['Accelerometer Z [not presently calibrated or used]']
        column_names += ['Raw pressure [most significant bit]']
        column_names += ['Raw pressure [least significant 16 bits]']
        column_names += ['Ambient Light [counts – not calibrated]']
        column_names += ['Not used (set to zero)']
        column_names += ['Computed optical transmission over path [dimensionless]']
        column_names += ['Beam-attenuation (c) [m-1]']

        df = pd.read_csv(csv_filename, names=column_names)

        df['date'] = pd.to_datetime(dict(
            year=df.Year,
            month=df.Month,
            day=df.Day,
            hour=df.Hour,
            minute=df.Minute,
            second=df.Second))

        return df

    def ingest_l0(self, files):

        influx_client = DataFrameClient('sintefpc6201', 8086, 'root', self.get_influx_pwd(), self.db_name)

        for f in files:
            storage_location = f"{self.measurement_name_l0}/{os.path.split(f)[1]}"
            process = subprocess.run([
                'az', 'storage', 'fs', 'file', 'upload', '-s', f, '-p', storage_location,
                '-f', 'oceanlabdlcontainer', '--account-name', 'oceanlabdlstorage',
                '--sas-token', self.get_azure_token()],
                stdout=subprocess.PIPE, universal_newlines=True)
            assert process.returncode == 0, f"Upload to az failed for file {f}. Msg: {process}"
            logger.info(f'File uploaded to Azure here: {storage_location}')

            ingest_data = {
                'date': pd.to_datetime(datetime.datetime.now()),
                'azure_location': storage_location}
            df = pd.DataFrame(columns=ingest_data.keys())
            df = df.append(ingest_data, ignore_index=True)
            # df = df.set_index('date').tz_localize('CET', ambiguous='infer')
            df = df.set_index('date').tz_localize('CET', ambiguous='infer').tz_convert('UTC')

            logger.info(f'Ingesting file {f} to {self.measurement_name_l0}.')
            influx_client.write_points(df, self.measurement_name_l0)

    def ingest_l1(self, files):

        # influx_client = DataFrameClient(
        #     config.sintef_influx_pc, 8086, self.get_influx_user(), self.get_influx_pwd(), self.db_name)

        for f in files:
            df = self.lisst200_csv_to_df(f)

            df = util.force_float_cols(df, not_float_cols=['date'])
            # TODO: Check this time is correct with what the instrument gives.
            df = df.set_index('date').tz_localize('UTC', ambiguous='infer')

            tag_values = {'tag_sensor': 'lisst_200',
                          'tag_edge_device': 'munkholmen_topside_pi',
                          'tag_platform': 'munkholmen',
                          'tag_data_level': 'processed',
                          'tag_approved': 'no',
                          'tag_unit': 'none'}

            df = util.add_tags(df, tag_values)

            logger.info(f'Ingesting file {f} to {self.measurement_name_l1}.')
            ingest.ingest_df(self.measurement_name_l1, df, self.influx_clients)

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('Lisst_200.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])

        if files['l1'] is not None:
            self.ingest_l1(files['l1'])
        logger.info('Lisst_200.rsync_and_ingest() finished.')
