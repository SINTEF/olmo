import logging
import string
import pandas as pd
import numpy as np
from influxdb import DataFrameClient

import sensor
import config
import util

logger = logging.getLogger('olmo.adcp')


class ADCP(sensor.Sensor):
    '''Class for rsyncing and ingesting the ADCP data.'''
    def __init__(
            self,
            data_dir=f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA',
            file_regex_l1=r"adcp_(\d{14})\.dat",
            drop_recent_files_l1=1,
            remove_remote_files_l1=True,
            max_files_l1=20,
            db_name='example',  # This is outdated in other files, but this whole file is outdated now...
            measurement_name_l1='adcp_raw_test'):

        # Init the Sensor() class: Unused levels are set to None.
        super(ADCP, self).__init__()
        self.data_dir = data_dir
        self.file_regex_l1 = file_regex_l1
        self.drop_recent_files_l1 = drop_recent_files_l1
        self.remove_remote_files_l1 = remove_remote_files_l1
        self.max_files_l1 = max_files_l1
        self.db_name = db_name
        self.measurement_name_l1 = measurement_name_l1

    def data_to_df(self, filename):
        '''Takes a adcp_*.dat fileaname and returns DataFrames for PNORI, PNORS, and PNORC'''

        with open(filename) as file:
            lines = file.readlines()
            lines = [line.rstrip() for line in lines]

        PNORI = pd.DataFrame()
        PNORS = pd.DataFrame()
        PNORC = pd.DataFrame()

        for l in lines:
            data = l.replace('*', ',')
            if (data[0:6].find('$PNORI')) == 0:
                PNORI = PNORI.append(pd.DataFrame(
                    [x.split(',') for x in data.split('\n')],
                    index=None,
                    columns=[
                        'Identifier',
                        'Instrument type',
                        'Head ID',
                        'Number of beams',
                        'Number of cells',
                        'Blanking (m)',
                        'Cell size (m)',
                        'Coordinate system',
                        'Checksum']))

            elif (data[0:6].find('$PNORS')) == 0:
                PNORS = PNORS.append(pd.DataFrame(
                    [x.split(',') for x in data.split('\n')],
                    index=None,
                    columns=[
                        'Identifier',
                        'Date',
                        'Time',
                        'Error Code (hex)',
                        'Status Code (hex)',
                        'Battery Voltage',
                        'Sound Speed',
                        'Heading',
                        'Pitch (deg)',
                        'Roll (deg)',
                        'Presssure (dBar)',
                        'Temperature (dec C)',
                        'Analog input #1',
                        'Analog input #s',
                        'Checksum']))

            elif (data[0:6].find('$PNORC')) == 0:
                PNORC = PNORC.append(pd.DataFrame(
                    [x.split(',') for x in data.split('\n')],
                    index=None,
                    columns=[
                        'Identifier',
                        'Date',
                        'Time',
                        'Cell number',
                        'Velocity 1 (m/s) (Beam1/X/East)',
                        'Velocity 2 (m/s) (Beam1/X/North)',
                        'Velocity 3 (m/s) (Beam1/X/Up1)',
                        'Velocity 4 (m/s) (Beam1/X/Up2)',
                        'Speed (m/s)',
                        'Direction (deg)',
                        'Amplitude unit',
                        'Amplitude (Beam 1)',
                        'Amplitude (Beam 2)',
                        'Amplitude (Beam 3)',
                        'Amplitude (Beam 4)',
                        'Correlation (%) (Beam 1)',
                        'Correlation (%) (Beam 2)',
                        'Correlation (%) (Beam 3)',
                        'Correlation (%) (Beam 4)',
                        'Checksum']))

        return PNORI, PNORS, PNORC

    def s_d_from_PNOR(self, PNORI, PNORC, PNORS, cor_thresh=80):
        '''Create variables for speed, direction, depth from the PNOR info'''

        # df = pd.DataFrame()
        blanking = PNORI['Blanking (m)'].astype(np.float64).values
        cell_number = PNORC['Cell number'].astype(np.float64).values
        cell_size = PNORI['Cell size (m)'].astype(np.float64).values
        sensor_depth = PNORS['Presssure (dBar)'].astype(np.float64).values
        depth = (sensor_depth + blanking + cell_size / 2) + ((cell_number - 1) * cell_size)
        # v_east = PNORC['Velocity 1 (m/s) (Beam1/X/East)'].astype(np.float64).values
        # v_north = PNORC['Velocity 2 (m/s) (Beam1/X/North)'].astype(np.float64).values
        speed = PNORC['Speed (m/s)'].astype(np.float64).values
        direction = PNORC['Direction (deg)'].astype(np.float64).values

        cor1 = PNORC['Correlation (%) (Beam 1)'].astype(np.float64).values
        cor2 = PNORC['Correlation (%) (Beam 2)'].astype(np.float64).values
        cor3 = PNORC['Correlation (%) (Beam 3)'].astype(np.float64).values
        cor4 = PNORC['Correlation (%) (Beam 4)'].astype(np.float64).values
        min_cor = np.min([cor1, cor2, cor3, cor4], axis=0)
        good_mask = min_cor > cor_thresh

        timestamp = pd.to_datetime(str(PNORS['Date'][0]) + ' ' + str(PNORS['Time'][0]))

        return speed, direction, depth, good_mask, timestamp

    def ingest_l1(self, files):

        influx_client = DataFrameClient(
            config.sintef_influx_pc, 8086, self.get_influx_user(), self.get_influx_pwd(), self.db_name)

        def boring_font(df):
            df.columns = df.columns.str.translate(str.maketrans('', '', string.punctuation)).str.lower().str.replace(' ', '_')
            return df

        for f in files:
            PNORI, PNORS, PNORC = self.data_to_df(f)

            df_ingest = pd.DataFrame()
            # First lets get the time info out:
            date_and_time = PNORS.Date + PNORS.Time
            df_ingest['date'] = pd.to_datetime(date_and_time, format='%m%d%y%H%M%S')
            # Remove identifier and date/time cols:
            # Standardise col names:
            # Rename the checksum col (since name not unique):
            PNORI = boring_font(PNORI.iloc[:, 1:]).rename(columns={'checksum': 'checksum_pnori'})
            PNORS = boring_font(PNORS.iloc[:, 3:]).rename(columns={'checksum': 'checksum_pnors'})
            PNORC = boring_font(PNORC.iloc[:, 3:]).rename(columns={'checksum': 'checksum_pnorc'})

            # Join into once big wide DF
            df_ingest = df_ingest.join(PNORI)
            df_ingest = df_ingest.join(PNORS)
            for i in range(PNORC.shape[0]):
                df_ingest = df_ingest.join(PNORC.iloc[[i]].add_suffix(f'_{i}'))

            # Force all numeric cols to be floats:
            not_float_cols = [
                'date', 'head_id', 'checksum_pnori',
                'error_code_hex', 'status_code_hex', 'checksum_pnors',
            ]
            for i in range(PNORC.shape[0]):
                not_float_cols.append(f"amplitude_unit_{i}")
                not_float_cols.append(f"checksum_pnorc_{i}")
            df_ingest = util.force_float_cols(df_ingest, not_float_cols=not_float_cols)

            # TODO: Check that this time is correctly input now:
            df_ingest = df_ingest.set_index('date').tz_localize('UTC', ambiguous='infer')
            logger.info(f'Ingesting file {f} to {self.measurement_name}.')
            influx_client.write_points(df_ingest, self.measurement_name)

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('ADCP.rsync() finished.')

        if files['l1'] is not None:
            self.ingest_l1(files['l1'])
        logger.info('ADCP.rsync_and_ingest() finished.')
