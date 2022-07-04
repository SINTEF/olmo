import pandas as pd
import seawater

import sensor
import config
import util
import ingest

logger = util.init_logger(config.main_logfile, name='olmo.ctd')


class CTD(sensor.Sensor):
    '''Class for rsyncing and ingesting the munkholmen ctd data.'''
    def __init__(
            self,
            data_dir=f'/home/{config.munkholmen_user}/olmo/munkholmen/DATA',
            db_name='oceanlab',
            file_regex_l0=r"ready_ctd_(\d{14})\.csv",
            drop_recent_files_l0=0,
            remove_remote_files_l0=True,
            max_files_l0=None,
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(CTD, self).__init__()
        self.data_dir = data_dir
        self.db_name = db_name
        self.file_regex_l0 = file_regex_l0
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.max_files_l0 = max_files_l0
        self.influx_clients = influx_clients
        self.MUNKHOLMEN_LATITUDE = 63.456314

    def ingest_l0(self, files):

        for f in files:
            df_all = pd.read_csv(f, sep=',')

            time_col = 'Timestamp'
            df_all = util.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
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
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_conductivity_munkholmen'
            field_keys = {"Conductivity": 'conductivity'}
            tag_values['tag_unit'] = 'siemens_per_metre'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_pressure_munkholmen'
            field_keys = {"Pressure": 'pressure'}
            tag_values['tag_unit'] = 'none'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # -------------drop----------------------------------------------- #
            measurement_name = 'ctd_sbe63_munkholmen'
            field_keys = {"SBE63": 'sbe63'}
            tag_values['tag_unit'] = 'none'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_salinity_munkholmen'
            field_keys = {"Salinity": 'salinity'}
            tag_values['tag_unit'] = 'none'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_voltages_munkholmen'
            field_keys = {"Volt0": 'volt0',
                          "Volt1": 'volt1',
                          "Volt2": 'volt2',
                          "Volt3": 'volt3',
                          "Volt4": 'volt4',
                          "Volt5": 'volt5'}
            tag_values['tag_unit'] = 'none'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_depth_munkholmen'
            field_keys = {"depth": 'depth'}
            tag_values['tag_unit'] = 'metres'
            tag_values['tag_data_level'] = 'processed'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            # ------------------------------------------------------------ #
            measurement_name = 'ctd_density_munkholmen'
            field_keys = {"density": 'density'}
            tag_values['tag_unit'] = 'kilograms_per_cubic_metre'
            tag_values['tag_data_level'] = 'processed'
            df = util.filter_and_tag_df(df_all, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

            logger.info(f'File {f} ingested.')

    def rsync_and_ingest(self):

        files = self.rsync()
        logger.info('ctd.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])

        logger.info('ctd.rsync_and_ingest() finished.')
