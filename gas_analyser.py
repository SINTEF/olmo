import os
import datetime
import pandas as pd
import zipfile

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
            remove_remote_files_l0=False,
            max_files_l0=None,
            recursive_file_search_l1=True,
            file_search_l1='gga_????-??-??_f????.txt.zip',
            drop_recent_files_l1=1,
            remove_remote_files_l1=False,
            max_files_l1=None,
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(GasAnalyser, self).__init__()
        self.data_dir = data_dir
        self.recursive_file_search_l0 = recursive_file_search_l0
        self.file_search_l0 = file_search_l0
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.max_files_l0 = max_files_l0
        self.recursive_file_search_l1 = recursive_file_search_l1
        self.file_search_l1 = file_search_l1
        self.drop_recent_files_l1 = drop_recent_files_l1
        self.remove_remote_files_l1 = remove_remote_files_l1
        self.max_files_l1 = max_files_l1
        self.influx_clients = influx_clients

    def ingest_l0(self, files):

        for f in files:

            # Special conditions:
            # Note that the place we get the data from will continually refill with data. So we will
            # continually reingest data. This is not optimal.
            #
            # If date is 2002-01-01: We should skip it.
            # If data is > 2022-09-01: We should label it as from the munkholmen buoy, otherwise its origin is unknown.

            # Unzip any zip files:
            file_type_tag = 'txt'
            if f[-4:] == '.zip':
                file_type_tag = 'zip'
                with zipfile.ZipFile(f, "r") as zip_ref:
                    zip_ref.extractall(os.path.dirname(f))
                f = f[:-4]
                # These zip files have a "PGP message" at the end, which must be removed.
                with open(f, "r+") as f_handle:
                    for num, line in enumerate(f_handle):
                        if '-----BEGIN PGP MESSAGE-----' in line:
                            cut_line = num
                            break
                    f_handle.seek(cut_line)
                    f_handle.truncate()

            # Will put this all into a try/except clause, since many files have odd formats I can't
            # be bothered dealing with (strange timestamps, not all cols)
            try:
                from_munkholmen = False
                if f[-20:-10] in ['1800-01-01']:
                    # print(f"Skipping file: {f}")
                    continue
                elif datetime.datetime.strptime(f[-20:-10], '%Y-%m-%d') > datetime.datetime(2022, 8, 31, 23, 59, 00):
                    from_munkholmen = True
                    # print(f"File {f} deemed from munkholmen.")

                df_all = pd.read_csv(f, sep=',', skiprows=1)

                time_col = '                     Time'
                df_all = util_db.force_float_cols(df_all, not_float_cols=[time_col], error_to_nan=True)
                df_all[time_col] = pd.to_datetime(df_all[time_col], format='  %d/%m/%Y %H:%M:%S.%f')
                df_all = df_all.set_index(time_col).tz_localize('CET', ambiguous='infer').tz_convert('UTC')

                ted = 'munkholmen_topside_pi' if from_munkholmen else 'none'
                tp = 'munkholmen' if from_munkholmen else 'none'
                tag_values = {'tag_sensor': 'LGR-UGGA',
                              'tag_edge_device': ted,
                              'tag_platform': tp,
                              'tag_data_level': 'raw',
                              'tag_approved': 'none',
                              'tag_file_type': file_type_tag}

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_ch4'
                field_keys = {"      [CH4]_ppm": util_db.format_str("      [CH4]_ppm"),
                              "   [CH4]_ppm_sd": util_db.format_str("   [CH4]_ppm_sd")}
                tag_values['tag_unit'] = 'ppm'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_h2o'
                field_keys = {"      [H2O]_ppm": util_db.format_str("      [H2O]_ppm"),
                              "   [H2O]_ppm_sd": util_db.format_str("   [H2O]_ppm_sd")}
                tag_values['tag_unit'] = 'ppm'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_co2'
                field_keys = {"      [CO2]_ppm": util_db.format_str("      [CO2]_ppm"),
                              "   [CO2]_ppm_sd": util_db.format_str("   [CO2]_ppm_sd")}
                tag_values['tag_unit'] = 'ppm'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_ch4d'
                field_keys = {"     [CH4]d_ppm": util_db.format_str("     [CH4]d_ppm"),
                              "  [CH4]d_ppm_sd": util_db.format_str("  [CH4]d_ppm_sd")}
                tag_values['tag_unit'] = 'ppm'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_co2d'
                field_keys = {"     [CO2]d_ppm": util_db.format_str("     [CO2]d_ppm"),
                              "  [CO2]d_ppm_sd": util_db.format_str("  [CO2]d_ppm_sd")}
                tag_values['tag_unit'] = 'ppm'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_gasp'
                field_keys = {"      GasP_torr": util_db.format_str("      GasP_torr"),
                              "   GasP_torr_sd": util_db.format_str("   GasP_torr_sd")}
                tag_values['tag_unit'] = 'torr'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_gast'
                field_keys = {"         GasT_C": util_db.format_str("         GasT_C"),
                              "      GasT_C_sd": util_db.format_str("      GasT_C_sd")}
                tag_values['tag_unit'] = 'degrees_celcius'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_ambt'
                field_keys = {"         AmbT_C": util_db.format_str("         AmbT_C"),
                              "      AmbT_C_sd": util_db.format_str("      AmbT_C_sd")}
                tag_values['tag_unit'] = 'degrees_celcius'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                # ------------------------------------------------------------ #
                measurement_name = 'gasanalyser_aux'
                field_keys = {"         RD0_us": util_db.format_str("         RD0_us"),
                              "      RD0_us_sd": util_db.format_str("      RD0_us_sd"),
                              "         RD1_us": util_db.format_str("         RD1_us"),
                              "      RD1_us_sd": util_db.format_str("      RD1_us_sd"),
                              "       Fit_Flag": util_db.format_str("       Fit_Flag"),
                              "      MIU_VALVE": util_db.format_str("      MIU_VALVE"),
                              "       MIU_DESC": util_db.format_str("       MIU_DESC")}
                tag_values['tag_unit'] = 'none'
                df = util_db.filter_and_tag_df(df_all, field_keys, tag_values, disapprove_nans=True)
                util_db.ingest_df(measurement_name, df, self.influx_clients)

                logger.info(f'File {f} ingested.')
            except (ValueError, KeyError) as error:
                logger.info(f"Failed on file: {f}\nError: {error}")

    def rsync_and_ingest(self):

        logger.info('gas.rsync() started.')
        print('gas.rsync() started.')
        files = self.rsync()
        logger.info('gas.rsync() finished.')
        print('gas.rsync() finished.')

        if files['l0'] is not None:
            self.ingest_l0(files['l0'])
        if files['l1'] is not None:
            # NOTE: we are sending l1 files to the l0 ingester here.
            # l0 is .txt files, l1 is .zip files. We will unzip within .ingest_l0.
            self.ingest_l0(files['l1'])

        logger.info('gas.rsync_and_ingest() finished.')
