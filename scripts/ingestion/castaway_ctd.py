import os
import numpy as np
import pandas as pd
import datetime
from influxdb import InfluxDBClient

import config
import ingest
import util_db
import util_file

# File to ingest a 'castaway CTD' file.

# NOTE: You will have to have run 'python setup.py develop' from
# the main dir of the repo before running this.


# ---- input data:

# Full file paths:
files = [
    '/home/exampleuser/path/CC1821002_20220823_115725.csv'
]

# Databases:
admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
clients = [
    InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab'),
    InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
]

# Data:
measurement_names = 'castawayctd_VARIABLE_portable'  # VARIABLE will be replaced with the variable name
tag_values = {
    'tag_sensor': 'castawayctd',
    'tag_serial': None,  # Will be read from the file
    'tag_edge_device': 'none',
    'tag_platform': 'none',
    'tag_data_level': 'raw',
    'tag_approved': 'none',
    'tag_unit': None,  # Added via the 'variables' dict below
}
variables = {
    'Pressure (Decibar)': ['pressure', 'dbar'],
    'Depth (Meter)': ['depth', 'metres'],
    'Temperature (Celsius)': ['temperature', 'degrees_celcius'],
    'Conductivity (MicroSiemens per Centimeter)': ['conductivity', 'microsiemens_per_centimeter)'],
    'Specific conductance (MicroSiemens per Centimeter)': ['specific_conductance', 'microsiemens_per_centimeter'],
    'Salinity (Practical Salinity Scale)': ['salinity', 'psu'],
    'Sound velocity (Meters per Second)': ['sound_velocity', 'metres_per_second'],
    'Density (Kilograms per Cubic Meter)': ['density', 'kilograms_per_cubic_meter'],
}
HEADER_LENGTH = 27


def main():

    print("Starting running manual_ingest.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Just loop through the files:
    for file in files:
        # ---- Loading hearder of the file:
        with open(file, 'r') as f:
            header = dict()
            for i in range(HEADER_LENGTH):
                line = f.readline()[2:].split(',')
                header[line[0]] = line[1][:-1]
        # Setting the serial number tag:
        tag_values['tag_serial'] = header['Device']
        # Will be useful to unpack the dict:
        col_names = dict()
        vars_units = dict()
        for k, v in variables.items():
            col_names[k] = v[0]
            vars_units[v[0]] = v[1]

        # ---- Loading the data into a time formatted dataframe:
        # NOTE: Measurement times will be linearly spaced between the "Cast time (UTC)"
        # and "Cast time (UTC) + Cast duration (Seconds)". This will probably not be
        # consistent with the "Samples per second".
        df_all = pd.read_csv(file, header=HEADER_LENGTH + 1)
        # Convert col names to simple names:
        df_all.rename(columns=col_names, inplace=True)
        df_all = ingest.float_col_fix(df_all, 'all')
        # Remember, all times in influxDB as UTC.
        start_time = datetime.datetime.strptime(header['Cast time (UTC)'], "%Y-%m-%d %H:%M:%S")
        delta = datetime.timedelta(seconds=np.float64(header['Cast duration (Seconds)']) / (df_all.shape[0] - 1))
        times = [start_time + i * delta for i in range(df_all.shape[0])]
        df_all['time'] = times
        df_all = df_all.set_index('time').tz_localize('UTC')
        # print(df_all.head())

        # ==== Data ingestion:
        for k, v in vars_units.items():
            measurement_name = measurement_names.replace('VARIABLE', k)
            tag_values['tag_unit'] = v
            df = util_db.filter_and_tag_df(df_all, {k: k}, tag_values)
            ingest.ingest_df(measurement_name, df, clients)
        print(f'Finished ingesting file {file}')

    print("All data ingested successfully, exiting.")


if __name__ == "__main__":
    main()
