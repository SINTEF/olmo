import os
import pandas as pd
import datetime
from influxdb import InfluxDBClient

import config
import util_db
import util_file

# File to ingest a specific piece of data into a table in the database.
# Initially made to ingest lat/lon locations of sensors to the correct table.
# NOTE: You will have to have run 'python setup.py develop' from
# the main dir of the repo before running this.


# ---- input data:

# Databases:
admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
clients = [
    InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab'),
    InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
]

# Data:
measurement_name = 'position_munkholmen_supportbuoy01'
field_values = {
    'latitude': ['0.'],
    'longitude': ['0.'],
}  # Must only be a single value for now
tag_values = {
    'tag_sensor': 'none',
    'tag_edge_device': 'none',
    'tag_platform': 'supportbuoy01',
    'tag_data_level': 'raw',
    'tag_approved': 'none',
    'tag_unit': 'degrees',
}
# timestamp = None
timestamp = datetime.datetime(2022, 5, 24, 17, 00)  # Code assumes UTC


def main():

    print("Starting running manual_ingest.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    for k, v in field_values.items():
        assert len(v) == 1, "Due to setting of data, must only input a single data value."

    df = pd.DataFrame.from_dict(field_values)
    if timestamp is None:
        df['date'] = datetime.datetime.now(datetime.timezone.utc)
        df = df.set_index('date').tz_convert('UTC')
    else:
        df['date'] = timestamp
        df = df.set_index('date').tz_localize('UTC').tz_convert('UTC')
    for (k, v) in tag_values.items():
        df[k] = v
    print(measurement_name, df)
    print(datetime.datetime.now(datetime.timezone.utc))
    util_db.ingest_df(measurement_name, df, clients)

    print("All data ingested successfully, exiting.")


if __name__ == "__main__":
    main()
