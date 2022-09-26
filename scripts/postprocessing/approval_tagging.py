import os
import time
import datetime
from influxdb import InfluxDBClient

import config
import util_db
import util_file

# File to change the 'approval' tag of a measurement (table)
# in influxdb.

# Databases:
admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
clients = [
    InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'example'),
    # InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
]

# List of measurements and variables which will be retrieved from
# influx from which the processed data should be collected.
measurement = 'ctd_conductivity_munkholmen'
# Time period:
start_time = '2022-06-02T12:09:54Z'
end_time = '2022-06-02T12:09:56Z'


def main():

    print("Starting running add_processed_data.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    periods = util_db.break_down_time_period(start_time, end_time)

    for p in periods:
        timeslice = f"time > '{p[0]}' AND time < '{p[1]}'"
        print("Doing timeslice:", timeslice)

        for client in clients:
            # =================== Get the data:
            df = util_db.query_influxdb(client, measurement, timeslice)
            df = df.set_index('time').tz_convert('UTC')  # Should be in correct TZ as comes from DB
            # print(df)

            # =================== Change approval tags:
            # You can of course write your custom logic here
            df['approved'] = 'yes'
            # print(df)

            # =================== Delete the data and reupload:
            df = util_db.retag_tag_cols(df, util_db.get_tag_keys(client, measurement))
            df.to_csv(os.path.join(config.output_dir, 'df_backup.csv'), index=True)
            # Deleting:
            _ = client.query(f"DELETE FROM {measurement} WHERE {timeslice}")
            print('Deleted data...')
            util_db.ingest_df(measurement, df, [client])
            print('Data written :)')

        print("Finished all at "
              + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    t = time.time()
    main()
    print(f"time taken: {time.time() - t}")
