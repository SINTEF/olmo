import os
import time
import datetime
from influxdb import InfluxDBClient

import config
import ingest
import util

# File to change the 'approval' tag of a measurement (table)
# in influxdb.

# Databases:
admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
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

    # Must break down the timeslice smaller periods:
    start_time_ = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
    end_time_ = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
    total_time_delta = end_time_ - start_time_
    periods = []
    # Assuming breaking down into days:
    d = 0
    while d < total_time_delta.days:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            (start_time_ + datetime.timedelta(days=d + 1)).strftime('%Y-%m-%dT%H:%M:%SZ')))
        d += 1
    if total_time_delta.seconds > 0:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time_.strftime('%Y-%m-%dT%H:%M:%SZ')))

    for p in periods:
        timeslice = f"time > '{p[0]}' AND time < '{p[1]}'"
        print("Doing timeslice:", timeslice)

        for client in clients:
            # =================== Get the data:
            df = util.query_influxdb(client, measurement, '*', timeslice, False, approved='all')
            df = df.set_index('time').tz_convert('UTC')  # Should be in correct TZ as comes from DB
            # print(df)

            # =================== Change approval tags:
            # You can of course write your custom logic here
            df['approved'] = 'yes'
            # print(df)

            # =================== Delete the data and reupload:
            df = util.retag_tag_cols(df, util.query_show_tag_keys(client, measurement))
            df.to_csv(os.path.join(config.output_dir, 'df_backup.csv'), index=True)
            # Deleting:
            _ = client.query(f"DELETE FROM {measurement} WHERE {timeslice}")
            print('Deleted data...')
            ingest.ingest_df(measurement, df, [client])
            print('Data written :)')

        print("Finished all at "
              + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    t = time.time()
    main()
    print(f"time taken: {time.time() - t}")
