import os
import time
import datetime
from influxdb import InfluxDBClient

from ctd import CTD
import config
import util_db
import util_file

'''
Takes data from a measurement in one DB, and puts that into another.
'''

# Databases:
admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
read_client = InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'test')
write_clients = [
    InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab'),
    # InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
]

# Measurements to be copied:
measurement = 'pi_status_munkholmen'

# Time period:
start_time = '2022-08-25T12:00:00Z'
end_time = '2022-10-20T12:00:00Z'


def main():

    print("Starting running add_processed_data.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    periods = util_db.break_down_time_period(start_time, end_time)

    # # ---- May need some custom code here:
    # # Get ctd object and load the calibration file.
    # ctd = CTD()
    # ctd.load_calibration()

    for p in periods:
        timeslice = f"time > '{p[0]}' AND time < '{p[1]}'"
        print("Doing timeslice:", timeslice, end='')

        # =================== Get the data:
        df = util_db.query_influxdb(read_client, measurement, timeslice)
        # The result doesn't have 'tag_' on tag cols, and the index isn't time yet.
        df = util_db.retag_tag_cols(df, util_db.get_tag_keys(read_client, measurement))
        # The below line should be checked, it might not be needed in your case.
        df = util_db.force_float_cols(df, not_float_cols=['time'], error_to_nan=True)  # Should be done after the above line
        df = df.set_index('time').tz_convert('UTC')  # Should be in correct TZ as comes from DB

        # =================== Upload to clinents:
        util_db.ingest_df(measurement, df, write_clients)
        print(f' ... {df.shape[0]} pts written :)')

    print("Finished all at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


if __name__ == "__main__":
    t = time.time()
    main()
    print(f"Time taken: {time.time() - t}")
