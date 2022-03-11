import os
import logging
import numpy as np
from influxdb import InfluxDBClient

import config
import util

'''
Functions to be directly used in ingesting data into influxdb.
'''

logger = logging.getLogger('olmo.ingest')

admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
client = InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab')


def float_col_fix(df, float_cols):
    '''Avoid problem where float cols give error if they "round to zero"'''
    for i, col in enumerate(df.columns):
        if col in float_cols:
            df[col] = df[col].astype(np.float64)
    return df


# def df_ingest(measurement, df, tag_sensor, tag_station, tag_dlevel, tag_approved, tag_unit):

#     # client = InfluxDBClient('localhost', database='my_db')
#     # measurement = 'measurement1'
#     # db_data = client.query('select value from %s' % (measurement))
#     data_to_write = [{
#         'measurement': measurement,
#         'tags': ['measurement1'],
#         'time': d['time'],
#         'fields': {'value': d['value']},
#         } for d in db_data.get_points()]
#     client.write_points(data_to_write)
