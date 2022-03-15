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


def ingest_df(measurement, df, clients):

    all_cols = df.columns
    tag_cols = [c for c in all_cols if c[:4] == 'tag_']
    field_cols = [c for c in all_cols if c not in tag_cols]

    data = []
    for index, row in df.iterrows():
        data.append({
            'measurement': measurement,
            'time': index,
            'tags': {t[4:]: row[t] for t in tag_cols},
            'fields': {f: row[f] for f in field_cols},
        })

    for c in clients:
        c.write_points(data)
