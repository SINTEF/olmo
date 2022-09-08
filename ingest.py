import logging
import numpy as np

'''
Functions to be directly used in ingesting data into influxdb.
'''

logger = logging.getLogger('olmo.ingest')


def ingest_df(measurement, df, clients):

    tag_cols = [c for c in df.columns if c[:4] == 'tag_']
    field_cols = [c for c in df.columns if c not in tag_cols]

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
