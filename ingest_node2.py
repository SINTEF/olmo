import os
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
from urllib.parse import quote_plus as url_quote
from influxdb import InfluxDBClient

import config
import util
import ingest


def db_create_engine(url: str):
    engine = db.create_engine(url, echo=False, pool_pre_ping=True)
    engine.dialect.description_encoding = None
    meta = db.MetaData(engine)
    return engine, meta


def check_table(engine, tablename):
    insp = db.inspect(engine)
    ret = insp.dialect.has_table(engine.connect(), tablename)
    return ret


def sql_to_df(rs, col_names):
    data = []
    for row in rs:
        d = [i for i in row]
        data.append(d)
    df = pd.DataFrame(data, columns=col_names)
    # Manually reformat cols:
    df['logger_sn'] = df['logger_sn'].astype(str)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)  # Remove white space
    df['value'] = df['value'].astype(np.float64)
    return df


def main():

    print("Starting running ingest_node2.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util.init_logger(config.node2_logfile, name='ingest_node2')
    logger.info("\n\n------ Starting data collection in main()")

    db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        config.node2_user, url_quote(config.node2_pwd), config.node2_host, config.node2_port, config.node2_dbname
    )
    engine, meta = db_create_engine(db_url)

    logger.info("Checking connection to node2 database.")
    tablename = 'wind_sensors'
    exist = check_table(engine, tablename)
    if not exist:
        logger.error(f'Table {tablename} does not exist. Create it first.')
        exit()

    logger.info("Fetching the influxdb clients.")
    admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    clients = [
        InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab'),
        InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
    ]

    cols = db.inspect(engine).get_columns('wind_sensors')  # This is the db schema
    col_names = [c['name'] for c in cols]

    # Hack variable to input correct information. Has form:
    #     sensor_sn (in postgres),
    #     sensor_measurement_type (in postgres),
    #     measurement name (in influx),
    #     platform,
    data_types = [
        ('21124500-1', 'Wind Speed', 'wind_speed_brattora01', 'brattora01'),
        ('21124500-2', 'Gust Speed', 'gust_speed_brattora01', 'brattora01'),
        ('21124500-3', 'Wind Direction', 'wind_direction_brattora01', 'brattora01'),
        ('21139640-1', 'Wind Speed', 'wind_speed_brattora02', 'brattora02'),
        ('21139640-2', 'Gust Speed', 'gust_speed_brattora02', 'brattora02'),
        ('21139640-3', 'Wind Direction', 'wind_direction_brattora02', 'brattora02'),
    ]

    logger.info("Looping through data types.")
    for d in data_types:
        with engine.connect() as con:
            rs = con.execute(f"SELECT * FROM wind_sensors WHERE sensor_sn = '{d[0]}' AND sensor_measurement_type = '{d[1]}' AND timestamp BETWEEN now() - (interval '24 hours') AND now()")

        df = sql_to_df(rs, col_names)

        # Map the unit to our format:
        unit_mapping = {"°": 'degrees', "m/s": 'metres_per_second'}
        df = df.replace({"unit": unit_mapping})

        # Rename columns so they match our schema:
        new_keys = {
            'timestamp': 'date',
            'logger_sn': 'tag_edge_device',
            'sensor_sn': 'tag_sensor',
            'value': d[1].lower().replace(' ', '_'),
            'unit': 'tag_unit',
            'id': 'postgres_id',
        }
        df = df.rename(columns=new_keys)

        # Add additional static tags:
        additional_tag_values = {
            'tag_platform': d[3],
            'tag_data_level': 'raw',
            'tag_approved': 'none',
        }
        for (k, v) in additional_tag_values.items():
            df[k] = v

        # Remove duplicate column:
        df.drop(['sensor_measurement_type'], axis=1, inplace=True)

        # date column should be the index, with utc timezone:
        df = df.set_index('date').tz_convert('UTC')

        ingest.ingest_df(d[2], df, clients)

    logger.info("All data transferred and ingested successfully, exiting.")


if __name__ == "__main__":
    main()
