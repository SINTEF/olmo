import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
from urllib.parse import quote_plus as url_quote

import config
import util


def db_create_engine(url: str):
    engine = db.create_engine(url, echo=False, pool_pre_ping=True)
    engine.dialect.description_encoding = None
    meta = db.MetaData(engine)
    return engine, meta


def check_table(engine, tablename):
    insp = db.inspect(engine)
    ret = insp.dialect.has_table(engine.connect(), tablename)
    return ret


def main():

    print("Starting running ingest_node2.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util.init_logger(config.node2_logfile, name='ingest_node2')
    logger.info("\n\n------ Starting data collection in main()")

    db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        config.node2_user, url_quote(config.node2_pwd), config.node2_host, config.node2_port, config.node2_dbname
    )
    engine, meta = db_create_engine(db_url)

    logger.info("\n\n------ Checking connection")
    tablename = 'wind_sensors'
    exist = check_table(engine, tablename)
    if not exist:
        print(f'Table {tablename} does not exist. Create it first.')
        exit()

    cols = db.inspect(engine).get_columns('wind_sensors')  # This is the db schema
    col_names = [c['name'] for c in cols]

    with engine.connect() as con:
        rs = con.execute("SELECT * FROM wind_sensors WHERE sensor_measurement_type = 'Wind Speed' LIMIT 3")
        # rs = con.execute("SELECT * FROM wind_sensors WHERE sensor_measurement_type = 'Wind Speed' AND timestamp BETWEEN now() - (interval '24 hours') AND now()")

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

    df = sql_to_df(rs, col_names)

    print(df)
    print(df.dtypes)

    print(df.sensor_sn[0])
    print(type(df.sensor_sn[0]))
    print(len(df.sensor_sn[0]))



    # nes_table = db.Table(tablename, meta, autoload=True, autoload_with=engine)


if __name__ == "__main__":
    main()

# Engine(postgresql+psycopg2://share_odp%40adaptive-postgresql:***@adaptive-postgresql.postgres.database.azure.com:5432/hobolink)
# Engine(postgresql+psycopg2://share_odp%40adaptive-postgresql:***@adaptive-postgresql.postgres.database.azure.com:5432/hobolink)

# MetaData(bind=Engine(postgresql+psycopg2://share_odp%40adaptive-postgresql:***@adaptive-postgresql.postgres.database.azure.com:5432/hobolink))
# MetaData(bind=Engine(postgresql+psycopg2://share_odp%40adaptive-postgresql:***@adaptive-postgresql.postgres.database.azure.com:5432/hobolink))