import datetime
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

    # # Connect to Postgres:
    # # load_dotenv('local.env') # local env
    # load_dotenv()
    # host = os.getenv('DB_AZURE_HOST')
    # dbname = 'smartshiprouting'
    # user = os.getenv('DB_AZURE_USERNAME')
    # password = os.getenv('DB_AZURE_PASSWORD')
    db_url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        config.node2_user, url_quote(config.node2_pwd), config.node2_host, config.node2_port, config.node2_dbname
    )
    engine, meta = db_create_engine(db_url)

    print("engine:", engine)
    print("meta:", meta)
    # print(config.node2_pwd)
    print("config.node2_user:", config.node2_user)
    print("config.node2_host:", config.node2_host)
    print("config.node2_port:", config.node2_port)
    print("config.node2_dbname:", config.node2_dbname)

    tablename = 'nesdata_new'
    exist = check_table(engine, tablename)
    if not exist:
        print(f'Table {tablename} does not exist. Create it first.')
        exit()

    # nes_table = db.Table(tablename, meta, autoload=True, autoload_with=engine)


if __name__ == "__main__":
    main()
