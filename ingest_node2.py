import os
import datetime
import logging

import config
import util


# host = os.getenv('DB_AZURE_HOST')
# dbname = 'smartshiprouting'
# user = os.getenv('DB_AZURE_USERNAME')
# password = os.getenv('DB_AZURE_PASSWORD')
# db_url = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(user, url_quote(password), host, dbname)
# engine, meta = db_create_engine(db_url)


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
    # db_url = 'postgresql+psycopg2://{}:{}@{}:5432/{}'.format(user, url_quote(password), host, dbname)
    # engine, meta = db_create_engine(db_url)

    # tablename = 'nesdata_new'
    # exist = check_table(engine, tablename)
    # if not exist:
    #     print(f'Table {tablename} does not exist. Create it first.')
    #     exit()

    # nes_table = db.Table(tablename, meta, autoload=True, autoload_with=engine)

    
if __name__ == "__main__":
    main()