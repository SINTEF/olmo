import os
from datetime import datetime
from influxdb import InfluxDBClient

import config
import util_file
from ais import AIS


def main():

    print("Starting running ingest_ais.py at "
          + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util_file.init_logger(config.main_logfile, name='ingest_ais')
    logger.info("\n\n------ Starting sync/ingest.")

    logger.info("Fetching the influxdb clients.")
    admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    clients = [
        InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'example'),
        InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
    ]

    ais = AIS(influx_clients=clients)
    ais.ingest_l0()


if __name__ == "__main__":
    main()
