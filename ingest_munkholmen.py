import os
from datetime import datetime
from influxdb import InfluxDBClient

import config
import util
from adcp import ADCP
from lisst_200 import Lisst_200


def main():

    print("Starting running ingest_munkholmen.py at "
          + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util.init_logger(config.main_logfile, name='ingest_munkholmen')
    logger.info("\n\n------ Starting sync/ingest.")

    logger.info("Fetching the influxdb clients.")
    admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    clients = [
        InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'example'),
        # InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
    ]

    lisst = Lisst_200(influx_clients=clients)
    lisst.rsync_and_ingest()

    # adcp = ADCP()
    # adcp.rsync_and_ingest()


if __name__ == "__main__":
    main()
