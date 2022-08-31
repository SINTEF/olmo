import os
from datetime import datetime
from influxdb import InfluxDBClient

import config
import util
from munkholmen_pi_status import Munkholmen_Pi


def main():

    print("Starting running ingest_munkholmen_pi_status.py at "
          + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util.init_logger(config.main_logfile, name='ingest__munkholmen_pi_status')
    logger.info("\n\n------ Starting sync/ingest.")

    logger.info("Fetching the influxdb clients.")
    admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    clients = [
        InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'oceanlab'),
        InfluxDBClient(config.sintef_influx_pc, 8086, admin_user, admin_pwd, 'test'),
    ]

    pi_status = Munkholmen_Pi(influx_clients=clients)
    pi_status.rsync_and_ingest()


if __name__ == "__main__":
    main()
