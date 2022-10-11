import os
from datetime import datetime
from influxdb import InfluxDBClient

import config
import util_file
from gas_analyser import GasAnalyser


def main():

    print("Starting running ingest_gasanalyser.py at "
          + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    logger = util_file.init_logger(config.main_logfile, name='ingest_gasanalyser')
    logger.info("\n\n------ Starting sync/ingest.")

    logger.info("Fetching the influxdb clients.")
    admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    methane_client = [
        InfluxDBClient(config.az_influx_pc, 8086, admin_user, admin_pwd, 'methane_closed'),
    ]

    gas = GasAnalyser(influx_clients=methane_client)
    gas.rsync_and_ingest()


if __name__ == "__main__":
    main()
