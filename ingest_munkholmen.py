import os
import logging
from datetime import datetime

import config
from adcp import ADCP
from lisst_200 import Lisst_200


def main():

    print("Starting running ingest_munkholmen.py at "
          + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # ---- Set up logging:
    logger = logging.getLogger('olmo')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(
        config.output_dir, config.main_logfile + datetime.now().strftime('%Y%m%d')), 'a+')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    logger.info("\n\n------ Starting sync/ingest.")

    lisst = Lisst_200()
    lisst.rsync_and_ingest()

    adcp = ADCP()
    adcp.rsync_and_ingest()


if __name__ == "__main__":
    main()
