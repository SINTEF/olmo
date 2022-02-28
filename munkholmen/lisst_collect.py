import lisst_comms as lisst
import time
import datetime
import os
import glob
import sys

while True:
    lisst.record_samples(240, delay=0)
    lisst.download_csv()

    files = glob.glob(os.path.join('DATA', '*.CSV'))

    for f in files:
        path, filename = os.path.split(f)
        os.rename(f, os.path.join(path, 'lisst_' + filename))

    lisst.delete_all_csv()

    lisst.send_str('ZZ')
    time.sleep(600-240-90)
