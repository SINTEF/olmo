import os
import glob
import shutil
from datetime import datetime
import subprocess
import numpy as np

import config
import util_file
import sensor_conversions

# In this file we will simulate both "silcam files" and
# the rsync-ing of those files (since dragon is down).
# Note that this is happeneing purely "on land" hence we also
# ingest in this file - as we usually ingest after the rsync.

# Make a simulated file. Instead of an image, I'll just put a timestamp and a number.
measurement_time = datetime.now()
silc_file_start = "silc_sim_data"
OUTFILE = os.path.join(config.output_dir, f"{silc_file_start}_{measurement_time.strftime('%Y%m%d-%H%M%S')}")
with open(OUTFILE, 'a') as f:
    f.write(f"{datetime.now().strftime('%Y%m%d.%H:%M:%S')},{np.random.rand() * 10:.0f},\n")

# Now copy and paste some of the code from the munkholmen/move_data_to_rsync.py
# as this is the data that appends the time and adds the completion flag.
TIME_STAMP = datetime.now().strftime('%Y%m%d%H%M%S')

files = glob.glob(os.path.join(config.output_dir, silc_file_start + '*'))
print(files)
for f in files:
    if os.path.isfile(f):
        # NOTE this has been changed to the rsync_inbox:
        shutil.move(f, util_file.add_timestring(util_file.change_dir(f, config.rsync_inbox), TIME_STAMP))
    else:
        print(f"File not found/no data: {f}")
# Finally at the flag that this process is complete
open(os.path.join(config.rsync_inbox, "completion_flag_" + TIME_STAMP), 'a').close()

# Get all silc_sim files for the timestamp:
files = glob.glob(os.path.join(config.rsync_inbox, silc_file_start + '*' + TIME_STAMP))
# First put file in the DataLake (and obtain link to it)
# then process and ingest those files into the timeseries database:
with open(os.path.join(config.secrets_dir, 'azure_token_datalake'), 'r') as f:
    az_token = f.read()  # This token to run out end of 2021
for f in files:
    storage_location = f"silc_testing/{os.path.split(util_file.remove_timestring(f))[1]}"
    process = subprocess.run([
        'az', 'storage', 'fs', 'file', 'upload', '-s', f, '-p', storage_location,
        '-f', 'oceanlabdlcontainer', '--account-name', 'oceanlabdlstorage',
        '--sas-token', az_token],
        stdout=subprocess.PIPE, universal_newlines=True)
    print(process)
    assert process.returncode == 0, f"Upload to az failed: {process}"

    sensor_conversions.ingest_silc_sim(
        os.path.join(config.rsync_inbox, f), storage_location
    )
