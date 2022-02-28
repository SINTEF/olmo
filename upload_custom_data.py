import os
import subprocess
import json
import pandas as pd

import config
import sensor_conversions

'''
This script is designed to upload a folder of some custom
data which is associated with OceanLab. The folder will
be uploaded to the Azure DataLake, and then a link with
some meta data will be included in the influxDB.

Idea: Maybe this should be replaced by a script that trawls
the folder in the DataLake, and then add this info.
'''

FOLDER_TO_UPLOAD = 'FULL_PATH_TO_FILE'
DATE = '210927'  # YYMMDD, Main date associated with folder
LOCATION = 'Trondheim'
LAT = None
LON = None


def main():
    # ---- Generated metadata:
    exts = set(os.path.splitext(f)[1] for dir, dirs, files in os.walk(FOLDER_TO_UPLOAD) for f in files)
    exts = set(ext for ext in exts if ext != '')
    exts = ', '.join(exts)

    # ---- First upload the file:
    with open(os.path.join(config.secrets_dir, 'azure_token_datalake')) as f:
        aztoken = f.read()

    upload_location = config.custom_data_folder + '/' + DATE + '_' + os.path.basename(FOLDER_TO_UPLOAD)
    process = subprocess.Popen([
        'az', 'storage', 'fs', 'directory', 'upload',
        '-s', FOLDER_TO_UPLOAD,
        '-d', upload_location, '--recursive',
        '-f', 'oceanlabdlcontainer', '--account-name', 'oceanlabdlstorage',
        '--sas-token', aztoken],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print('\n' + '=' * 10 + ' Azure output from upload:')
    print('\n' + '-' * 10 + ' STDOUT:')
    print(stdout)
    print('\n' + '-' * 10 + ' STDERR:')
    print(stderr)
    print()

    # ---- Checkout if this worked
    # Error output from AZ is complex, so to check it, just look
    # for the folder in the container.
    process = subprocess.Popen([
        'az', 'storage', 'fs', 'directory', 'exists',
        '--name', upload_location,
        '-f', 'oceanlabdlcontainer', '--account-name', 'oceanlabdlstorage',
        '--sas-token', aztoken],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    stdout = json.loads(stdout)
    if not stdout['exists']:
        print('\n' + '=' * 10 + ' Azure output from directory existence check:')
        print('\n' + '-' * 10 + ' STDOUT:')
        print(stdout)
        print('\n' + '-' * 10 + ' STDERR:')
        print(stderr)
    elif stdout['exists']:
        print('\n' + '=' * 10 + ' Azure output from directory existence check:')
        print("'exists': True")

        # --- If uploaded, add link to influxDB.
        metadata = {
            'date': [DATE],
            'storage_location': [upload_location],
            'associated_location': [LOCATION],
            'lat': [LAT],
            'lon': [LON],
            'file_types': exts
        }
        df = pd.DataFrame.from_dict(metadata)
        df.date = pd.to_datetime(df['date'], format='%y%m%d')
        df = df.set_index('date')
        sensor_conversions.add_custom_data_directory(df)
        print('Link added to influxDB.')


if __name__ == '__main__':
    main()
