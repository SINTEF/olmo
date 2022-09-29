import os
import subprocess
import logging
import json

import config

logger = logging.getLogger('olmo.util_az')


def upload_file(local_file, az_file, container, content_type='text/html', overwrite=True):
    '''
    Upload a file to the azure dl storage.

    TODO: overwrite currently only can be 'True'

    Parameters
    ----------
    local_file : str
        Path to local file.
    az_file : str
        Path and name of file uploaded to azure.
    container : str
        Name of 'container' in azure to upload to.
    content_type : str
    overwrite : bool
    '''

    with open(os.path.join(config.secrets_dir, 'azure_token_web')) as f:
        aztoken = f.read()
    process = subprocess.Popen([
        'az', 'storage', 'fs', 'file', 'upload',
        '--source', local_file, '-p', az_file,
        '-f', container, '--account-name', 'oceanlabdlstorage', '--overwrite',
        '--content-type', content_type,
        '--sas-token', aztoken[:-1]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=600)
    # logger.info("STDOUT from 'az file upload':\n" + stdout.decode(errors="ignore"))
    if process.returncode != 0:
        # logger.error("az file upload failed. stderr:\n" + stderr.decode(errors="ignore"))
        print("we got an error")
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))
    # logger.info('Backup, archive and transfer to azure completed successfully.')


def delete_file(az_file, container, token_file='azure_token_web'):
    '''
    Deletes a fiels from the the azure dl storage.

    Parameters
    ----------
    az_file : str
        Path and name of file to be deleted.
    container : str
        Name of 'container' in azure where the file is stored.
    token_file : str
    '''

    with open(os.path.join(config.secrets_dir, token_file)) as f:
        aztoken = f.read()

    process = subprocess.Popen([
        'az', 'storage', 'fs', 'file', 'delete', '--yes',
        '--account-name', 'oceanlabdlstorage',
        '--file-system', container,
        '--path', az_file,
        '--sas-token', aztoken[:-1]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=60)
    if process.returncode != 0:
        print("We got an error.")
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))


def container_ls(container, prefix=None, token_file='azure_token_web'):
    '''
    List the files in the given container. Prefix used for 'subfolders'.

    Parameters
    ----------
    container : str
    prefix : None or str
    token_file : str
    '''

    with open(os.path.join(config.secrets_dir, token_file)) as f:
        aztoken = f.read()

    process = subprocess.Popen([
        'az', 'storage', 'fs', 'file', 'list',
        '--account-name', 'oceanlabdlstorage',
        '--file-system', container,
        '--path', prefix,
        '--sas-token', aztoken[:-1]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=60)
    if process.returncode != 0:
        print("We got an error.")
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))
    else:
        stdout = json.loads(stdout.decode('utf-8'))
        # Output has the form:
        #     [{"contentLength": 1049468602,
        #     "etag": "0x8DA9F6301D6B099",
        #     "group": "$superuser",
        #     "isDirectory": false,
        #     "lastModified": "2022-09-26T02:01:20",
        #     "name": "influx_backups/influxbackup_20220926.zip",
        #     "owner": "$superuser",
        #     "permissions": "rw-r-----"}, ... ]
        files = []
        for f in stdout:
            files.append(f['name'][len(prefix) + 1:])  # We remove the prefix also
        return files
