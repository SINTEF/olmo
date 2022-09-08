import os
import subprocess
import logging

import config

logger = logging.getLogger('olmo.util_az')


def upload_file(local_file, az_file, container, content_type='text/html', overwrite=True):
    '''
    Upload a file to the azure dl storage.

    TODO: overwrite currently only can be 'True'

    Parameters
    ----------
    local_file : str
        Path to local file file.
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