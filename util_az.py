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


def container_ls(container, prefix=None):
    '''
    List the files in the given container. Prefix used for 'subfolders'.

    Parameters
    ----------
    container : str
    prefix : None or str
    '''

    # with open('/home/oceanlab/Secrets/azure_token_web') as f:
    with open(os.path.join(config.secrets_dir, 'azure_token_web')) as f:
        aztoken = f.read()

    # az storage blob list --container-name container1
    # az storage fs list --account-name myadlsaccount --account-key 0000-0000
    # process = subprocess.Popen([
    #     'az', 'storage', 'fs', 'list',
    #     '--prefix', prefix,  # '-c', container,
    #     '--account-name', 'oceanlabdlstorage',
    #     '--sas-token', aztoken[:-1]],
    #     stdout=subprocess.PIPE,
    #     stderr=subprocess.PIPE)
    # az storage blob directory list -c MyContainer -d DestinationDirectoryPath --account-name MyStorageAccount
    process = subprocess.Popen([
        'az', 'storage', 'blob', 'directory', 'list',
        '-c', container,
        '-d', prefix,
        '--account-name', 'oceanlabdlstorage',
        '--sas-token', aztoken[:-1]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=60)
    if process.returncode != 0:
        print("We got an error.")
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))
    else:
        print(stdout)
