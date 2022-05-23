import os
import logging
import datetime
import subprocess
import shutil

import config


timestamp = datetime.datetime.now().strftime('%Y%m%d')


def main():

    # ---- Set up logging:
    logger = logging.getLogger('olmo')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(
        config.output_dir, config.bu_logfile_basename + datetime.datetime.now().strftime('%Y%m%d')), 'a+')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    logger.info("\n\n------ Starting backup of influxdb to AzureDL.")

    if not os.path.exists(config.backup_dir):
        logger.info(f"Backup directory {config.backup_dir} didn't exist, creating...")
        os.makedirs(config.backup_dir)

    backup_folder = os.path.join(config.backup_dir, config.backup_basename + timestamp)
    process = subprocess.Popen(
        ['influxd', 'backup', '-portable', backup_folder],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=600)
    logger.info("STDOUT from 'influxd backup':\n" + stdout.decode(errors="ignore"))
    if process.returncode != 0:
        logger.error("Backup of influxdb failed at backup step. stderr:\n" + stderr.decode(errors="ignore"))
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))

    shutil.make_archive(backup_folder, 'zip', backup_folder)
    logger.info("Backup directory archived into zip file.")

    # ---- Upload that folder to the azure datalake
    with open(os.path.join(config.secrets_dir, 'azure_token_datalake')) as f:
        aztoken = f.read()
    az_filename = config.az_backups_folder + '/' + config.backup_basename + timestamp + '.zip'
    process = subprocess.Popen([
        'az', 'storage', 'fs', 'file', 'upload',
        '--source', backup_folder + '.zip', '-p', az_filename,
        '-f', 'oceanlabdlcontainer', '--account-name', 'oceanlabdlstorage',
        '--sas-token', aztoken[:-1]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=600)
    logger.info("STDOUT from 'az file upload':\n" + stdout.decode(errors="ignore"))
    if process.returncode != 0:
        logger.error("az file upload failed. stderr:\n" + stderr.decode(errors="ignore"))
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))
    logger.info('Backup, archive and transfer to azure completed successfully.')

    # ---- Clean up the backup folder and the .zip file in the container:
    shutil.rmtree(backup_folder)
    os.remove(backup_folder + '.zip')
    logger.info('Local files removed.')
    logger.info("Backup complete, exiting.")


if __name__ == "__main__":
    main()
