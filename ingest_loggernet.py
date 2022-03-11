import os
import logging
import datetime
import subprocess
import time
import paramiko

import config
import loggernet

# I couldn't install rsync on the cmd prompt on the remote
# (although I could on git bash...) but since I could get
# standard ssh, this imitates a custom "rsync" with scp and rm.


def dir_remote(username, address):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(address, username=username)

    command = f"cd c:\\Users\{config.loggernet_user}\LoggerNet_output&&dir"
    stdin, stdout, stderr = ssh.exec_command(command)

    return stdout.read().decode(errors='ignore'), stderr.read().decode(errors='ignore')


def move_remote(username, address, filename):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(address, username=username)

    command = f"move c:\\Users\{config.loggernet_user}\LoggerNet_output\\{filename} c:\\Users\{config.loggernet_user}\LoggerNet_output\duplicate_data"
    stdin, stdout, stderr = ssh.exec_command(command)

    return stdout.read().decode(errors='ignore'), stderr.read().decode(errors='ignore')


def scp_file(username, address, file_path, destination, return_info=False, timeout=240):
    '''
    Parameters
    ----------
    file_path : str
        Note can be relative to the USERNAME's home directory on that pc
        or the full path.
    '''
    process = subprocess.Popen(
        ['scp', '-T', username + '@' + address + ":" + file_path, destination],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    process.wait(timeout=timeout)
    if return_info:  # This is not working, as we don't assign these.
        return stdout, stderr


def get_file_list(dir_output, file_basename, logger):
    '''Finds files that start with file_basename within dir_output.

    Full files should be something like file_basename2021_11_01_0930.dat
    but they can also have an "_1" before the file extension.

    Parameters
    ----------
    dir_output : string
        output of the 'dir' command on the correct directory on the loggenet PC
    file_basename : string
        Start of the full LoggerNet filename (without timestamp and extension)
    logger
        The logger object
    Returns
    -------
    list
        List of file names
    '''

    files = []
    dir_string = dir_output
    name_length = len(file_basename) + 19  # deal with '_X' files later
    while True:
        start = dir_string.find(file_basename)
        if start == -1:
            break
        # Now deal with multiple files with same timestamp, '_X', from LoggerNet.
        if dir_string[start + name_length - 4:start + name_length] == '.dat':
            f_name = dir_string[start:start + name_length]
        else:
            f_name = dir_string[start:start + name_length + 2]
            if f_name[-4:] != '.dat':
                logger.warning(f"This file shouldn't be here: {f_name}")
        files.append(f_name)
        dir_string = dir_string[start + len(f_name):]

    return files


def main():

    print("Starting running ingest_loggernet.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # ---- Set up logging:
    logger = logging.getLogger('olmo')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(
        config.output_dir, config.loggernet_logfile + datetime.datetime.now().strftime('%Y%m%d')), 'a+')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    logger.info("\n\n------ Starting data collection in main()")
    # ---- List files in the remote directory:
    stdout, stderr = dir_remote(config.loggernet_user, config.loggernet_pc)

    for file_type in config.loggernet_files_basenames:

        # ---- Get a list of files of the current file_type:
        files = get_file_list(stdout, file_type, logger)
        logger.info(f'{len(files)} files found on {config.loggernet_pc} for file type: {file_type}.')

        if len(files) < 2:
            logger.info(f"No new files to be ingested for file type: {file_type}.")
            continue

        print(f"Number of files: {len(files)}")

        # ---- scp the files over, then delete
        for f in files[:-1]:  # files[:-1]:  # Don't copy the latest file - it might be being written to.

            print(f"Ingesting file: {f}")

            # Check we don't have a version of the file locally.
            if os.path.isfile(os.path.join(config.loggernet_inbox, f)):
                logger.warning(f"File {f} to be copied already exists on this machine, moving to duplicate_data and skipping.")
                print(f"Warning: File {f} found already on this machine. Skipping. Moved to 'duplicate_data'.")
                stdout, stderr = move_remote(config.loggernet_user, config.loggernet_pc, f)
                logger.info(f"File move output: {stdout}")
                if stderr:
                    logger.warning(f"File {f} move not successful with message: {stderr}")
                continue

            # ---- scp over file:
            i = 0
            scp_file(
                config.loggernet_user, config.loggernet_pc,
                config.loggernet_outbox + "\\" + f, config.loggernet_inbox)
            if not os.path.isfile(os.path.join(config.loggernet_inbox, f)):
                logger.warning(f'Error: File {f} not copied.')
                while i < config.logpc_ssh_max_attempts:
                    logger.info("Will try again.")
                    time.sleep(2)
                    scp_file(
                        config.loggernet_user, config.loggernet_pc,
                        config.loggernet_outbox + "\\" + f, config.loggernet_inbox)
                    if os.path.isfile(os.path.join(config.loggernet_inbox, f)):
                        break
                    logger.warning(f'Error: File {f} not copied.')
                    i += 1
            if i == config.logpc_ssh_max_attempts:
                msg = f"Max tries exceeded. File {f} could not be copied. Exiting."
                logger.error(msg)
                print(msg)
                continue

            # ---- Ingest the file:
            loggernet.ingest_loggernet_file(os.path.join(config.loggernet_inbox, f), file_type)
            logger.info(f'Data for file {f} added to influxDB.')

            # ---- Remove the file from the origin PC (sintefutv012)
            i = 0
            rm_output = os.popen(f'''ssh {config.loggernet_user}@{config.loggernet_pc} "Del {config.loggernet_outbox}\\{f}"''').read()
            if rm_output:
                logger.warning("Remove original file failed.")
                while i < config.logpc_ssh_max_attempts:
                    logger.info("Will try again.")
                    time.sleep(2)
                    rm_output = os.popen(f'''ssh {config.loggernet_user}@{config.loggernet_pc} "Del {config.loggernet_outbox}\\{f}"''').read()
                    if not rm_output:
                        break
                    logger.warning(f'Error: File {f} not copied.')
                    i += 1
            if i == config.logpc_ssh_max_attempts:
                msg = "Max tries exceeded. Ignoring (this may cause build up of files on sintefutv012)."
                logger.error(msg)
                print(msg)
            print(f'Data for file {f} added to influxDB, file removed from sintefuvt012.')

    logger.info("All files transferred and ingested successfully, exiting.")


if __name__ == "__main__":
    main()
