import os
import re
import logging
import datetime
import paramiko

import config

logger = logging.getLogger('olmo.util_file')


def change_dir(filepath, new_dir):
    '''
    Changes a file path to have same file name, but be in new dir

    Parameters
    ----------
    filepath : str

    Returns
    -------
    str
    '''
    filename = os.path.split(filepath)[-1]
    return os.path.join(new_dir, filename)


def add_timestring(filepath, timestring):
    '''
    Appends a time_stamp to a filename (before the extension).

    Parameters
    ----------
    filepath : str

    Returns
    -------
    str
    '''
    if '.' in filepath:
        name, ext = filepath.rsplit('.', 1)
        return name + '_' + timestring + '.' + ext
    else:
        return filepath + '_' + timestring


def remove_timestring(filepath):
    '''
    Removes a timestring from the end of a filename.
    This function is the assumed pair of the 'util_file.add_timestring()'.

    Parameters
    ----------
    filepath : str

    Returns
    -------
    str
    '''

    if '.' in filepath:
        name, ext = filepath.rsplit('.', 1)
        base_name, timestring = name.rsplit('_', 1)
        error_msg = f"Completion flag date, {timestring}, conatains non ints."
        assert not re.findall("[^0-9]", timestring, re.MULTILINE), error_msg
        return base_name + '.' + ext
    else:
        base_name, timestring = filepath.rsplit('_', 1)
        error_msg = f"Completion flag date, {timestring}, conatains non ints."
        assert not re.findall("[^0-9]", timestring, re.MULTILINE), error_msg
        return base_name


def ls_remote(user, machine, directory, port=22, custom_search=None):
    '''
    Perform 'ls' over ssh onto linux machine.

    Parameters
    ----------
    user : str
    machine : str
        IP address of the maching you will connect to.
    directory : str
    port : int, default 22

    Returns
    -------
    str
    '''
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machine, username=user, port=port)

    command = f"ls {directory}"
    stdin, stdout, stderr = ssh.exec_command(command)
    stdout = stdout.read().decode(errors='ignore'), stderr.read().decode(errors='ignore')

    return stdout


def find_remote(user, machine, directory, serach, port=22):
    '''
    Perform 'find {directory} -name '{custom_search}'"' over ssh onto linux machine.

    Parameters
    ----------
    user : str
    machine : str
        IP address of the maching you will connect to.
    directory : str
    search : str
    port : int, default 22

    Returns
    -------
    str
    '''
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machine, username=user, port=port)

    command = f"find {directory} -name '{serach}'"
    stdin, stdout, stderr = ssh.exec_command(command)
    stdout = stdout.read().decode(errors='ignore'), stderr.read().decode(errors='ignore')

    return stdout


def get_user_pwd(file):
    '''
    Get user and pwd from a "credentials file".

    File is expect to be formatted like this:
    USER=good_username
    PWD=12345password

    Parameters
    ----------
    file : str
        Full path to credential file.

    Returns
    -------
    str, str
    '''
    with open(file, 'r') as f:
        user_line = f.readline().rstrip('\n')
        pwd_line = f.readline().rstrip('\n')
    assert user_line[:5] == 'USER=', f"Credentials file {file} not correct format."
    assert pwd_line[:4] == 'PWD=', f"Credentials file {file} not correct format."
    user = user_line[5:]
    pwd = pwd_line[4:]
    return user, pwd


def init_logger(logfile, name='olmo'):
    '''
    Define the logger object for logging.

    Parameters
    ----------
    logfile : str
        Full path of the output log file.
    name : str
        Name of the logger, used by the logging library.

    Returns
    -------
        logger."logging-object"
    '''

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(
        config.output_dir, logfile + datetime.datetime.now().strftime('%Y%m%d')), 'a+')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    return logger
