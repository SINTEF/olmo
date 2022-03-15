import os
import re
import logging
import datetime
import paramiko
import numpy as np

import config

logger = logging.getLogger('olmo.util')


def change_dir(filepath, new_dir):
    filename = os.path.split(filepath)[-1]
    return os.path.join(new_dir, filename)


def add_timestring(filepath, time_stamp):
    if '.' in filepath:
        name, ext = filepath.rsplit('.', 1)
        return name + '_' + time_stamp + '.' + ext
    else:
        return filepath + '_' + time_stamp


def remove_timestring(filepath):
    '''
    This function is the assumed pair of the 'add_timestring()' fn. above.
    '''
    if '.' in filepath:
        name, ext = filepath.rsplit('.', 1)
        base_name, time_stamp = name.rsplit('_', 1)
        error_msg = f"Completion flag date, {time_stamp}, conatains non ints."
        assert not re.findall("[^0-9]", time_stamp, re.MULTILINE), error_msg
        return base_name + '.' + ext
    else:
        base_name, time_stamp = filepath.rsplit('_', 1)
        error_msg = f"Completion flag date, {time_stamp}, conatains non ints."
        assert not re.findall("[^0-9]", time_stamp, re.MULTILINE), error_msg
        return base_name


def ls_remote(user, machine, directory, port=22):
    '''Perform 'ls' over ssh onto linux machine.'''
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(machine, username=user, port=port)

    command = f"ls {directory}"
    stdin, stdout, stderr = ssh.exec_command(command)

    return stdout.read().decode(errors='ignore'), stderr.read().decode(errors='ignore')


def get_files_list(ls_string, pattern, drop_recent=1):
    '''DEPRECATED, see class definition.
    Use regex to find all files in ls_string matching pattern.

    Note that currently the most recent files are simpy those listed
    last in the ls_string

    Parameters
    ----------
    ls_string : str
        string to search within
    pattern : str
        regex pattern for file name convention
    drop_recent : int
        Number of most recent files to drop

    Returns
    -------
    list
    '''

    files = []
    while True:
        match = re.search(pattern, ls_string)
        if match is None:
            break
        else:
            files.append(match.group())
            ls_string = ls_string[match.span()[1]:]

    if len(files) < drop_recent + 1:
        logger.info(f"No new files found matching regex pattern: {pattern}")
        return None
    elif drop_recent == 0:
        return files
    else:
        return files[:-drop_recent]


def force_float_cols(df, float_cols=None, not_float_cols=None):
    '''Avoid problem where float cols give error if they "round to zero"'''
    if float_cols is not None:
        assert not_float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col in float_cols:
                df[col] = df[col].astype(np.float64)
        return df
    elif not_float_cols is not None:
        assert float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col not in not_float_cols:
                df[col] = df[col].astype(np.float64)
        return df
    else:
        raise ValueError("'float_cols', or 'not_float_cols' should be a list of cols.")


def get_influx_user_pwd(file):

    '''Get user and pwd from a "credentials file".

    File is expect to be formatted like this:
    USER=good_username
    PWD=12345password
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
    '''Define the logger object for logging.'''

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(
        config.output_dir, logfile + datetime.datetime.now().strftime('%Y%m%d')), 'a+')
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    return logger


# Currently not sure if I need this, so ignoring for now.
# def execute_subprocess(command, communicate=True, timeout=600):
#     '''Executes a terminal command using subrpocess.Popen.

#     Parameters
#     ----------
#     command : list
#         Command to be run.
#     communicate : bool
#         If we should return the stdout and stderr

#     Returns
#     -------
#     stdout, stderr
#         Optional. Output and error from running the command.
#     '''
#     if communicate:
#         proc = subprocess.Popen(
#             command,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE)
#         try:
#             stdout, stderr = proc.communicate(timeout=timeout)
#         except TimeoutExpired:
#             proc.kill()
#             outs, errs = proc.communicate()
