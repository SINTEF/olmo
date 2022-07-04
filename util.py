import os
import re
import subprocess
import logging
import datetime
import paramiko
import numpy as np
import pandas as pd

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


def force_float_cols(df, float_cols=None, not_float_cols=None, error_to_nan=False):
    '''Avoid problem where float cols give error if they "round to zero"'''
    if float_cols is not None:
        assert not_float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col in float_cols:
                if error_to_nan:
                    df[col] = df[col].apply(pd.to_numeric, errors='coerce')
                else:
                    df[col] = df[col].astype(np.float64)
        return df
    elif not_float_cols is not None:
        assert float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col not in not_float_cols:
                if error_to_nan:
                    df[col] = df[col].apply(pd.to_numeric, errors='coerce')
                else:
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


def query_influxdb(client, measurement, variable, timeslice, downsample, approved='yes'):

    if approved == 'all':
        approved_text = ''
    else:
        approved_text = f'''AND "approved" = '{approved}' '''

    if variable == '*':
        variable_text = '*'
        df = pd.DataFrame(columns=['time'])
    elif isinstance(variable, list):
        variable_text = ", ".join(variable)
        df = pd.DataFrame(columns=variable.insert(0, 'time'))
    else:
        variable_text = f'"{variable}"'
        df = pd.DataFrame(columns=['time', variable])

    if downsample:
        q = f'''SELECT mean({variable_text}) AS "{variable}" FROM "{measurement}" WHERE {timeslice} {approved_text}GROUP BY {downsample}'''
    else:
        q = f'''SELECT {variable_text} FROM "{measurement}" WHERE {timeslice} {approved_text}'''

    result = client.query(q)
    for table in result:
        # Not sure this works if there are multiple tables.
        col_names = [k for k in table[0].keys()]
        col_vals = [[] for _ in col_names]
        for pt in table:
            for i, v in enumerate(pt.values()):
                col_vals[i].append(v)
        df = pd.DataFrame.from_dict({col_names[i]: col_vals[i] for i in range(len(col_names))})
    try:
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df


def add_tags(df, tag_values):
    '''Adds tags to a dataframe. tag_values needs be a correct dict.'''
    for (k, v) in tag_values.items():
        df[k] = v
    return df


def filter_and_tag_df(df_all, field_keys, tag_values):
    '''Returns a df with tag_values and field_key values'''
    df = df_all.loc[:, [k for k in field_keys.keys()]]
    df = df.rename(columns=field_keys)
    df = add_tags(df, tag_values)
    return df


def upload_file(local_file, az_file, container, content_type='text/html', overwrite=True):

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
