import os
import subprocess
import datetime
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from influxdb import InfluxDBClient

import config
import util

def adcp_from_influx(client, timeslice):
    q = f'''SELECT * FROM signature_100_current_speed_munkholmen WHERE {timeslice}'''
    result = client.query(q)

    columns = []
    for table in result:
        for k in table[0].keys():
            columns.append(k)

    df = pd.DataFrame(columns=columns)
    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)

    q = f'''SELECT * FROM signature_100_current_direction_munkholmen WHERE {timeslice}'''
    result = client.query(q)

    columns = []
    for table in result:
        for k in table[0].keys():
            columns.append(k)

    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)

    q = f'''SELECT * FROM signature_100_pressure_munkholmen WHERE {timeslice}'''
    result = client.query(q)

    columns = []
    for table in result:
        for k in table[0].keys():
            columns.append(k)

    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)


    # Assuming that influx reports times in UTC:
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df


def adcp_speed_from_influx(client, timeslice):
    
    q = f'''SELECT * FROM signature_100_current_speed_munkholmen WHERE {timeslice}'''
    result = client.query(q)

    columns = []
    for table in result:
        for k in table[0].keys():
            columns.append(k)

    df = pd.DataFrame(columns=columns)
    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)
    # Assuming that influx reports times in UTC:
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df

def query_influxdb(client, measurement, variable, timeslice, downsample, approved='yes'):

    if downsample:
        q = f'''SELECT mean("{variable}") AS "{variable}" FROM "{measurement}" WHERE {timeslice} AND "approved" = '{approved}' GROUP BY {downsample}'''
    else:
        q = f'''SELECT "{variable}" FROM "{measurement}" WHERE {timeslice} AND "approved" = '{approved}' '''

    result = client.query(q)
    df = pd.DataFrame(columns=['time', variable])
    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)
    # Assuming that influx reports times in UTC:
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df


def make_subplot(df, key, label):
    subplot = go.Scatter(x=df['time'], y=df[key], name=label)
    return subplot


def upload_figure(local_file, az_file):

    with open(os.path.join(config.secrets_dir, 'azure_token_web')) as f:
        aztoken = f.read()
    process = subprocess.Popen([
        'az', 'storage', 'fs', 'file', 'upload',
        '--source', local_file, '-p', az_file,
        '-f', '$web', '--account-name', 'oceanlabdlstorage', '--overwrite',
        '--content-type', 'text/html',
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


standard_timeslice = 'time > now() - 1d'
standard_downsample = 'time(1m)'  # To turn off use: False
plot = {
    'title': 'Munkholmen ADCP Speed',
    'ylabel': 'Speed [m/s]',
    'timeslice': standard_timeslice,
    'lower_filter': -3000,
    'upper_filter': 5000,
    'downsample': False,
    'approved': 'none',
    }


def main():
    print("Starting running generate_adco_plots.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # some setup constants that should be read from the CR6 once mobilis does an update
    blanking = 2.0 #m
    cell_size = 5.0 #m

    # Below is the Azure DB client:
    admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    client = InfluxDBClient(
        host=config.az_influx_pc, port=8086,
        username=admin_user, password=admin_pwd,
        database='oceanlab')

    fig = make_subplots(rows=1, cols=1, subplot_titles=plot['title'])
    fig.update_layout(template='plotly_white')
    # df = query_influxdb(client, p['measurement'], p['variable'], p['timeslice'], p['downsample'], approved=p['approved'])
    df = adcp_from_influx(client, plot['timeslice'])
    
    columns = df.columns
    print(columns)

    data_cols = list(df.columns[2:])

    speed_cols = [d for d in data_cols if d.startswith('current_speed_adcp')]
    direction_cols = [d for d in data_cols if d.startswith('current_direction_adcp')]

    depth = np.zeros(len(speed_cols))
    speed = np.zeros(len(speed_cols))
    direction = np.zeros(len(direction_cols))

    for i, c in enumerate(speed_cols):
        cell_number = int(c.split('(')[1].rstrip(')'))
        depth[i] = (df['pressure'].values + blanking + cell_size / 2) + ((cell_number - 1) * cell_size)
        speed[i] = df[c]
        
    for i, c in enumerate(direction_cols):
        direction[i] = df_all[c]

    return

    # Simple simple filtering:
    df.loc[df[p['variable']] < p['lower_filter'], p['variable']] = np.nan
    df.loc[df[p['variable']] > p['upper_filter'], p['variable']] = np.nan

    # Make the actual 'plot'
    subplot = make_subplot(df, p['variable'], p['title'])
    fig.append_trace(subplot, row=i + 1, col=1)

    # Add axis labels:
    # fig.update_xaxes(title_text='Time', row=i + 1, col=1)  # Removed as overlaps with title
    fig.update_yaxes(title_text=p['ylabel'], row=i + 1, col=1)

    filename = "adcp.html"
    local_file = os.path.join(config.webfigs_dir, filename)
    az_file = 'influx_data/' + filename
    fig.update_layout(height=100 + 400 * len(plots), width=1200, showlegend=False)
    fig.write_html(local_file)
    upload_figure(local_file, az_file)


if __name__ == "__main__":
    main()
