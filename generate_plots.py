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


def query_influxdb(client, measurement, variable, timeslice, downsample):

    if downsample:
        q = f'SELECT mean("{variable}") AS "{variable}" FROM "{measurement}" WHERE {timeslice} GROUP BY {downsample}'
    else:
        q = f'SELECT "{variable}" FROM "{measurement}" WHERE {timeslice}'

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
standard_downsample = 'time(1m)'
plots = [
    {
        'title': 'Munkholmen Air Temperature',
        'ylabel': 'Temperature [deg]',
        'measurement': 'meteo_temperature_munkholmen',
        'variable': 'temperature',
        'timeslice': standard_timeslice,
        'lower_filter': -30,
        'upper_filter': 50,
        'downsample': standard_downsample,
    },
    {
        'title': 'Munkholmen Wind Speed',
        'ylabel': 'Wind speed [m/s]',
        'measurement': 'meteo_wind_speed_munkholmen',
        'variable': 'wind_speed',
        'timeslice': standard_timeslice,
        'lower_filter': 0,
        'upper_filter': 100,
        'downsample': standard_downsample,
    },
    {
        'title': 'Munkholmen Wind Direction',
        'ylabel': 'Wind direction [deg]',
        'measurement': 'meteo_wind_direction_munkholmen',
        'variable': 'wind_direction',
        'timeslice': standard_timeslice,
        'lower_filter': 0,
        'upper_filter': 360,
        'downsample': standard_downsample,
    },
    # {
    #     'title': 'Munkholmen Air Pressure',
    #     'ylabel': 'Air pressure [hPa]',
    #     'measurement': 'meteo_atmospheric_pressure_munkholmen',
    #     'variable': 'atmospheric_pressure',
    #     'timeslice': standard_timeslice,
    #     'lower_filter': 800,
    #     'upper_filter': 1200,
    #     'downsample': standard_downsample,
    # },
    {
        'title': 'Munkholmen Humidity',
        'ylabel': 'Humidity [%]',
        'measurement': 'meteo_humidity_munkholmen',
        'variable': 'humidity',
        'timeslice': standard_timeslice,
        'lower_filter': 0,
        'upper_filter': 100,
        'downsample': standard_downsample,
    },
]


def main():
    print("Starting running generate_plots.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Below is the Azure DB client:
    admin_user, admin_pwd = util.get_influx_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    client = InfluxDBClient(
        host=config.az_influx_pc, port=8086,
        username=admin_user, password=admin_pwd,
        database='oceanlab')

    fig = make_subplots(rows=len(plots), cols=1, subplot_titles=[p['title'] for p in plots])
    fig.update_layout(template='plotly_white')
    for i, p in enumerate(plots):
        df = query_influxdb(client, p['measurement'], p['variable'], p['timeslice'], p['downsample'])

        # Simple simple filtering:
        df.loc[df[p['variable']] < p['lower_filter'], p['variable']] = np.nan
        df.loc[df[p['variable']] > p['upper_filter'], p['variable']] = np.nan

        # Make the actual 'plot'
        subplot = make_subplot(df, p['variable'], p['title'])
        fig.append_trace(subplot, row=i + 1, col=1)

        # Add axis labels:
        # fig.update_xaxes(title_text='Time', row=i + 1, col=1)  # Removed as overlaps with title
        fig.update_yaxes(title_text=p['ylabel'], row=i + 1, col=1)

    filename = "weather_1d.html"
    local_file = os.path.join(config.webfigs_dir, filename)
    az_file = 'influx_data/' + filename
    fig.update_layout(height=100 + 400 * len(plots), width=1200, showlegend=False)
    fig.write_html(local_file)
    upload_figure(local_file, az_file)


if __name__ == "__main__":
    main()
