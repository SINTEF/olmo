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


def time_series(df, key, label, title):

    trace1 = go.Scatter(x=df['time'], y=df[key], name=label)
    # trace2 = go.Scatter(x=df['date'], y=df['tmin [degC]'], name='tmin [degC]')

    data = [trace1]  # , trace2]

    layout = go.Layout(title=title,
                       yaxis=dict(title=label),
                       template='plotly_white'
                       )

    fig = go.Figure(data=data, layout=layout)
    return fig


def query_influxdb(client, measurement, variable, timeslice, downsample):

    if downsample:
        q = f'SELECT mean("{variable}") AS "{variable}" FROM "{measurement}" WHERE {timeslice} GROUP BY {downsample}'
    else:
        q = f'SELECT "{variable}" FROM "{measurement}" WHERE {timeslice}'
    print(q)

    result = client.query(q)
    df = pd.DataFrame(columns=['time', variable])
    for table in result:
        for pt in table:
            df = df.append(pt, ignore_index=True)
    # Assuming that influx reports times in UTC:
    df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df


def make_subplot(df):
    return go.Scatter(x=[3, 4, 5], y=[1000, 1100, 1200])


def query_and_upload_measurement(
        name, axis_label, plotname,
        variable, measurement, timeslice, client,
        lower_filter=-100., upper_filter=100.):

    data = query_influxdb(client, measurement, variable, timeslice)

    data.loc[data[variable] < lower_filter, variable] = np.nan
    data.loc[data[variable] > upper_filter, variable] = np.nan

    filename = f"{name}.html"
    local_file = os.path.join(config.webfigs_dir, filename)
    az_file = 'influx_data/' + filename
    fig = time_series(data, variable, axis_label, plotname)
    fig.write_html(local_file)

    # ---- Upload to azure:
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
    print(f"file uploaded for {name}")
    if process.returncode != 0:
        # logger.error("az file upload failed. stderr:\n" + stderr.decode(errors="ignore"))
        print("we got an error")
        raise ValueError("process.returncode != 0.\n" + stderr.decode(errors="ignore"))
    # logger.info('Backup, archive and transfer to azure completed successfully.')


plots = [
    {
        'name': 'wind_1d',
        'title': 'Munkholmen Wind Speed',
        'ylabel': 'Wind speed [m/s]',
        'measurement': 'meteo_wind_speed_munkholmen',
        'variable': 'wind_speed',
        'timeslice': 'time > now() - 1d',
        'lower_filter': -100,
        'upper_filter': 100,
        'downsample': 'time(1m)'
    }
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

    fig = make_subplots(rows=len(plots), cols=1)
    for i, p in enumerate(plots):
        df = query_influxdb(client, p['measurement'], p['variable'], p['timeslice'], p['downsample'])
        print(i)
        print(df)

        subplot = make_subplot(df)
        print(subplot)

        fig.append_trace(subplot), row=i + 1, col=1)

        # fig.append_trace(go.Scatter(
        # x=[3, 4, 5],
        # y=[1000, 1100, 1200],
        # ), row=1, col=1)

    # query_and_upload_measurement(
    #     'wind_1d', 'Wind speed [m/s]', 'Munkholmen Wind Speed',
    #     'wind_speed_digital', 'loggernet_public', 'time > now() - 1d', client,
    #     lower_filter=-100., upper_filter=100.)

    # query_and_upload_measurement(
    #     'wind_direction_1d', 'Wind direction [deg]', 'Munkholmen Wind Direction',
    #     'wind_direction_digital', 'loggernet_public', 'time > now() - 1d', client,
    #     lower_filter=-10., upper_filter=370.)

    # query_and_upload_measurement(
    #     'temp_1d', 'Temperature [deg]', 'Munkholmen Air Temperature',
    #     'temperature_digital', 'loggernet_public', 'time > now() - 1d', client,
    #     lower_filter=-100., upper_filter=100.)

    # query_and_upload_measurement(
    #     'pressure_1d', 'Air pressure [hPa]', 'Munkholmen Air Pressure',
    #     'pressure_digital', 'loggernet_public', 'time > now() - 1d', client,
    #     lower_filter=0., upper_filter=1500.)

    # query_and_upload_measurement(
    #     'humidity_1d', 'Humidity', 'Munkholmen Humidity',
    #     'humidity_digital', 'loggernet_public', 'time > now() - 1d', client,
    #     lower_filter=-10., upper_filter=110.)

    print("done with all")


if __name__ == "__main__":
    main()
