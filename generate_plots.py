import os
import datetime
import re
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from influxdb import InfluxDBClient
import influxdb_client

import config
import util_az
import util_db
import util_file


def make_subplot(df, key, label):
    subplot = go.Scatter(x=df['time'], y=df[key], name=label)
    return subplot


def make_weather_plots(filename="weather_1d.html", upload_to_az=True):

    standard_timeslice = 'time > now() - 1d'
    standard_downsample = 'time(1m)'  # To turn off use: False
    plot_data = [
        {
            'title': 'Munkholmen Buoy Air Temperature',
            'ylabel': 'Temperature [deg]',
            'measurement': 'meteo_temperature_munkholmen',
            'variable': 'temperature',
            'timeslice': standard_timeslice,
            'lower_filter': -30,
            'upper_filter': 50,
            'downsample': standard_downsample,
        },
        {
            'title': 'Munkholmen Buoy Wind Speed',
            'ylabel': 'Wind speed [m/s]',
            'measurement': 'meteo_wind_speed_munkholmen',
            'variable': 'wind_speed',
            'timeslice': standard_timeslice,
            'lower_filter': 0,
            'upper_filter': 100,
            'downsample': standard_downsample,
        },
        {
            'title': 'Munkholmen Buoy Wind Direction',
            'ylabel': 'Wind direction [deg]',
            'measurement': 'meteo_wind_direction_munkholmen',
            'variable': 'wind_direction',
            'timeslice': standard_timeslice,
            'lower_filter': 0,
            'upper_filter': 360,
            'downsample': standard_downsample,
        },
        {
            'title': 'Munkholmen Buoy Humidity',
            'ylabel': 'Humidity [%]',
            'measurement': 'meteo_humidity_munkholmen',
            'variable': 'humidity',
            'timeslice': standard_timeslice,
            'lower_filter': 0,
            'upper_filter': 100,
            'downsample': standard_downsample,
        },
    ]

    # Below is the Azure DB client:
    admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    client = InfluxDBClient(
        host=config.az_influx_pc, port=8086,
        username=admin_user, password=admin_pwd,
        database='oceanlab')

    fig = make_subplots(rows=len(plot_data), cols=1, subplot_titles=[p['title'] for p in plot_data])
    fig.update_layout(template='plotly_white')
    for i, p in enumerate(plot_data):
        df = util_db.query_influxdb(client, p['measurement'], p['variable'], p['timeslice'], p['downsample'])

        # Simple simple filtering:
        df.loc[df[p['variable']] < p['lower_filter'], p['variable']] = np.nan
        df.loc[df[p['variable']] > p['upper_filter'], p['variable']] = np.nan

        # Make the actual 'plot'
        subplot = make_subplot(df, p['variable'], p['title'])
        fig.append_trace(subplot, row=i + 1, col=1)

        # Add axis labels:
        # fig.update_xaxes(title_text='Time', row=i + 1, col=1)  # Removed as overlaps with title
        fig.update_yaxes(title_text=p['ylabel'], row=i + 1, col=1)
    client.close()

    local_file = os.path.join(config.webfigs_dir, filename)
    az_file = 'influx_data/' + filename
    fig.update_layout(height=100 + 400 * len(plot_data), width=1200, showlegend=False)
    fig.write_html(local_file)
    if upload_to_az:
        util_az.upload_file(local_file, az_file, '$web', overwrite=True)


def name_to_bin_beam(name):
    '''Gets bin number and beam number from a munkholmen ADCP variable name.'''
    beam_num = re.findall('\d', name)[0]
    bin_num = name.split('(')[1].replace(')', '')
    return (int(bin_num), int(beam_num))


def adcp_raw_data_to_array(df, n_bins, n_beams):
    time = df.index.values
    arr = np.zeros((time.shape[0], n_bins, n_beams))
    for c in df.columns:
        bin_num, beam_num = name_to_bin_beam(c)
        arr[:, bin_num - 1, beam_num - 1] = df[c]
    return time, arr


def query_influx_table(measurement, timespan):

    influx_user, influx_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_read_credentials'))
    bucket = "oceanlab/autogen"
    token = f"{influx_user}:{influx_pwd}"
    url = "https://oceanlab.azure.sintef.no:8086"

    query = f'''from(bucket:"{bucket}")
 |> range({timespan})
 |> filter(fn:(r) => r._measurement == "{measurement}")'''

    with influxdb_client.InfluxDBClient(url=url, token=token) as client:
        result = client.query_api().query(query=query)

    n_time = len(result[0].records)
    n_vars = len(result)
    times = []
    var_names = []
    vals = np.zeros((n_time, n_vars))
    for table in result:
        var_names.append(table.records[0].get_field())
    for record in result[0]:
        times.append(record.get_time())
    for i, table in enumerate(result):
        for j, record in enumerate(table.records):
            vals[j, i] = record.get_value()

    return pd.DataFrame(data=vals, index=times, columns=var_names)


def make_adcp_plots_all(upload_to_az=True):

    n_bins = 28
    n_beams = 4
    blanking = 2
    cell_size = 3
    start_time = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')
    stop_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    time_range = f'start: {start_time}, stop: {stop_time}'
    # print(time_range)

    # Get the data and put into arrays:
    # data arrays have shape: [time, bins, beams]
    df = query_influx_table('signature_100_velocity_munkholmen', time_range)
    times, velocity = adcp_raw_data_to_array(df, n_bins, n_beams)
    df = query_influx_table('signature_100_correlation_munkholmen', time_range)
    times_c, correlation = adcp_raw_data_to_array(df, n_bins, n_beams)
    df = query_influx_table('signature_100_amplitude_munkholmen', time_range)
    times_a, amplitude = adcp_raw_data_to_array(df, n_bins, n_beams)

    for a in [amplitude, correlation, velocity]:
        a[a == -7999] = np.nan
    velocity[velocity == -32.77] = np.nan

    def add_remove_by_index(idx, df):
        '''Sets nans, or removes values such that df.index is the same as idx'''
        for i in np.setdiff1d(idx, df.index):
            df.loc[i, :] = np.nan
        df.drop(np.setdiff1d(df.index, idx), axis=0, inplace=True)
        return df
    df_pressure = query_influx_table('signature_100_pressure_munkholmen', time_range)
    # Make sure that the pressure values are present at ever point where we have data values.
    df_pressure = add_remove_by_index(df.index, df_pressure)

    offset = np.outer(df_pressure['pressure'].values, np.ones(n_bins))
    bin_num_matrix = np.outer(np.ones(times.shape[0]), np.arange(0, velocity.shape[1]))
    # depths is of shape [times, bin_number]
    depths = offset + blanking + (cell_size / 2) + bin_num_matrix * cell_size

    f, a = plt.subplots(2, 2, figsize=(20, 20), facecolor='white')
    a = a.flatten()

    AMIN = 50
    AMAX = 220
    CORMIN = 50
    CORMAX = 100
    VMAX = 0.2
    YLIM = (86, 0)

    def set_plots():
        plt.colorbar()
        plt.gca().invert_yaxis()
        plt.xticks(rotation=90)
        plt.ylim(YLIM)
        plt.ylabel('Depth (m)')
        plt.plot(times_mat, offset, 'k', linewidth=1)
        # plt.vlines(pd.to_datetime('2022-06-24 11:10:00'), 100, 0, 'g', linewidth=2)
        # plt.vlines(pd.to_datetime('2022-06-22 13:00:00'), 100, 0, 'g', linewidth=2)

    times_mat = np.tile(times, [n_bins, 1]).T

    # Turn off warnings for plotting becaeuse...
    warnings.filterwarnings('ignore')
    for i in range(4):
        plt.sca(a[i])
        plt.pcolor(times_mat, depths, amplitude[:, :, i], shading="nearest", vmin=AMIN, vmax=AMAX, cmap='cmo.amp')
        plt.title(f'Amplitude {i + 1}')
        set_plots()

    plt.savefig(
        os.path.join(config.output_dir, 'ADCP_Amplitude.png'),
        dpi=300, bbox_inches='tight', transparent=False)

    f, a = plt.subplots(2, 2, figsize=(20, 20), facecolor='white')
    a = a.flatten()

    for i in range(4):
        plt.sca(a[i])
        plt.pcolor(times_mat, depths, correlation[:, :, i], shading="nearest", vmin=CORMIN, vmax=CORMAX, cmap='cmo.amp')
        plt.title(f'Correlation {i + 1}')
        set_plots()

    plt.savefig(
        os.path.join(config.output_dir, 'ADCP_Correlation.png'),
        dpi=300, bbox_inches='tight', transparent=False)

    f, a = plt.subplots(2, 2, figsize=(20, 20), facecolor='white')
    a = a.flatten()

    def set_plots():
        plt.colorbar()
        plt.gca().invert_yaxis()
        plt.xticks(rotation=90)
        plt.ylim(YLIM)
        plt.ylabel('Depth (m)')
        plt.plot(times_mat, offset, 'k', linewidth=1)
        # plt.vlines(pd.to_datetime('2022-06-24 11:10:00'), 100, 0, 'g', linewidth=2)
        # plt.vlines(pd.to_datetime('2022-06-22 13:00:00'), 100, 0, 'g', linewidth=2)

    titlestr = ['North', 'East', 'Up', 'Up']
    for i in range(4):
        plt.sca(a[i])
        plt.pcolor(times_mat, depths, velocity[:, :, i], shading="nearest", vmin=-VMAX, vmax=VMAX, cmap='cmo.balance')
        plt.title(f'Velocity {i + 1}' + titlestr[i])
        set_plots()

    plt.savefig(
        os.path.join(config.output_dir, 'ADCP_Velocity.png'),
        dpi=300, bbox_inches='tight', transparent=False)
    warnings.filterwarnings('always')

    if upload_to_az:
        for f in ['ADCP_Amplitude.png', 'ADCP_Correlation.png', 'ADCP_Velocity.png']:
            az_file = 'adcp/' + f
            if upload_to_az:
                util_az.upload_file(os.path.join(config.output_dir, f), az_file, '$web', overwrite=True)


def make_velocity_plots(days=2, upload_to_az=True):

    n_bins = 28
    n_beams = 4
    blanking = 2
    cell_size = 3
    start_time = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    stop_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    time_range = f'start: {start_time}, stop: {stop_time}'
    # print(time_range)

    # Get the data and put into arrays:
    # data arrays have shape: [time, bins, beams]
    df = query_influx_table('signature_100_velocity_munkholmen', time_range)
    times, velocity = adcp_raw_data_to_array(df, n_bins, n_beams)

    velocity[velocity == -32.77] = np.nan

    def add_remove_by_index(idx, df):
        '''Sets nans, or removes values such that df.index is the same as idx'''
        for i in np.setdiff1d(idx, df.index):
            df.loc[i, :] = np.nan
        df.drop(np.setdiff1d(df.index, idx), axis=0, inplace=True)
        return df
    df_pressure = query_influx_table('signature_100_pressure_munkholmen', time_range)
    # Make sure that the pressure values are present at ever point where we have data values.
    df_pressure = add_remove_by_index(df.index, df_pressure)

    offset = np.outer(df_pressure['pressure'].values, np.ones(n_bins))
    bin_num_matrix = np.outer(np.ones(times.shape[0]), np.arange(0, velocity.shape[1]))
    # depths is of shape [times, bin_number]
    depths = offset + blanking + (cell_size / 2) + bin_num_matrix * cell_size

    VMAX = 0.2
    YLIM = (86, 0)

    times_mat = np.tile(times, [n_bins, 1]).T

    plt.style.use('dark_background')

    # Turn off warnings for plotting becaeuse...
    warnings.filterwarnings('ignore')

    f, a = plt.subplots(2, 2, figsize=(20, 20), facecolor='white')
    a = a.flatten()

    def set_plots():
        cb = plt.colorbar()
        cb.ax.set_ylabel('Velocity (m/s)')
        plt.gca().invert_yaxis()
        plt.xticks(rotation=90)
        plt.ylim(YLIM)
        plt.ylabel('Depth (m)')
        plt.plot(times_mat, offset, 'w', linewidth=1)

    titlestr = ['Eastward', 'Northward', 'Upward', 'Upward']
    for i in range(4):
        plt.sca(a[i])
        plt.pcolor(times_mat, depths, velocity[:, :, i], shading="nearest",
                   vmin=-VMAX, vmax=VMAX, cmap='cmo.balance')
        if i == 0:
            plt.title(str(days) + ' days\n' + 'Figure updated at ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' (local time, Trondheim)\n' + f'[Velocity {i + 1}] ' + titlestr[i], loc='left')
        else:
            plt.title(f'[Velocity {i + 1}] ' + titlestr[i], loc='left')
        set_plots()

    fig_filename = os.path.join(config.output_dir, 'ADCP_Velocity_' + str(days) + '.png')
    plt.savefig(fig_filename, dpi=300, bbox_inches='tight', transparent=False)
    az_file = 'adcp/' + os.path.split(fig_filename)[-1]
    util_az.upload_file(fig_filename, az_file, '$web', content_type='image/png', overwrite=True)
    warnings.filterwarnings('always')


def main():
    print("Starting running generate_plots.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    make_weather_plots()

    make_velocity_plots(days=2)
    make_velocity_plots(days=7)


if __name__ == "__main__":
    main()
