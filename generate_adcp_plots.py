import os
import subprocess
import datetime
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from influxdb import InfluxDBClient
import matplotlib.pyplot as plt

import config
import util_file
import util_db


def adcp_from_influx(client, timeslice):
    sig_tables = ['signature_100_amplitude_munkholmen', 'signature_100_battery_voltage_munkholmen', 'signature_100_correlation_munkholmen', 'signature_100_current_direction_munkholmen', 'signature_100_current_speed_munkholmen', 'signature_100_error_code_munkholmen',
                  'signature_100_heading_munkholmen', 'signature_100_pitch_munkholmen', 'signature_100_pressure_munkholmen', 'signature_100_roll_munkholmen', 'signature_100_sound_speed_munkholmen', 'signature_100_temperature_munkholmen', 'signature_100_velocity_munkholmen']

    df = pd.DataFrame(columns=['time'])
    for i, tab in enumerate(sig_tables):
        print('loading:', tab)
        df_new = util_db.query_influxdb(client, tab, '*', timeslice, False, approved='all')
        df_new = df_new.drop(columns=['approved', 'data_level', 'edge_device', 'platform', 'sensor', 'unit'])
        df = pd.merge(df, df_new, on='time', how='outer')

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


# standard_timeslice = 'time > now() - 5d'
standard_timeslice = 'time > now() - 60d'
standard_downsample = 'time(1m)'  # To turn off use: False
plot = {
    'title': 'Munkholmen ADCP Speed',
    'ylabel': 'Speed [m/s]',
    'timeslice': standard_timeslice,
    'lower_filter': -3000,
    'upper_filter': 5000,
    'downsample': False,
    'approved': 'all',
}


def main():
    print("Starting running generate_adcp_plots.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # Below is the Azure DB client:
    admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
    client = InfluxDBClient(
        host=config.az_influx_pc, port=8086,
        username=admin_user, password=admin_pwd,
        database='oceanlab')

    fig = make_subplots(rows=1, cols=1, subplot_titles=plot['title'])
    fig.update_layout(template='plotly_white')
    # df = util_db.query_influxdb(client, p['measurement'], p['variable'], p['timeslice'], p['downsample'], approved=p['approved'])
    df = adcp_from_influx(client, plot['timeslice'])

    return df

    data_cols = list(df.columns)

    current_speed_cols = [d for d in data_cols if d.startswith('current_speed')]
    current_speed = df[current_speed_cols].values.T

    for name in ['velocity', 'amplitude', 'correlation']:
        for i in range(1,5):
            exec(f'{name}{i}_cols = [d for d in data_cols if d.startswith("{name}{i}")]')
            exec(f'{name}{i} = df[{name}{i}_cols].values.T')
            exec(f'{name}{i}[{name}{i} == -32.77] = np.nan')

    # some setup constants that should be read from the CR6 once mobilis does an update
    blanking = 2.0 #m
    cell_size = 3.0 #m

    number_of_timstamps = df.shape[0]

    number_of_depths = 0
    for i, c in enumerate(velocity1_cols):
        cell_number = int(c.split('(')[1].rstrip(')'))
        number_of_depths = np.max((number_of_depths, cell_number))

    depth = np.zeros((number_of_depths, number_of_timstamps), dtype=np.float64)
    for d in range(number_of_depths):
        depth[d, :] = (df['pressure'].values + blanking + (cell_size / 2)) + ((d) * cell_size)

    f, a = plt.subplots(2,2, figsize=(20,20))

    VMAX = 0.25
    YLIM =(80, 0)

    def set_plots():
        plt.colorbar()
        plt.gca().invert_yaxis()
        plt.xticks(rotation = 45)
        plt.ylim(YLIM)
        plt.ylabel('Depth (m)')

    plt.sca(a[0,0])
    plt.pcolor(df.time, depth, amplitude1) #, vmin=0, vmax=VMAX)
    plt.title('amplitude1')
    set_plots()

    plt.sca(a[0,1])
    plt.pcolor(df.time, depth, amplitude2) #, vmin=0, vmax=VMAX)
    plt.title('amplitude2')
    set_plots()

    plt.sca(a[1,0])
    plt.pcolor(df.time, depth, amplitude3) #, vmin=0, vmax=VMAX)
    plt.title('amplitude3')
    set_plots()

    plt.sca(a[1,1])
    plt.pcolor(df.time, depth, amplitude4) #, vmin=0, vmax=VMAX)
    plt.title('amplitude4')
    set_plots()

    f, a = plt.subplots(2,2, figsize=(20,20))

    plt.sca(a[0,0])
    plt.pcolor(df.time, depth, velocity1) #, vmin=0, vmax=VMAX)
    plt.title('velocity1')
    set_plots()

    plt.sca(a[0,1])
    plt.pcolor(df.time, depth, velocity2) #, vmin=0, vmax=VMAX)
    plt.title('velocity2')
    set_plots()

    plt.sca(a[1,0])
    plt.pcolor(df.time, depth, velocity3) #, vmin=0, vmax=VMAX)
    plt.title('velocity3')
    set_plots()

    plt.sca(a[1,1])
    plt.pcolor(df.time, depth, velocity4) #, vmin=0, vmax=VMAX)
    plt.title('velocity4')
    set_plots()

    

    return
    
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
