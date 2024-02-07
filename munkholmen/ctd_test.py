import serial
import time
import pandas as pd
from io import StringIO
import re
import numpy as np
import glob
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

if os.environ.get('DISPLAY','') == '':
    print('no display found. Using :0.0')
    os.environ.__setitem__('DISPLAY', ':0.0')

#CTD_COM = '/dev/ttyUSB0'
CTD_COM = '/dev/ttyUSB1'

def send_str(txt):
    ser = send_to_ctd(str(txt), leave_open=True)
    flush(ser=ser)


def send_to_ctd(command_string, leave_open=False):
    ser = serial.Serial(CTD_COM, 9600, timeout=1)
    ser.flushInput() #flush serial port input
    ser.write(command_string.encode()+ b"\n")
    time.sleep(0.1)
    if leave_open:
        return ser
    else:
        ser.close()


def flush(ser=None):
    t=0
    retries=4
    if ser is None:
        ser = serial.Serial(CTD_COM, 9600, timeout=1)
    lines = ser.read()
    #while len(lines) != 0:
    while True:
        #lines = ser.read(100)
        lines = ser.readall()
        if len(lines) == 0:
            time.sleep(0.1)
            t += 1
            if t > retries:
                print('TIMEOUT')
                break
            print('.')
            continue
        print(lines)
        t=0
        if lines.endswith(b'<Executed/>'):
            break


def wake_ctd(ser):
    readout=b'no'

    print('Starting wakeup:')
    while not readout.endswith(b'<Executed/>'):
        ser.flushInput() #flush serial port input
        ser.flushOutput() #flush serial port input
        stop_ctd(ser)
        for i in range(5):
            ser.write(b'\r') #send a carriage return to the instruments until it responds wit an <Executed/> promtpt
            time.sleep(0.1)
        print('    readall')
        readout = ser.read(200)
        print(readout)
        print('    stop')
        stop_ctd(ser)

    print('wakeup done.')


def download(ser):
    ser.write(b'DC48,48\r')


def get_header(ser):
    ser.write(b'DH\r')


def start_ctd(ser):
    ser.write(b'Startnow\r')

def stop_ctd(ser):
    ser.write(b'Stop\r')


def configure_outputs(ser):
    ser.write(b'OutputSal=Y\r')
    time.sleep(0.5)
    ser.write(b'OutputFormat=3\r')


def ctd_status(ser):
    ser.write(b'DS\r')


def ctd_sleep(ser):
    ser.write(b'QS\r') #QS command to put the istrument into low power mode


def string_to_df(filename, string, timestamp):
    parsed_str = string.decode()
    parsed_str = [x.split(',') for x in parsed_str.split('\r\n')]

    df = pd.DataFrame(parsed_str, index=None, columns=ctd_columns()[:-1])
    df = df.iloc[:-1, :]
    df['Timestamp'] = timestamp
    df.to_csv(filename, mode='a', header=False, index=False)
    write_latest(df)
    return df


def write_latest(df):
    filename = '/media/pi/DATA/CTD/latest_ctd.csv'
    df_latest = df[df['Timestamp'] == max(df['Timestamp'])]
    df_latest.to_csv(filename, mode='w', index=False)


def ctd_columns():
    return ['Temperature','Conductivity','Pressure','Volt0','Volt1','Volt2','Volt4',
            'Volt5','SBE63','SBE63Temperature','Salinity','Timestamp']


def collect_samples(ser, n_samples=30, sample_frequency=4):
    c = 0
    print('Start logging')
    start_ctd(ser)
    time.sleep(1)
    df = pd.DataFrame(columns=ctd_columns())
    filename = '/media/pi/DATA/CTD/ctd_' + pd.datetime.now().strftime('%Y%m%d%H%M%S') + '.csv'
    print('CTD fileanme: ' + filename)
    
    starttime = pd.datetime.now()
    empty_line_count = 0
    timeout_seconds_during_collection = 10 # seconds

    df.to_csv(filename, mode='w', index=False)
    while True:
        timestamp = pd.datetime.now()
        print(timestamp)
        lines = ser.readline()
        print(lines)

        if (pd.datetime.now() - starttime) > (pd.to_timedelta((n_samples / sample_frequency) + timeout_seconds_during_collection, 'seconds')):
            print('timeout on profile data! not enough samples received within the time period - there is likely a problem.')
            success = False
            break

        if len(lines) == 0:
            empty_line_count += 1
            print('empty_line_count', empty_line_count)
            time.sleep(0.1)

            if empty_line_count > (sample_frequency * timeout_seconds_during_collection):
                print('timeout on profile data! too many empty lines received from the instrument - there is likely a problem.')
                success = False
                break
            
            continue

        data_str = lines.strip(b'<Executed/>')
        if data_str.decode().startswith(' ') and not data_str.decode().startswith(' V'):
            try:
                string_to_df(filename, data_str, timestamp)
                empty_line_count = 0
            except:
                print('error in data_str')
                success = False
                break
                
            #df = df.append(string_to_df(data_str, timestamp))
            #print('DataFrame:')
            #print(df)

        c += 1
        if c > n_samples:
            print('enough samples')
            success = True
            break

    print('stop logging.')
    stop_ctd(ser)
    flush(ser)

    # rename csv files ready for offload
    files = glob.glob(os.path.join('/media/pi/DATA/CTD', 'ctd*.csv'))
    for f in files:
        path, filename = os.path.split(f)
        os.rename(f, os.path.join(path, 'ready_' + filename))
    #plot_ctd_fig(path, 'ready_' + filename)

    return success


def plot_ctd_fig(path, filename):

    data = pd.read_csv(os.path.join(path, filename))

    f, a = plt.subplots(2,2, figsize=(10,10))

    plt.sca(a[0,0])
    plt.plot(data.Pressure, '.', alpha=0.25)
    plt.xlabel('Sample #')
    plt.ylabel('Pressure')
    plt.gca().invert_yaxis()
    plt.title(str(data.Timestamp.min()) + ' - ' + str(data.Timestamp.max()),
            loc='left')

    plt.sca(a[0,1])
    plt.plot(data.Conductivity, data.Pressure, '.', alpha=0.25)
    plt.xlabel('Conductivity')
    plt.ylabel('Pressure')
    plt.gca().invert_yaxis()

    plt.sca(a[1,1])
    plt.plot(data.Temperature, data.Pressure, '.', alpha=0.25)
    plt.xlabel('Temperature')
    plt.ylabel('Pressure')
    plt.gca().invert_yaxis()

    plt.sca(a[1,0])
    plt.plot(data.Salinity, data.Pressure, '.', alpha=0.25)
    plt.xlabel('Salinity')
    plt.ylabel('Pressure')
    plt.gca().invert_yaxis()

    print('writing CTD_Latest.png ....')
    plt.savefig(os.path.join(path, 'CTD_Latest.png'), dpi=100, bbox_inches='tight', facecolor='white', transparent=False)
    print('  CTD_Latest.png updated')


def str_to_unicode(string):
    return (re.sub('.', lambda x: r'\u % 04X' % ord(x.group()), string))


def ctd_profile(sample_duration_minutes=25, sample_frequency=4):
    sample_duration = sample_duration_minutes * 60
    n_samples = sample_frequency * sample_duration

    ser = serial.Serial(CTD_COM, 9600, timeout=1)
    ser.flushInput() #flush serial port input

    stop_ctd(ser)
    wake_ctd(ser)
    #configure_outputs(ser)
    #ctd_status(ser)
    flush(ser)
    ctd_status(ser)
    success = collect_samples(ser, n_samples=n_samples, sample_frequency=sample_frequency)
    #get_header(ser)
    #download(ser)
    #flush(ser)
    ctd_sleep(ser)
    flush(ser)

    ser.close()
    print('CLOSED.')
    return success


if __name__ == '__main__':

    sample_duration_minutes = 25 

    starttime = pd.datetime.now()
    remaining_time = sample_duration_minutes
    while True:
        print('CTD profile', remaining_time)
        success = ctd_profile(sample_duration_minutes=remaining_time)
        print('success', success)

        if not success:
            stoptime = pd.datetime.now()
            used_time = stoptime - starttime
            remaining_time = (pd.to_timedelta(sample_duration_minutes, 'minutes') - used_time).total_seconds()/60.
            print('remaining_time', remaining_time)
            remaining_time = round(remaining_time, 2)
        
            if remaining_time < 0.5:
                print('remaining_time is less than 30 seconds. not restarting CTD.')
                break


# conversion notes:
# s = string.strip().replace(' ','').split(',')
# np.array(s, dtype=float)
