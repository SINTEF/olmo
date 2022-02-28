import serial
import serial.tools.list_ports
import sys
import time
import glob
import pandas as pd
import os
from datetime import datetime

import config


LISST_COM = '/dev/ttyUSB0'
logfile = os.path.join(config.base_dir, 'lisst_comms.log')


def getListPortCom():
    try:
        if sys.platform.startswith('win'):
            com_list = [comport.device for comport in serial.tools.list_ports.comports()]
        elif sys.platform.startswith('linux'):
            com_list = glob.glob('/dev/tty[A-Za-z]*')
    except AttributeError:
        com_list = []

    return com_list


def send_to_lisst(command_string, leave_open=False):
    ser = serial.Serial(LISST_COM, 9600, timeout=1)
    ser.write(command_string.encode()+ b"\n")
    time.sleep(0.1)
    if leave_open:
        return ser
    else:
        ser.close()


def gx():
    ser = send_to_lisst("GX", leave_open=True)
    read_lisst(ser=ser)


def send_str(txt):
    ser = send_to_lisst(str(txt), leave_open=True)
    read_lisst(ser=ser)


def offload_data(filename="L3531235.RBN"):
    #ser = send_to_lisst("YS 4 " + filename, leave_open=True)
    #flush(ser=ser)
    pass


def instrument_status():
    ser = send_to_lisst("ds", leave_open=True)
    read_lisst(ser=ser)
    ser = send_to_lisst("dd", leave_open=True)
    read_lisst(ser=ser)


def disk_directory():
    send_to_lisst("dd")
    read_lisst()


def delete_all_csv():
    send_break()
    filenames = get_csv_filenames()
    print(filenames)
    for filename in filenames:
        print('FILENAME:', filename)
        send_str('DL ' + filename)
        send_str('Y')
        flush()
        time.sleep(0.5)


def delete_all_rbn():
    send_break()
    filenames = get_rbn_filenames()
    print(filenames)
    for filename in filenames:
        print('FILENAME:', filename)
        send_str('DL ' + filename)
        send_str('Y')
        flush()
        time.sleep(0.5)


def download_csv():
    send_break()
    filenames = get_csv_filenames()
    print(filenames)
    for filename in filenames:
        print('FILENAME:', filename, 'downloading....')
        ser = send_to_lisst(("TYPE " + filename), leave_open=True)
        time.sleep(0.5)
        lines = ser.readlines()
        for l in lines:
            if len(l) < 20:
                continue

            parsed_str = l.decode()
            parsed_str = [x.split(',') for x in parsed_str.split('\r\n')]

            df = pd.DataFrame()
            df = pd.DataFrame(parsed_str, index=None)
            df = df.iloc[:-1, :]
            df.to_csv(os.path.join('DATA', filename), mode='a', header=False)
        print('FILENAME:', filename, ' Done.')

    ser.close()


def get_rbn_filenames():
    ser = send_to_lisst("dd", leave_open=True)
    time.sleep(0.5)
    lines = ser.readlines()
    ser.close()
    filenames = []
    for l in lines:
        parsed_str = l.decode()
        rbn_detect = parsed_str.find('.RBN')
        if rbn_detect != -1:
            filenames.append(parsed_str[0:rbn_detect] + '.RBN')
    return filenames


def get_csv_filenames():
    ser = send_to_lisst("dd", leave_open=True)
    time.sleep(0.5)
    lines = ser.readlines()
    ser.close()
    filenames = []
    for l in lines:
        parsed_str = l.decode()
        csv_detect = parsed_str.find('.CSV')
        if csv_detect != -1:
            filenames.append(parsed_str[0:csv_detect] + '.CSV')
    return filenames


def send_break():
    ser = serial.Serial(LISST_COM, 9600, timeout=1)
    command_string = "\x03"
    for i in range(5):
        ser.write(command_string.encode()+ b"\n")
        time.sleep(0.2)
    read_lisst(ser=ser)


def start_recodring():
    send_to_lisst("GO")
    read_lisst()


def log(txt):
    pass
    with open(logfile, 'a+') as f:
        f.write(str(txt) + '\n')


def read_lisst(ser=None):
    if ser is None:
        # open LISST_COM port with 9600 br and 1 second timeout
        ser = serial.Serial(LISST_COM, 9600, timeout=1)
    lines = ser.readlines()
    for l in lines:
        parsed_str = l.decode()
        print(parsed_str)
        log(parsed_str)
    ser.close()
    log(datetime.now())


def monitor():
    with serial.Serial(LISST_COM, 9600, timeout=1) as ser:
        while True:
            while ser.in_waiting > 0:
                line = ser.readline().decode('utf-8').rstrip()
                print(line)
            time.sleep(0.1)


def wait_for_sampling_complete():
    with serial.Serial(LISST_COM, 9600, timeout=1) as ser:
        while True:
            while ser.in_waiting > 0:
                line = ser.readline().decode('utf-8').rstrip()
                print(line)
                complete_detect = line.find('Sampling complete.')
                print('complete_detect:', complete_detect)
                if complete_detect != -1:
                    return
            time.sleep(0.1)


def flush(ser=None):
    if ser is None:
        ser = serial.Serial(LISST_COM, 9600, timeout=1)
    lines = ser.read()
    while len(lines) != 0:
        lines = ser.read(100)
        print(lines)
    ser.close()


def record_samples(nsamples, delay=1):
    send_break()
    #send_str('DL *.RBN')
    #send_str('dd')
    #flush()
    #sys.exit()

    ## CONFIGURATION ##
    sync_clock()
    send_str('SAVEDATA 1')
    send_str('Y')
    send_str('SAVEPSD 1')
    send_str('SENDDATA 0')
    send_str('ST 5') # delayed start
    send_str('TD ' + str(int(delay))) # start after 1 minute
    send_str('SP 5')
    send_str('PD ' + str(int(nsamples))) # number of samples
    instrument_status()
    send_str('GO')

    wait_for_sampling_complete()

    #start_recodring()
    #time.sleep(30)
    flush()
    #send_break()


def sync_clock():
    send_str('SC')
    timestr = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    send_str(timestr)
    send_str('DT')
    flush()


if __name__ == '__main__':
    print('connected com ports:')
    print(getListPortCom())
    print('')
    print('LISST_COM:', LISST_COM)
    print('')


    #monitor()
    #sys.exit()

    #delete_all_rbn()


    #delete_all_csv()
    #flush()
    #sys.exit()


