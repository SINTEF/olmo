import serial
import serial.tools.list_ports
import sys
import glob
import pandas as pd

UVP_COM = 'COM3'


def list_com_ports():
    try:
        if sys.platform.startswith('win'):
            com_list = [comport.device for comport in serial.tools.list_ports.comports()]
        elif sys.platform.startswith('linux'):
            com_list = glob.glob('/dev/tty[A-Za-z]*')
    except AttributeError:
        com_list = []

    return com_list


def flush(ser=None):
    if ser is None:
        ser = serial.Serial(UVP_COM, 38400, timeout=1)
    lines = ser.readlines()
    # while len(lines) != 0:
    while True:
        lines = ser.readlines()
        for l in lines:
            uvp_string = l.decode()
            parse_uvp_string(uvp_string)
    # ser.close()


def parse_uvp_string(uvp_string):
    print('parse_uvp_string')
    uvp_string = uvp_string.replace(';', '')
    uvp_string = [x.split(',') for x in uvp_string.split('\n')]
    df = pd.DataFrame(uvp_string, index=None)
    df = df.iloc[:-1, :]
    data_type = df.loc[0, 0]
    if data_type == 'BLACK_DATA':
        print('BLACK_DATA')
        pass_black_data(df)
    elif data_type == 'LPM_DATA':
        print('LPM_DATA')
        pass_lpm_data(df)
    else:
        print('unexpected data!')
    pass


def pass_lpm_data(df_lpm):
    lpm_columns = ['LPM_DATA', 'Depth', 'Date', 'Time', 'Number of analysed images',
                   'Internal temperature'
                   ]
    n_classes = 18
    for n in range(n_classes):
        new_string = 'Cumulated number of objects for class ' + str(n + 1)
        lpm_columns.append(new_string)
    for n in range(n_classes):
        new_string = 'Mean grey level of objects from class ' + str(n + 1)
        lpm_columns.append(new_string)
    df = pd.DataFrame(columns=lpm_columns, data=df_lpm.values)
    print('LPM')
    df.to_csv('lpm.csv', index=False, mode='a', header=False)
    pass


def pass_black_data(df_black):
    black_columns = ['LPM_DATA', 'Depth', 'Date', 'Time', 'Number of analysed images',
                     'Internal temperature']
    n_classes = 18
    for n in range(n_classes):
        new_string = 'Cumulated number of objects for class ' + str(n + 1)
        black_columns.append(new_string)
    df = pd.DataFrame(columns=black_columns, data=df_black.values)
    print('BLACK')
    df.to_csv('black.csv', index=False, mode='a', header=False)
    pass


if __name__ == '__main__':
    print('connected com ports:')
    print(list_com_ports())
    print('')
    print('UVP_COM:', UVP_COM)
    print('')

    flush()
