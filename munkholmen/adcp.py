import serial
import time
import os
import datetime

import config

datapath = 'DATA'

logfile = os.path.join(config.base_dir, 'adcp.log')


def log(txt):
    with open(logfile, 'a+') as f:
        f.write(str(txt) + '\n')


if __name__ == '__main__':
    ser = serial.Serial('/dev/ttyUSB1', 9600, timeout=1)
    ser.flush()

    while True:
        try:
                timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                filename = 'adcp_' + timestamp + '.dat'
                outputfile = os.path.join(datapath, filename)
                while ser.in_waiting > 0:
                    with open(outputfile, 'a+') as fh:
                        line = ser.readline().decode('utf-8').rstrip()
                        print(line)
                        fh.write(line + '\n')
                
                    time.sleep(0.1)

                time.sleep(0.1)
        except Exception as e:
            log('Error')
            log(e)
            time.sleep(10)
            continue
