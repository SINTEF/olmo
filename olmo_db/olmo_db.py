# https://github.com/influxdata/influxdb-python
import os
import time
from datetime import datetime
import numpy as np
from influxdb import InfluxDBClient

import config
import util_file


admin_user, admin_pwd = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))

client = InfluxDBClient('localhost', 8086, admin_user, admin_pwd, 'example')

while True:
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    json_body = [{
        "measurement": "cpu_load_short",
        "tags": {
            "host": "server01",
            "region": "us-west"
        },
        "time": current_time,
        "fields": {
            "value": np.random.rand(1)[0]
        }
    }]

    client.write_points(json_body)
    print(json_body)

    time.sleep(10)

print('--END--')
