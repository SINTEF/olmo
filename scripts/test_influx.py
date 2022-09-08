import os
import requests
from influxdb import InfluxDBClient

import config
import util_file

'''
Small file made when we wanted to check that we could access
the influxDB instance on the Azure VM over the internet.
'''


USER, PASSWORD = util_file.get_user_pwd(os.path.join(config.secrets_dir, 'influx_admin_credentials'))
USER = 'XXXX'
PASSWORD = 'XXXX'
HOST_IP = 'XXXX'

# ---- Check the endpoint health:
print("---- Check the endpoint health:")
r = requests.get(f"{HOST_IP}:8086/health")
# r = requests.get("https://www.yr.no/nb/v%C3%A6rvarsel/daglig-tabell/1-211102/Norge/Tr%C3%B8ndelag/Trondheim/Trondheim")

print(r.status_code)
print(r)

# client = InfluxDBClient(host=HOST_IP, port=8086, username=USER, password=PASSWORD)
client = InfluxDBClient(host=HOST_IP, port=8086, username=USER, password=PASSWORD)
print('client OK')

client.switch_database('example')
print("switched database")

res = client.get_list_database()
print('got databases', res)

results = client.query('SELECT "wind_speed_digital" FROM "loggernet_public"')
# print(results)
