import influxdb_client
import pandas as pd

'''
Basic file to query data using the influxdb API
For further query examples see here:
https://github.com/influxdata/influxdb-client-python/blob/master/examples/query.py
'''

USER, PASSWORD = 'username', 'password'
DATABASE = 'oceanlab'
RETPOLICY = 'autogen'
bucket = f"{DATABASE}/{RETPOLICY}"
url = "https://oceanlab.azure.sintef.no:8086"


def query_to_df(query):
    with influxdb_client.InfluxDBClient(url=url, token=f'{USER}:{PASSWORD}') as client:
        df = client.query_api().query_data_frame(query)
    return df


silcam = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start: 2022-12-01T12:00:00Z, stop: 2022-12-07T12:00:00Z)
        |> filter(fn:(r) => r._measurement == "silcam_total_volume_contentration_munkholmen")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> limit(n: 50)
    ''')
print('\n# ---------------------------- Specific time range')
print(silcam.columns)


#### get water current data from approx 10m depth ####

USER, PASSWORD = 'iliad_waterquality', 'hXHCLX2UjjbgxrHZ'
DATABASE = 'oceanlab'
RETPOLICY = 'autogen'
bucket = f"{DATABASE}/{RETPOLICY}"
url = "https://oceanlab.azure.sintef.no:8086"


current_speed = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start: 2022-12-01T12:00:00Z, stop: 2022-12-07T12:00:00Z)
        |> filter(fn:(r) => r._measurement == "signature_100_current_speed_munkholmen")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "_measurement", "platform", "current_speed_adcp(2)"])
        |> limit(n: 50)
    ''')
print('\n# ---------------------------- Specific time range')
print(current_speed.columns)


current_direction = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start: 2022-12-01T12:00:00Z, stop: 2022-12-07T12:00:00Z)
        |> filter(fn:(r) => r._measurement == "signature_100_current_direction_munkholmen")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "_measurement", "platform", "current_direction_adcp(2)"])
        |> limit(n: 50)
    ''')
print('\n# ---------------------------- Specific time range')
print(current_direction.columns)
