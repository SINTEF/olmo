import influxdb_client

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


# Basic query, returning all columns:
# NOTE: 'result', 'table', '_start' and '_stop' are probably not useful to you.
timespan = '-2h'
measurement = 'meteo_temperature_munkholmen'
df = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start:{timespan})
        |> filter(fn:(r) => r._measurement == "{measurement}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    ''')
print('\n# ---------------------------- Basic query.')
print(f"Here is a full list of columns:\n{','.join(df.columns)}")
print(df.head())

# Filtered query, returning only a subset of columns
df = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start:{timespan})
        |> filter(fn:(r) => r._measurement == "{measurement}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "platform", "unit", "temperature"])
    ''')
print('\n# ---------------------------- Filtered query')
print(df.head())

# Query different table:
# NOTE: There are tables for the position of the sensors. Each table/sensor
# should have a platform, and each platform should have a table giving its position.
measurement = 'meteo_position_munkholmen'
df = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start:{timespan})
        |> filter(fn:(r) => r._measurement == "{measurement}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "platform", "latitude", "longitude", "unit"])
    ''')
print('\n# ---------------------------- Querying a different table.')
print(df.head())

# Query a specific time range (see changes in '|> range' row)
timespan = 'start: 2022-06-24T12:00:00Z, stop: 2022-06-24T14:00:00Z'
measurement = 'meteo_temperature_munkholmen'
df = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range({timespan})
        |> filter(fn:(r) => r._measurement == "{measurement}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "platform", "unit", "temperature"])
    ''')
print('\n# ---------------------------- Specific time range')
print(df.head())

# Interesting use of filters (this can be quite powerful)
# Here we find any tables with 'latitude' as a field name.
# NOTE: I also '|> keep' the measurement name, as that might be helpful.
# NOTE: Filtering by this field filters away other fields (but not tags).
timespan = '-2h'
df = query_to_df(f'''
    from(bucket:"{bucket}")
        |> range(start:{timespan})
        |> filter(fn:(r) => r._field == "latitude")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> keep(columns: ["_time", "_measurement", "platform", "latitude"])
    ''')
print('\n# ---------------------------- Filter based on _field, not _measurement.')
print(f"Unique measurement (table) names returned in this search:\n{df['_measurement'].unique()}")
print(df.head())
