import pandas as pd
import influxdb_client

'''
Basic file to query data using the influxdb API
'''

USER, PASSWORD = 'iliad_waterquality', 'hXHCLX2UjjbgxrHZ'
DATABASE = 'example'


def query_influx_table(measurement, timespan, database, user, pwd, retpolicy='autogen'):

    bucket = f"{database}/{retpolicy}"
    url = "https://oceanlab.azure.sintef.no:8086"

    client = influxdb_client.InfluxDBClient(url=url, token=f'{user}:{pwd}')

    query = f'''from(bucket:"{bucket}")
 |> range(start: {timespan})
 |> filter(fn:(r) => r._measurement == "{measurement}")'''
    print(query)

    result = client.query_api().query(query=query)

    # It seems the 'table' inside the 'result' can either be a new variable, or just a
    # continuation of the same variable (maybe this relates to shard groups?). All very
    # confusing to William.
    all_data = []
    var_names = dict()
    counter = 0
    for table in result:
        for record in table.records:
            var = record.get_field()
            print('var', var)
            val = record.get_value()
            print('val', val)
            time = record.get_time()
            print('time', time)
            return
            if var not in var_names.keys():
                var_names[var] = counter
                all_data.append([])
                counter += 1
            all_data[var_names[var]].append((val, time))

    var_numbers = {v: k for k, v in var_names.items()}
    # all_data should be a list of lists of data. Each sublist will be the
    print(all_data[0])
    if len(all_data) == 1:
        df = pd.DataFrame(
            data=[x[0] for x in all_data[0]],
            index=[x[1] for x in all_data[0]],
            columns=[var_numbers[0]])
    return df.sort_index()


df = query_influx_table('silcam_total_volume_contentration_munkholmen', '2022-12-01T12:00:00Z, stop: 2022-12-07T12:00:00Z', DATABASE, USER, PASSWORD)

print(df)
