import logging
import datetime
import numpy as np
import pandas as pd

logger = logging.getLogger('olmo.util_db')


def ingest_df(measurement, df, clients):
    '''
    Ingest a df to a list of influxdb clients.
    df must have a specific form:
        1. Index must be timestamps, in UTC.
        2. Any cols that are 'tags' must have 'tag_' at the start of the
           column name, for ex. tag_sensor. The 'tag_' will be removed on
           uplaod.
        3. field value cols are simple those WITHOUT 'tag_'.

    Parameters
    ----------
    measurement : str
    df : pd.DataFrame
        See above for clarification
    clients : list
        Should be a list of influxdb.InfluxDBClient
    '''
    tag_cols = [c for c in df.columns if c[:4] == 'tag_']
    field_cols = [c for c in df.columns if c not in tag_cols]

    data = []
    for index, row in df.iterrows():
        data.append({
            'measurement': measurement,
            'time': index,
            'tags': {t[4:]: row[t] for t in tag_cols},
            'fields': {f: row[f] for f in field_cols},
        })

    for c in clients:
        c.write_points(data)


def force_float_cols(df, float_cols=None, not_float_cols=None, error_to_nan=False):
    '''
    Force cols of a df to be np.float64.

    If ingesting a df into influx where all values don't have decimal palces (e.g. a
    temperature value of '12') it will think they should have type int. This can cause
    that col to be given data type 'int' and then future values will all loose their
    decimal places.

    NOTE: We should almost NEVER use int type in influx.

    Parameters
    ----------
    df : pd.DataFrame
    float_cols : list or None
        List of columns to make force float type
    not_float_cols : list or None
        List of columns. Force float type for all columns except those in list.
    error_to_nan : bool
        If True any values that can't be converted to numeric, or are np.nan are
        filled with the value -7999

    Returns
    -------
    pd.DataFrame
    '''
    if float_cols is not None:
        assert not_float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col in float_cols:
                if error_to_nan:
                    df[col] = df[col].apply(pd.to_numeric, errors='coerce').fillna(-7999)
                else:
                    df[col] = df[col].astype(np.float64)
        return df
    elif not_float_cols is not None:
        assert float_cols is None, "Only one col list should be given"
        for col in df.columns:
            if col not in not_float_cols:
                if error_to_nan:
                    df[col] = df[col].apply(pd.to_numeric, errors='coerce').fillna(-7999)
                else:
                    df[col] = df[col].astype(np.float64)
        return df
    else:
        raise ValueError("'float_cols', or 'not_float_cols' should be a list of cols.")


def retag_tag_cols(df, tag_cols):
    '''
    Adds 'tag_' to name of tag cols.

    Parameters
    ----------
    df : pd.DataFrame
    tag_cols : str

    Returns
    -------
    pd.DataFrame
    '''
    rename = {c: f"tag_{c}" for c in tag_cols}
    return df.rename(columns=rename)


def add_tags(df, tag_values):
    '''
    Adds tags (name and value) to a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
    tag_cols : dict
        Should be key value pairs of tag names and tag values.
        It is assume all rows in the df have the same tag values.

    Returns
    -------
    pd.DataFrame
    '''
    for (k, v) in tag_values.items():
        df[k] = v
    return df


def filter_and_tag_df(df_all, field_keys, tag_values):
    '''
    Given a df with a large set of data returns a df with only the data
    from 'field_keys' and tagged with 'tag_values'

    Parameters
    ----------
    df_all : pd.DataFrame
    field_keys : dict
        key, value : col name in df_all, col name in output df
    tag_values : dict
        Should be key value pairs of tag names and tag values.
        It is assume all rows in the df have the same tag values.

    Returns
    -------
    pd.DataFrame
    '''
    df = df_all.loc[:, [k for k in field_keys.keys()]]
    df = df.rename(columns=field_keys)
    df = add_tags(df, tag_values)
    return df


def query_influxdb(client, measurement, variable, timeslice, downsample, approved='yes'):
    '''
    Querires influxDB measurement within some particular timeslice.

    Wrapper around the client.query() method to return a properly formatted
    df, with a few handy options (see paramters).

    TODO: I'm not sure that downsampling will work if given variable='*'

    Parameters
    ----------
    client : influxdb.InfluxDBClient
    measurement : str
    variable : str or list
        If '*' will return all
        If 'list' should be a list of variables (str)
        If str, should be exact variable name
    timeslice : str
        Example: 'time > now() - 60d'
        Example: "time > '2022-06-01T00:00:00Z' AND time < '2022-07-10T08:00:00Z'"
    downsample : bool or str
        If not False should be str of form: 'time(1m)'
    approved : str
        'all' or tag_approved to filter by

    Returns
    -------
    pd.DataFrame
    '''

    if approved == 'all':
        approved_text = ''
    else:
        approved_text = f'''AND "approved" = '{approved}' '''

    if variable == '*':
        variable_text = '*'
        df = pd.DataFrame(columns=['time'])
    elif isinstance(variable, list):
        variable_text = ", ".join(variable)
        df = pd.DataFrame(columns=variable.insert(0, 'time'))
    else:
        variable_text = f'"{variable}"'
        df = pd.DataFrame(columns=['time', variable])

    if downsample:
        q = f'''SELECT mean({variable_text}) AS "{variable}" FROM "{measurement}" WHERE {timeslice} {approved_text}GROUP BY {downsample}'''
    else:
        q = f'''SELECT {variable_text} FROM "{measurement}" WHERE {timeslice} {approved_text}'''

    result = client.query(q)
    for table in result:
        # Not sure this works if there are multiple tables.
        col_names = [k for k in table[0].keys()]
        col_vals = [[] for _ in col_names]
        for pt in table:
            for i, v in enumerate(pt.values()):
                col_vals[i].append(v)
        df = pd.DataFrame.from_dict({col_names[i]: col_vals[i] for i in range(len(col_names))})
    try:
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%dT%H:%M:%S.%fZ')
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('CET')
    return df


def get_field_keys(client, measurement):
    '''
    Querires influxDB measurement to get field keys AND field key types.

    Parameters
    ----------
    client : influxdb.InfluxDBClient
    measurement : str

    Returns
    -------
    list, list
    '''
    result = client.query(f"SHOW FIELD KEYS FROM {measurement}")
    results = []
    for r in result:
        results.append(r)
    assert len(results) == 1, "I believe this result should should always have len 1."
    field_keys = []
    field_key_types = []
    for dictionary in results[0]:
        field_keys.append(dictionary['fieldKey'])
        field_key_types.append(dictionary['fieldType'])
    return field_keys, field_key_types


def get_tag_keys(client, measurement):
    '''
    Querires influxDB measurement to get tag keys.
    Tags are always strings, hence it doesn't return the types.

    Parameters
    ----------
    client : influxdb.InfluxDBClient
    measurement : str

    Returns
    -------
    list
    '''
    result = client.query(f"SHOW TAG KEYS FROM {measurement}")
    results = []
    for r in result:
        results.append(r)
    assert len(results) == 1, "I believe this result should should always have len 1."
    tag_keys = []
    for dictionary in results[0]:
        tag_keys.append(dictionary['tagKey'])
    return tag_keys


def break_down_time_period(start_time, end_time):
    '''
    Breaks a time slice down into a list of time periods no longer than 1 day
    TODO: Eventually could add option to specify the length of the sub periods.

    Parameters
    ----------
    start_time : str
        Of the form: '2022-07-15T00:00:00Z'
    end_time : str
        Of the form: '2022-07-22T08:00:00Z'

    Returns
    -------
    list
        A list of touples. Each touple is a pair of 'start/stop' times
        in the same format as the expected input.
    '''

    start_time_ = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
    end_time_ = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
    total_time_delta = end_time_ - start_time_
    periods = []

    # Assuming breaking down into days:
    d = 0
    while d < total_time_delta.days:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            (start_time_ + datetime.timedelta(days=d + 1)).strftime('%Y-%m-%dT%H:%M:%SZ')))
        d += 1
    if total_time_delta.seconds > 0:
        periods.append((
            (start_time_ + datetime.timedelta(days=d)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_time_.strftime('%Y-%m-%dT%H:%M:%SZ')))

    return periods
