'''
Import data from Oceanlab to a netCDF format and layout that can be read by OSCAR/DREAM.
'''
import os
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import influxdb_client
from dotenv import dotenv_values  # Used to import the user pwd. You can just set the
                                  # "token" (line 112) to 'your_username:your_password'.


def create_DataArray(values: np.ndarray, coordinates: dict, attributes: dict):
    return xr.DataArray(data=values, coords=coordinates, dims=coordinates.keys(), attrs=attributes)


def make_cf_attributes(std_name, units, coordinates):
    return {'standard_name': std_name,
            'units': units,
            'coordinates': coordinates}


def extendvar_latlon(var, nr_lat, nr_lon):
    '''
    Add spatial dimension to variable
    '''
    return np.tile(var[:, :, np.newaxis, np.newaxis], reps=(nr_lat, nr_lon))


def make_fates_netcdf_input_from_coordinate_timeseries(u_east, v_north, salinity, temperature, depth_levels, time,
                                                       lons=[0., 10., 20.], lats=[50., 60., 70.]):
    '''
    Extent a point profile time series to cover a certain horizontal region
    (3x3 grid). Target layout for the NODENS reader for WOA data.
    '''
    # Extend to cover nxm horizontal grid, default is 3x3
    nlon = len(lons)
    nlat = len(lats)

    # Create a DataArray for each variable then create a DataSet and save to netCDF for input to Fates
    data_vars = {}
    common_coordinates = {'MT': time, 'Depth': depth_levels, 'Latitude': lats, 'Longitude': lons}

    # U
    u_extended = extendvar_latlon(u_east, nlon, nlat)
    u_attrs = make_cf_attributes('eastward_sea_water_velocity', 'm/s', 'MT Depth Latitude Longitude lat lon')
    data_vars['u'] = create_DataArray(u_extended, common_coordinates, u_attrs)

    # V
    v_extended = extendvar_latlon(v_north, nlon, nlat)
    v_attrs = make_cf_attributes('northward_sea_water_velocity', 'm/s', 'MT Depth Latitude Longitude lat lon')
    data_vars['v'] = create_DataArray(v_extended, common_coordinates, v_attrs)

    # Temperature
    temp_extended = extendvar_latlon(temperature, nlon, nlat)
    temperature_attrs = make_cf_attributes('sea_water_potential_temperature', 'Celsius', 'MT Depth Latitude Longitude lat lon')
    data_vars['t_an'] = create_DataArray(temp_extended, common_coordinates, temperature_attrs)

    # Salinity
    sal_extended = extendvar_latlon(salinity, nlon, nlat)
    salinity_attrs = make_cf_attributes('sea_water_salinity', '1e-3', 'MT Depth Latitude Longitude lat lon')
    data_vars['s_an'] = create_DataArray(sal_extended, common_coordinates, salinity_attrs)

    # Create Dataset from DataArray
    ds = xr.Dataset(data_vars)

    ds.Longitude.attrs['standard_name'] = 'longitude'
    ds.Latitude.attrs['standard_name'] = 'latitude'
    ds.Depth.attrs['positive'] = 'down'
    ds.Depth.attrs['units'] = 'm'
    ds.MT.attrs['long_name'] = 'time'
    ds.MT.attrs['axis'] = 'T'

    return ds


def test():
    '''
    Create some dummy data, make a Dataset and store as netCDF.
    '''
    num_times = 20
    num_depths = 5

    # Create some datetimes, hourly time step
    times = pd.date_range(start='2022-06-01T00:00', freq='T', periods=num_times)

    # Depth range
    depths = np.linspace(0, 100, num_depths)

    # Random values for data
    u_east = np.random.random((num_times, num_depths))
    v_north = np.random.random((num_times, num_depths))
    salinity = np.random.uniform(34.0, 36.0, size=(num_times, num_depths))
    temperature = np.random.uniform(10., 20., size=(num_times, num_depths))

    # Create Fates-compatible dataset
    ds = make_fates_netcdf_input_from_coordinate_timeseries(u_east, v_north, salinity, temperature, depths, times)

    # Store as netcdf, ensure Fates-compatible data type for the time variable (MT, float32)
    encoding = dict(MT={'dtype': 'float32'})
    ds.to_netcdf('test_data_fates.nc')

    print(ds)
    return ds


def query_influx_table(measurement, timespan):

    secrets = dotenv_values(os.path.join('..', ".env"))
    bucket = "share_bistro/autogen"
    token = f"bistro:{secrets['INFLUX_PWD']}"
    url = "https://oceanlab.azure.sintef.no:8086"

    client = influxdb_client.InfluxDBClient(url=url, token=token)

    query = f'''from(bucket:"{bucket}")
 |> range(start:-{timespan})
 |> filter(fn:(r) => r._measurement == "{measurement}")'''

    result = client.query_api().query(query=query)

    n_time = len(result[0].records)
    n_vars = len(result)
    times = []
    var_names = []
    vals = np.zeros((n_time, n_vars))
    for table in result:
        var_names.append(table.records[0].get_field())
    for record in result[0]:
        times.append(record.get_time())
    for i, table in enumerate(result):
        for j, record in enumerate(table.records):
            vals[j, i] = record.get_value()

    return pd.DataFrame(data=vals, index=times, columns=var_names)


def get_oceanlab_testdata(time_range='240m'):
    '''
    Load and clean some actual oceanlab data
    '''

    # Load and filter current speed
    df_currspeed = query_influx_table('signature_100_current_speed_munkholmen', time_range)
    df_currspeed = df_currspeed.replace([46.34, -7999], np.nan)
    for c in df_currspeed.columns:
        if c != 'time':
            c.replace('current_speed_adcp', '').replace('(', '').replace(')', '')
    df_currspeed.columns = [int(c.replace('current_speed_adcp', '').replace('(', '').replace(')', ''))
                            for c in df_currspeed.columns]
    df_currspeed = df_currspeed.sort_index(axis=1)
    df_currspeed.index = pd.to_datetime(df_currspeed.index)

    # Load and filter current direction
    df_currdir = query_influx_table('signature_100_current_direction_munkholmen', time_range)
    df_currdir = df_currdir.replace([46.34, -7999], np.nan)
    df_currdir.columns = [int(c.replace('current_direction_adcp', '').replace('(', '').replace(')', ''))
                          for c in df_currdir.columns]
    df_currdir = df_currdir.sort_index(axis=1)
    df_currdir.index = pd.to_datetime(df_currdir.index)

    # Rename cols with depths:
    df_pressure = query_influx_table('signature_100_pressure_munkholmen', time_range)
    df_config = query_influx_table('signature_100_depth_config_munkholmen', time_range)

    # At each time step the position of the bins changes slightly, but we just take the average
    # pressure (i.e. the average position of the buoy over the period).
    offset = np.mean(df_pressure['pressure'])
    blanking = np.mean(df_config['blanking'])  # This should be a constant, I'll be lazy and take the mean
    cell_size = np.mean(df_config['cell_size'])  # This should be a constant, I'll be lazy and take the mean

    df_currspeed.columns = [offset + blanking + (cell_size / 2) + (c - 1) * cell_size
                            for c in df_currspeed.columns]
    df_currdir.columns = [offset + blanking + (cell_size / 2) + (c - 1) * cell_size
                          for c in df_currdir.columns]

    u_east = df_currspeed * np.sin(np.radians(df_currdir)).interpolate()
    v_north = df_currspeed * np.cos(np.radians(df_currdir)).interpolate()

    return u_east, v_north


def get_oceanlab_ctd(depth_bins_out, time_range='240m'):
    '''
    Get CTD data timeseries from Munkholmen buoy
    '''
    # Load CTD data fields for Munkholen
    df_temperature = query_influx_table('ctd_temperature_munkholmen', time_range)
    df_salinity = query_influx_table('ctd_salinity_munkholmen', time_range)
    df_pressure = query_influx_table('ctd_pressure_munkholmen', time_range)

    # Combine to CTD DataFrame
    df_ctd = pd.concat([df_temperature, df_salinity, df_pressure], axis=1)


    # Create histogram into regular depth bins
    depth_bins = np.linspace(0, df_ctd['pressure'].max(), 20)
    df_ctd['DepthBin'] = pd.cut(df_ctd['pressure'], depth_bins, labels=False)
    df_ctd_bins = df_ctd.groupby('DepthBin').mean().set_index(depth_bins[:-1])

    # Reindex to output bins, forward fill values
    df_ctd_bins = df_ctd_bins.reindex(depth_bins_out, method='ffill')

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(2 * 4, 8), sharey=True)
    df_ctd.plot(kind='scatter', y='pressure', x='temperature', ax=axes[0])
    axes[0].plot(df_ctd_bins['temperature'], df_ctd_bins.index, ls='--', c='darkred')
    df_ctd.plot(kind='scatter', y='pressure', x='salinity', ax=axes[1])
    axes[1].plot(df_ctd_bins['salinity'], df_ctd_bins.index, ls='--', c='darkred')
    axes[0].invert_yaxis()
    plt.savefig('ctd-data.png', dpi=150)
    plt.close(fig)

    df_ctd.plot(y=['temperature', 'salinity', 'pressure'],
                subplots=True, layout=(3, 1), figsize=(8, 3 * 4))
    plt.savefig('ctd-data-time.png', dpi=150)

    return df_ctd_bins


def netcdf_from_oceanlab(time_range='240m'):
    '''
    Create Fates-compatible netCDF from Oceanlab data (u, v, temperature, salinity)
    '''
    u_east, v_north = get_oceanlab_testdata(time_range=time_range)

    # Extract support variables
    depths = u_east.columns.values
    times = u_east.index.values
    num_times = times.size

    # Get CTD data binned into same depth bins as ADCP, copy values along time axis
    # In case of some issues with getting the CTD data (e.g. no profiling has
    # occured in the period), revert to creating some random dummy data for
    # temperature and salinity.
    # depth_bins = np.append(depths, [depths[-1]+np.diff(depths)[-1]])
    try:
        df_ctd_bins = get_oceanlab_ctd(depths, time_range=time_range)
        temperature = np.tile(df_ctd_bins['temperature'].values, reps=(num_times, 1))
        salinity = np.tile(df_ctd_bins['salinity'].values, reps=(num_times, 1))
    except:
        # Dummy temperature and salinity for now
        num_depths = depths.size
        salinity = np.random.uniform(34.0, 36.0, size=(num_times, num_depths))
        temperature = np.random.uniform(10., 20., size=(num_times, num_depths))

    # Create Fates-compatible dataset
    ds = make_fates_netcdf_input_from_coordinate_timeseries(u_east.values, v_north.values, salinity, temperature, depths, times)

    # Store as netcdf, ensure Fates-compatible data type for the time variable (MT, float32)
    encoding = dict(MT={'dtype': 'float32'})
    ds.to_netcdf('test_data_fates.nc', encoding=encoding)

    # Plot
    fig, axes = plt.subplots(4, 1, figsize=(8, 4 * 4), sharex=True)
    for ax, var in zip(axes, ds.data_vars):
        ds.isel(Longitude=0, Latitude=0)[var].plot(ax=ax, y='Depth', yincrease=False)
        ax.set_title(var)
        ax.set_xlabel('')
    plt.savefig('netcdf-data-plot.png', dpi=200, bbox_inches='tight')

    # Plot u and v and store figure
    fig, axes = plt.subplots(2, 1, figsize=(10, 2 * 4), sharex=True)
    ds.isel(Longitude=0, Latitude=0).u.plot.pcolormesh(ax=axes[0], x='MT', y='Depth', yincrease=False)
    ds.isel(Longitude=0, Latitude=0).v.plot.pcolormesh(ax=axes[1], x='MT', y='Depth', yincrease=False)
    plt.tight_layout()
    for ax in axes:
        ax.set_title('')
        ax.set_xlabel('')
    plt.savefig('current_u_v.png', dpi=150, bbox_inches='tight')

    print(ds)
    return ds


if __name__ == '__main__':

    # test()
    # get_oceanlab_testdata()
    # get_oceanlab_ctd(depth_bins_out=np.arange(0, 45, 5))
    netcdf_from_oceanlab(time_range='600m')
