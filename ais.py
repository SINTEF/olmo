import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
import re

import sensor
import config
import util
import ingest

logger = util.init_logger(config.main_logfile, name='olmo.ctd')


def get_ais_df(mmsi):

    url = 'https://www.myshiptracking.com/vessels/' + str(mmsi)

    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}

    req = urllib.request.Request(url, None, hdr)
    with urllib.request.urlopen(req) as response:
        the_page = response.read()
    soup = BeautifulSoup(the_page, features="lxml")

    df = pd.DataFrame()

    text = str(soup.find(text=re.compile('Longitude')).findNext())
    df['longitude'] = [float(text[4:-6])]

    text = str(soup.find(text=re.compile('Latitude')).findNext())
    df['latitude'] = [float(text[4:-6])]

    text = str(soup.find(text=re.compile('as reported on')).findNext())
    df['timestamp'] = [pd.to_datetime(text[8:-9])]

    return df


class AIS(sensor.Sensor):
    '''Class for rsyncing and ingesting the munkholmen ctd data.'''
    def __init__(
            self,
            influx_clients=None):

        # Init the Sensor() class: Unused vars/levels are set to None.
        super(AIS, self).__init__()
        self.influx_clients = influx_clients
        self.mmsi_list = {'munkholmen': 992581014,
                          'ingdalen': 992581017,
                          'gunnerus': 258342000,
                          'ntnuflyer': 257012170,
                          'ntnu_usv_grethe': 258006650
                          }

    def ingest_l0(self):

        time_col = 'timestamp'

        tag_values = {'tag_sensor': 'ais_web',
                      'tag_edge_device': 'https://www.myshiptracking.com',
                      'tag_data_level': 'raw',
                      'tag_approved': 'no',
                      'tag_unit': 'degrees'}

        # ------------------------------------------------------------ #
        # loop through mmsi_list and ingest to respective measurement tables
        for m in self.mmsi_list:
            df = get_ais_df(self.mmsi_list[m])
            measurement_name = 'ais_web_' + m
            field_keys = {"longitude": 'longitude',
                          "latitude": 'latitude',
                          }
            tag_values['tag_platform'] = m
            df = util.force_float_cols(df, not_float_cols=[time_col], error_to_nan=True)
            df[time_col] = pd.to_datetime(df[time_col], format='%Y-%m-%d %H:%M:%S')
            df = df.set_index(time_col).tz_localize('UTC', ambiguous='infer').tz_convert('UTC')
            df = util.filter_and_tag_df(df, field_keys, tag_values)
            ingest.ingest_df(measurement_name, df, self.influx_clients)

        logger.info('AIS web data ingested.')
