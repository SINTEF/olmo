# Database contents, explanation and help

Note that in influxDB the following nomencalture is used:
 * `measurement` -> A sql table
 * `field value` -> An actual measurement value (almost always a float).
 * `tag value` -> A string 'tag' for a measurement value. The tags are included in the index.


## Explanation of tags and values (columns):

Value keys should be the measurement type, and probably will be repeated in the measurement name (e.g. `temperature`, `humidity`).

Tag keys are given below along with a short explanation.

```
| TAG name      | Explanation
| ------------- | ---------------------------------------
| time          | Timestamp in UTC
| approved      | 'yes' if passed a data quality filter, 'no' if not, 'none' if no filter applied
| data_level    | Descriptor of data handling e.g. 'raw', 'processed', 'modelled'
| edge_device   | Name/serial number of logger/computer handling the data on the edge
| platform      | Name of platform/location where date measured
| sensor        | Name/serial number of sensor
| unit          | The units of the measurement
```

## Database guidelines:

We follow the follow guidelines to make the data more standardised in how it is saved:

 * All names should only use the chars: `['a-z', '0-9' '_']`
 * Measurements:
   - Names should roughly follow `[measured variable]_[platform]`
   - Can have long names, as must be unique.
   - Avoid multiple measurements going into the same table (an expection would be something like lat/lon)
   - Derived quantities will be in a seperate table (as they have different tag, but same timestamp)
 * Tag keys to be included:
   - `sensor`, `edge_device`, `platform`, `data_level`, `approved`, `unit`
 * Field keys should be simple words to describe value, longer information should be in measurement name, or tags.
 * Field values should be floats and not integers.
