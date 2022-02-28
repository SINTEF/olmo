# https://www.arubacloud.com/tutorial/how-to-install-influxdb-on-ubuntu-20-04.aspx
curl -sL https://repos.influxdata.com/influxdb.key | apt-key add -
echo "deb https://repos.influxdata.com/ubuntu bionic stable" | tee /etc/apt/sources.list.d/influxdb.list
echo "deb https://repos.influxdata.com/ubuntu bionic stable" | tee /etc/apt/sources.list.d/influxdb.list
apt update
apt install influxdb
systemctl status influxdb
systemctl enable --now influxdb

# When installed as a service, InfluxDB stores data in the following locations:
# Time series data: /var/lib/influxdb/engine/
# Key-value data: /var/lib/influxdb/influxd.bolt.
# influx CLI configurations: ~/.influxdbv2/configs (see influx config for more information) .

