# https://grafana.com/docs/grafana/latest/installation/debian/

apt-get install -y apt-transport-https
apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key | apt-key add -

echo "deb https://packages.grafana.com/oss/deb stable main" | tee -a /etc/apt/sources.list.d/grafana.list

apt-get update
apt-get install grafana

# data sotred: /var/lib/grafana
# open server on localhost:3000


##
#Installs binary to /usr/sbin/grafana-server
#Installs Init.d script to /etc/init.d/grafana-server
#Creates default file (environment vars) to /etc/default/grafana-server
#Installs configuration file to /etc/grafana/grafana.ini
#Installs systemd service (if systemd is available) name grafana-server.service
#The default configuration sets the log file at /var/log/grafana/grafana.log
#The default configuration specifies a SQLite3 db at /var/lib/grafana/grafana.db
#Installs HTML/JS/CSS and other Grafana files at /usr/share/grafana

## BACKUP:
# use https://github.com/ysde/grafana-backup-tool
# see instructions under Configuration in the README
# also manually backup /var/lib/grafana/grafana.db as that didn't seem to get done by grafana-backup-tool
# 
# to backup grafana:
# - activate olmo_db environment
# - install https://github.com/ysde/grafana-backup-tool (pip install grafana-backup)
# - create API token with admin role in grafana: https://grafana.com/docs/grafana/latest/http_api/auth/
# - export the env variable for the generated grafana token: e.g.
#        export GRAFANA_TOKEN=PUT_YOUR_GRAFANA_TOKEN_HERE
# - run: grafana-backup save
# - copy the _OUTPUT_ folder to a backup location
# - manually copy /var/lib/grafana/grafana.db as this is not done properly by grafana-backup-tool
