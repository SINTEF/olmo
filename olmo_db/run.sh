sudo systemctl enable --now influxdb
sudo service influxdb start

# sudo service influxdb status

source ~/anaconda3/etc/profile.d/conda.sh
conda activate olmo_db

python olmo_db.py

sudo systemctl start grafana-server