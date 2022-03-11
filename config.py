import os
import json


# General:
if (base_dir := os.getenv("OLMO_BASE_DIRECTORY")) is None:
    print("Warning. Env var OLMO_BASE_DIRECTORY not set correctly.")

output_dir = os.path.join(base_dir, 'Output')
secrets_dir = os.path.join(base_dir, 'Secrets')

with open(os.path.join(secrets_dir, 'config_secrets.json')) as f:
    data = json.load(f)
loggernet_pc = data['loggernet_pc']
loggernet_user = data['loggernet_user']
munkholmen_pc = data['munkholmen_pc']
munkholmen_ssh_port = int(data['munkholmen_ssh_port'])
munkholmen_user = data['munkholmen_user']
sintef_influx_pc = data['sintef_influx_pc']
az_influx_pc = data['az_influx_pc']


# Sync munkholmen (main):
rsync_inbox = os.path.join(base_dir, 'Rsync_inbox')
rsync_inbox_adcp = os.path.join(base_dir, 'Rsync_inbox_adcp')
main_logfile = "log_munkholmen_ingest_"


# Sync loggernet:
loggernet_outbox = f"c:\\Users\{loggernet_user}\LoggerNet_output"
loggernet_inbox = os.path.join(base_dir, 'Loggernet_inbox')
loggernet_files_basenames = [
    "CR6_EOL2,0_meteo_ais_",
    # "CR6_EOL2p0_Power_",
    # "CR6_EOL2,0_Meteo_avgd_",
    # "CR6_EOL2p0_Current_",
    # "CR6_EOL2,0_Wave_sensor_",
    # "CR6_EOL2p0_Winch_log_",
]
loggernet_logfile = "log_loggernet_ingest_"
logpc_ssh_max_attempts = 3


# Backup files:
backup_dir = os.path.join(base_dir, 'backups')
bu_logfile_basename = "log_influx_backup_"
backup_basename = 'influxbackup_test_'
az_backups_folder = 'influx_backups_test'


# Website figures
webfigs_dir = os.path.join(output_dir, 'Website_figures')


# Silcam ingestion:


# Custom data ingestion:
custom_data_folder = "Custom_Data"
