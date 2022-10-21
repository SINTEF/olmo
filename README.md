# OLMO
OceanLab Observatory, Automated Data Handling

# Data collection

## Munkholmen sensor data

Each sensor on munkholmen should have a class object associated with it.
See for example `adcp.py`. The `rsync_and_ingest()` method of this class should
be run every 2 mins via the `ingest_munkholmen.py` script.

In the init of this class the following variables will be used to rsync
and ingest the data. In cases where there is an `_L#`, there can be 4 different
versions of this variable for the 4 data quality levels.

 * `data_dir`: Directory on the munkholmen raspberry pi where data files are found
 * `file_search_l#`: Regex used to match files in `data_dir` (or list of regex's). This is the main
 controller for the 'level', setting this to None will mean the level is ignored in rsync/ingest.
 * `drop_recent_files_l#`: Number of latest files to ignore (in case they are being written to)
 * `remove_remote_files_l#`
 * `max_files_l#`: Max number in any one running of the cronjob.
 * `measurement_name_l#`: Measurement (table) name in influx

## Munkholmen operation (LoggerNet) data

The files are transferred to the LoggerNet pc over loggernet. See `Loggernet Windows machine`.
From there we have a cronjob that runs `ingest_loggernet.py`. This transfers over all files but the latest one, and ingests them into influx. For more info see the file `ingest_loggernet.py` and the function `sensor_conversions.ingest_loggernet_file()`.


# Uploading custom data

Currently we simply support this through uploading directories. So if you have a single file to be linked in with the data, just put it in a directory.

This works by filling in all the necessary fields of metadata (and folder location) before running the python file to upload the data.

Note that the python file must be run from a computer which has the "az" command line tool installed, and there needs to be a file called `azure_token_datalake` with a valid access token in the directory above this repo. See Torfinn2 for an example.

To generate the access token:

 * go to `portal.azure.com` and navigate to our container.

 * Click on Shared access tokens and create one.

 * Under Allowed IP addresses p the IP of the computer you are on. This can be found with: `curl api.ipify.org`

Note that the current access token on Torfinn2 expires at the start of 2023.


# Loggernet Windows machine:

This has been installed under user Loggernet_user on the machine sintefutv012. Contact William if you need to access this user.

On the machine we have installed OpenSSH-Win64. This needs to be started up if it stops running. You can do this via:

1. Open a command prompt as administrator.
2. `Start-Service sshd`

Note that I also set `Set-Service -Name sshd -StartupType 'Automatic'`, which I hope will start this on start up, but this is yet to be tested.

I have now added LoggerNet to the TaskScheduler, with the trigger that it starts on startup of the machine.


# Getting started with notebooks

Step 1: Follow steps 1 to 3 of 'Getting started':

  `conda env install -f environment.yml`

  `conda activate olmo`

  `python setup.py develop`

Step 2 (optional): We also installed some helpful extensions to notebooks, but this needs to be activated within 'jupyter':

  `jupyter contrib nbextension install --user`

Step 3: Finally start the notebook server (`jupyter notebook`). This will open up a page in your brower with the files in this repo.

Notebooks are found in the `Notebooks` folder. You will also note there is a tab at the top called `Nbextensions`. I like to click on that and enable `Table of Contents (2)`.


# Front end

We have implemented a grafana front end, and have some data being displayed on the website. These are not currently open resources.


# Development

To develop the code, we generally test into a newly created DB. Running python files from your 'personal' user on the controller PC.

Files on the remote computers should not be deleted until testing has verified that the workflow works correctly. This can be done using the variable `drop_recent_files_lX`, by setting this to false.
