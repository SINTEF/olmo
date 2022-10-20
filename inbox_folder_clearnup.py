import os
import time
import datetime

import config
import util_az

'''
Files rsynced/scp'ed onto the sintef network PC aren't cleaned up later.
This delete any files older than 'days_to_keep' that are in 'folders_to_clean'.
'''


def clean_inbox_folders(
        folders_to_clean=[config.loggernet_inbox, config.rsync_inbox, config.rsync_inbox_adcp],
        days_to_keep=7):
    '''
    Deletes all files in the folders 'folers_to_clean' oder than 'days_to_keep'.

    Parameters
    ----------
    folders_to_clean : list
    days_to_keep : int
    '''
    now = time.time()
    for folder in folders_to_clean:
        for f in os.listdir(folder):
            f = os.path.join(folder, f)
            if os.stat(f).st_mtime < now - days_to_keep * 86400:  # Using modification time.
                if os.path.isfile(f):
                    os.remove(f)


def clean_influx_backups(days=5, weeks=3, months=3):
    '''
    Deletes older backups to reduce storage space.
    There will be days + weeks + months files kept.
    Weeks aren't exact, just keeps 1st, 7th 14th and 21st of month.
    Months just keeps the 1st of the month.

    Example:
    On 16th of July with input days=4, weeks=2, months=3 it would keep:
        July 16, 15, 14, 13, 7, 1, June 1, May 1, April 1

    Parameters
    ----------
    days : int
    weeks : int
    months : int
    '''

    day = datetime.datetime.now()
    days_to_keep = [day.strftime('%Y%m%d')]
    counter = 0
    while True:
        day = day - datetime.timedelta(days=1)
        days_to_keep.append(day.strftime('%Y%m%d'))
        counter += 1
        if counter >= (days - 1):
            break
    counter = 0
    while True:
        day = day - datetime.timedelta(days=1)
        if day.strftime('%d') in ['01', '07', '14', '21']:
            days_to_keep.append(day.strftime('%Y%m%d'))
            counter += 1
        if counter >= weeks:
            break
    counter = 0
    while True:
        day = day - datetime.timedelta(days=1)
        if day.strftime('%d') == '01':
            days_to_keep.append(day.strftime('%Y%m%d'))
            counter += 1
        if counter >= months:
            break

    backups = util_az.container_ls('oceanlabdlcontainer', prefix=config.az_backups_folder, token_file='azure_token_dlcontainer')

    for f in backups:
        if f[len(config.backup_basename):len(config.backup_basename) + 8] not in days_to_keep:
            util_az.delete_file(
                config.az_backups_folder + '/' + f,
                'oceanlabdlcontainer', token_file='azure_token_dlcontainer')


if __name__ == "__main__":
    print("Starting running inbox_folder_clearnup.py at "
          + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    clean_inbox_folders()
    clean_influx_backups()
