import os
import time

import config

'''
Files rsynced/scp'ed onto the sintef network PC aren't cleaned up later.
This delete any files older than 'days_to_keep' that are in 'folders_to_clean'.
'''

folders_to_clean = [
    config.loggernet_inbox,
    config.rsync_inbox,
    config.rsync_inbox_adcp,
]
days_to_keep = 7


def main():
    now = time.time()
    for folder in folders_to_clean:
        for f in os.listdir(folder):
            f = os.path.join(folder, f)
            if os.stat(f).st_mtime < now - days_to_keep * 86400:  # Using modification time.
                if os.path.isfile(f):
                    os.remove(f)


if __name__ == "__main__":
    main()
