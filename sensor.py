import os
import logging
import re

import util_file
import config

logger = logging.getLogger('olmo.sensor')


class Sensor:
    '''Base sensor class that non-loggernet sensors should inherit from.'''
    def __init__(
            self,
            # ---- Rsync params:
            data_dir=None,
            recursive_file_search_l0=False,
            recursive_file_search_l1=False,
            recursive_file_search_l2=False,
            recursive_file_search_l3=False,
            file_search_l0=None,
            file_search_l1=None,
            file_search_l2=None,
            file_search_l3=None,
            drop_recent_files_l0=1,
            drop_recent_files_l1=1,
            drop_recent_files_l2=1,
            drop_recent_files_l3=1,
            remove_remote_files_l0=True,
            remove_remote_files_l1=True,
            remove_remote_files_l2=True,
            remove_remote_files_l3=True,
            max_files_l0=None,
            max_files_l1=None,
            max_files_l2=None,
            max_files_l3=None,
            # ---- Ingest params:
            measurement_name_l0=None,
            measurement_name_l1=None,
            measurement_name_l2=None,
            measurement_name_l3=None,):

        self.data_dir = data_dir
        self.recursive_file_search_l0 = recursive_file_search_l0
        self.recursive_file_search_l1 = recursive_file_search_l1
        self.recursive_file_search_l2 = recursive_file_search_l2
        self.recursive_file_search_l3 = recursive_file_search_l3
        self.file_search_l0 = file_search_l0
        self.file_search_l1 = file_search_l1
        self.file_search_l2 = file_search_l2
        self.file_search_l3 = file_search_l3
        self.drop_recent_files_l0 = drop_recent_files_l0
        self.drop_recent_files_l1 = drop_recent_files_l1
        self.drop_recent_files_l2 = drop_recent_files_l2
        self.drop_recent_files_l3 = drop_recent_files_l3
        self.remove_remote_files_l0 = remove_remote_files_l0
        self.remove_remote_files_l1 = remove_remote_files_l1
        self.remove_remote_files_l2 = remove_remote_files_l2
        self.remove_remote_files_l3 = remove_remote_files_l3
        self.max_files_l0 = max_files_l0
        self.max_files_l1 = max_files_l1
        self.max_files_l2 = max_files_l2
        self.max_files_l3 = max_files_l3
        self.measurement_name_l0 = measurement_name_l0
        self.measurement_name_l1 = measurement_name_l1
        self.measurement_name_l2 = measurement_name_l2
        self.measurement_name_l3 = measurement_name_l3

    def fetch_files_list(self, file_regex, recursive_file_search, drop_recent_files):
        '''Use regex to find all files up to self.drop_recent_files

        Will match all files from the remote data_dir using the file_regex
        pattern, and dropping the final (most recent) drop_recent_files files.

        Note that currently the most recent files are simpy those listed
        last in the ls_string

        Parameters
        ----------
        file_regex : str
        recursive_file_search : bool
            If the file_regex is to be interpreted as the input to a linux 'find' query, not regex
        drop_recent_files : int
            Number of latest files to ignore.

        Returns
        -------
        list
        '''

        if (self.data_dir is None) or (file_regex is None):
            raise ValueError("fetch_files_list() requires 'data_dir' and 'file_regex' are set.")

        # TODO: Need to handle the ls_err (below in both statements) in some way.
        if recursive_file_search:
            ls_out, ls_err = util_file.find_remote(
                config.munkholmen_user, config.munkholmen_pc,
                self.data_dir, file_regex, port=config.munkholmen_ssh_port)
            # Using find_remote() it should already filter. Thus just split up string:
            files = ls_out.split('\n')
            if files[-1] == '':
                files = files[:-1]
            # Remove the 'self.data_dir' from the file path, so consistet with below
            for i, f in enumerate(files):
                files[i] = f[len(self.data_dir) + 1:]
        else:
            ls_out, ls_err = util_file.ls_remote(
                config.munkholmen_user, config.munkholmen_pc,
                self.data_dir, port=config.munkholmen_ssh_port)
            files = []
            while True:
                match = re.search(file_regex, ls_out)
                if match is None:
                    break
                else:
                    files.append(match.group())
                    ls_out = ls_out[match.span()[1]:]

        if len(files) <= drop_recent_files:
            logger.info(f"No new files found matching regex pattern: {file_regex}")
            return None
        elif drop_recent_files == 0:
            return files
        else:
            return files[:-drop_recent_files]

    def rsync(self):
        '''rsync's files from munkholmen to the controller PC.

        Parameters
        ----------
        self.remove_remote_files : bool
            If rsynced files are deleted using '--remove-source-files' flag
        self.max_files : int
            If not None: maximum number of files to transfer
        '''

        def rsync_file_level(files, remove_remote_files, max_files):

            if files is None:
                return

            if max_files is not None:
                if max_files >= len(files):
                    logger.warning(f"max_files {max_files} >= len(files), rsyncing all appropriate files")
                else:
                    files = files[:max_files]

            if remove_remote_files:
                remove_flag = ' --remove-source-files'
            else:
                remove_flag = ''

            rsynced_files = []
            for f in files:
                rsync_path = f"{config.munkholmen_user}@{config.munkholmen_pc}:{os.path.join(self.data_dir, f)}"
                exit_code = os.system(f'rsync -a{remove_flag} --rsh="ssh -p {config.munkholmen_ssh_port}" {rsync_path} {config.rsync_inbox_adcp}')
                if exit_code != 0:
                    logger.error(f"Rsync for file {f} didn't work, output sent to stdout, (probably the log from the cronjob).")
                    return rsynced_files
                else:
                    logger.info(f"rsync'ed file: {os.path.join(config.rsync_inbox_adcp, os.path.basename(f))}")
                    rsynced_files.append(os.path.join(config.rsync_inbox_adcp, os.path.basename(f)))

            return rsynced_files

        def fetch_and_sync(file_regex, recursive_file_search, drop_recent_files, remove_remote_files, max_files):
            files = self.fetch_files_list(file_regex, recursive_file_search, drop_recent_files)
            files = rsync_file_level(files, remove_remote_files, max_files)
            return files

        rsynced_files = {'l0': None, 'l1': None, 'l2': None, 'l3': None}
        if isinstance(self.file_search_l0, str):
            rsynced_files['l0'] = fetch_and_sync(
                self.file_search_l0, self.recursive_file_search_l0, self.drop_recent_files_l0,
                self.remove_remote_files_l0, self.max_files_l0)
        if isinstance(self.file_search_l1, str):
            rsynced_files['l1'] = fetch_and_sync(
                self.file_search_l1, self.recursive_file_search_l1, self.drop_recent_files_l1,
                self.remove_remote_files_l1, self.max_files_l1)
        if isinstance(self.file_search_l2, str):
            rsynced_files['l2'] = fetch_and_sync(
                self.file_search_l2, self.recursive_file_search_l2, self.drop_recent_files_l2,
                self.remove_remote_files_l2, self.max_files_l2)
        if isinstance(self.file_search_l3, str):
            rsynced_files['l3'] = fetch_and_sync(
                self.file_search_l3, self.recursive_file_search_l3, self.drop_recent_files_l3,
                self.remove_remote_files_l3, self.max_files_l3)
        return rsynced_files

    def get_influx_user(self, file=os.path.join(config.secrets_dir, 'influx_admin_credentials')):
        admin_user, _ = util_file.get_user_pwd(file)
        return admin_user

    def get_influx_pwd(self, file=os.path.join(config.secrets_dir, 'influx_admin_credentials')):
        _, admin_pwd = util_file.get_user_pwd(file)
        return admin_pwd

    def get_azure_token(self, file=os.path.join(config.secrets_dir, 'azure_token_datalake')):
        with open(file, 'r') as f:
            az_token = f.read()  # This token to run out end of 2021
        return az_token

    def ingest(self):
        raise NotImplementedError("This method should be implemented in sensor subclass.")

    def rsync_and_ingest(self):
        raise NotImplementedError("This method should be implemented in sensor subclass.")
