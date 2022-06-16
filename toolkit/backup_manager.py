# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import csv
import os
from queue import Queue
from threading import Thread
from toolkit.utils import CSV_HEADER, generate_hash, get_user_confirmation

BACKUP_HOME = 'backups'
FILE_NAME_PATTERN = '{input_hash}.csv'


def get_backup_file(input_file_path):
    content_hash = generate_hash(input_file_path)
    return os.path.join(BACKUP_HOME, FILE_NAME_PATTERN.format(input_hash=content_hash))


class BackupManager:
    task_queue = Queue()
    target_file = None
    csv_writer = None
    writer_thread = None

    def __init__(self, input_file_path, ask_confirmation=False):
        self.input_file_path = input_file_path
        os.makedirs(BACKUP_HOME, exist_ok=True)
        self.target_filename = get_backup_file(input_file_path)
        print(f"Backup is going to be written to {self.target_filename}")
        if os.path.exists(self.target_filename) and ask_confirmation:
            if not self.ask_user_whether_to_overwrite_backup():
                raise Exception("User elected not to continue.")
        self.init_as_write()

    def ask_user_whether_to_overwrite_backup(self):
        print("A backup exists already for {}. "
              "Are you sure you want to continue? "
              "This will overwrite the existing backup."
              "If you want to restore this backup press N now"
              "and then call the script with the --restore option."
              "\nSelecting no will terminate the script."
              "\n"
              "Overwrite the existing backup? [Y/N]".format(self.input_file_path))
        if not get_user_confirmation():
            return False
        return True

    def init_as_write(self):
        self.target_file = open(self.target_filename, 'w+')
        self.csv_writer = csv.DictWriter(self.target_file, fieldnames=CSV_HEADER)
        self.writer_thread = Thread(target=self._writer_thread)
        self.writer_thread.setDaemon(True)
        self.writer_thread.start()
        print("Saving backup in {}".format(self.target_filename))

    def do_backup(self, package, origin_configuration):
        task = (package, origin_configuration)
        self.task_queue.put(task)

    def get_backup_file_for_input(self):
        return self.target_filename

    def _writer_thread(self):
        self.csv_writer.writeheader()
        while True:
            package, origin_configuration = self.task_queue.get()
            line = {
                'domain': package['domain'],
                'repository': package['repository'],
                'namespace': package['namespace'],
                'format': package['format'],
                'package': package['package'],
                'upstream': origin_configuration['upstream'],
                'publish': origin_configuration['publish']
            }
            self.csv_writer.writerow(line)
            self.target_file.flush()

