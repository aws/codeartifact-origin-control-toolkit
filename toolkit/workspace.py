# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import os.path
import shutil
import csv

from toolkit.utils import CSV_HEADER, generate_hash


class WorkspaceManager:
    TODO_FOLDER_NAME = 'todo'
    ERROR_FOLDER_NAME = 'error'
    DONE_FOLDER_NAME = 'done'
    FILE_NAME_PATTERN = '{domain}-{repository}-{format}-{namespace}-{package}'
    WORKSPACES_HOME = "workspaces"
    workspace_dir = None

    def __init__(self, input_file_path, workspaces_home=None):
        print("Initializing workspace from", input_file_path)
        self.input_file_path = os.path.expanduser(input_file_path)
        self.content_hash = generate_hash(self.input_file_path)
        # we support relative paths - expand every time into absolute
        self.workspaces_home = os.path.expanduser(workspaces_home if workspaces_home else self.WORKSPACES_HOME)
        self.todo_folder, self.error_folder, self.done_folder = self.setup_workspace()
        self.create_input_files()
        print("Workspace initialized at", self.workspace_dir)

    def setup_workspace(self):
        self.workspace_dir = os.path.join(self.workspaces_home, self.content_hash)
        todo_folder = os.path.join(self.workspace_dir, self.TODO_FOLDER_NAME)
        error_folder = os.path.join(self.workspace_dir, self.ERROR_FOLDER_NAME)
        done_folder = os.path.join(self.workspace_dir, self.DONE_FOLDER_NAME)
        try:
            os.mkdir(self.workspaces_home)
        except FileExistsError:
            pass
        try:
            os.mkdir(self.workspace_dir)
            os.mkdir(todo_folder)
            os.mkdir(error_folder)
            os.mkdir(done_folder)
        except FileExistsError:
            pass
        return todo_folder, error_folder, done_folder

    def is_workspace_clean(self):
        if os.listdir(self.done_folder) != []\
                and os.listdir(self.todo_folder) == [] \
                and os.listdir(self.error_folder) == []:
            print("All items already processed correctly and no error found.")
            return True

    def should_recreate_files(self):
        if self.is_workspace_clean():
            return False
        if os.listdir(self.error_folder) != []:
            print("Error items are present, skipping file parsing.")
            return False
        if os.listdir(self.todo_folder) != []:
            print("To do items are present, skipping file parsing.")
            return False
        return True

    def create_input_files(self):
        if not self.should_recreate_files():
            return

        with open(self.input_file_path, "r") as f:
            reader = csv.DictReader(f)
            for line in reader:
                target_filename = self.FILE_NAME_PATTERN.format(domain=line['domain'],
                                                                repository=line['repository'],
                                                                format=line['format'],
                                                                namespace=line['namespace'],
                                                                package=line['package'])
                target_filepath = os.path.join(self.todo_folder, target_filename)
                with open(target_filepath, 'w+') as target_file:
                    writer = csv.DictWriter(target_file, fieldnames=CSV_HEADER)
                    writer.writerow(line)

    def delete_workspace(self, force=False):
        if not force:
            if not self.is_workspace_clean():
                return
        shutil.rmtree(self.workspace_dir)

    def get_fully_qualified_filename(self, filename):
        return os.path.expanduser(os.path.join(self.todo_folder, filename))

    '''
    Callback to transition task state to done. Whatever code is running the task calls this function
    at the end of processing.
    '''
    def move_to_done(self, filename):
        source = os.path.join(self.todo_folder, filename)
        if not os.path.exists(source):
            source = os.path.join(self.error_folder, filename)
        shutil.move(source, self.done_folder)

    '''
    Callback to transition task state to error.
    '''
    def move_to_error(self, filename):
        source = os.path.join(self.todo_folder, filename)
        destination = os.path.join(self.error_folder, filename)
        # if we aren't retrying an already failed task
        if not os.path.exists(destination):
            shutil.move(source, self.error_folder)

    '''
    Function used to move a task to the todo state
    '''
    def move_to_retry(self, filename):
        source = os.path.join(self.error_folder, filename)
        shutil.move(source, self.todo_folder)

    '''
    A function that returns the task's payload
    '''
    def reader(self, filename):
        source = os.path.join(self.todo_folder, filename)
        if not os.path.exists(source):
            source = os.path.join(self.error_folder, filename)
        with open(source, 'r') as target_file:
            reader = csv.DictReader(target_file, fieldnames=CSV_HEADER)
            return next(reader)

    '''
    An iterable function is to be consumed by a runner type of logic, where the workspace and whatever activity 
    the tasks represent interface with each other: for every item in the workspace (every file in the "todo" folder) 
    we yield a tuple consisting of the task id, callbacks to access its payload, and move its state to 
    either "done" or "error" depending on the result.  
    '''
    def get_tasks(self):
        for item in os.listdir(self.todo_folder):
            yield item, self.reader, self.move_to_done, self.move_to_error

    '''
    Same as above, but it yields tasks that have errored out instead (so they can be listed or retried)
    '''
    def get_failed_tasks(self):
        for item in os.listdir(self.error_folder):
            yield item, self.reader, self.move_to_done, self.move_to_error
