# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import csv
import sys
import os
import argparse
import threading
from collections import Counter
from functools import partial
from tqdm.contrib.concurrent import thread_map

from toolkit.backup_manager import BackupManager, get_backup_file
from toolkit.codeartifact_client import CodeArtifactClient
from toolkit.utils import CSV_HEADER, ALLOWED_FLAG_VALUES, get_user_confirmation
from toolkit.workspace import WorkspaceManager


def run_individual_task(codeartifact_client, task, backup_manager):
    task_id, reader, done_callback, error_callback = task
    res = None
    try:
        if args.ask_confirmation and not get_user_confirmation():
            return task_id, res, False
        if backup_manager:
            backup_manager.do_backup(reader(task_id), codeartifact_client.get_restrictions(**reader(task_id)))
        if not args.dry_run:
            res = codeartifact_client.apply_restrictions(**reader(task_id))
    except Exception as e:
        print(f"error! {e}")
        error_callback(task_id)
        return task_id, res, False

    done_callback(task_id)
    return task_id, res, True


def dispatch_work(file_length=None):
    workspace_path = get_backup_file(args.input) if args.restore else args.input
    workspace = WorkspaceManager(workspace_path)
    backup_manager = None if (args.no_backup or args.restore) else BackupManager(args.input, args.ask_confirmation)

    if args.delete_workspace:
        print(f"--delete-workspace was called. Deleting workspace for {args.input} and exiting.")
        workspace.delete_workspace(force=True)
        return
    if args.list_failed:
        for task in workspace.get_failed_tasks():
            task_id, reader, done_callback, error_callback = task
            print(reader(task_id))
        return

    codeartifact_client = CodeArtifactClient(profile=args.profile if args.profile else os.environ.get('AWS_PROFILE'),
                                             region_name=args.region,
                                             codeartifact_domain=args.domain)

    task_generator = workspace.get_failed_tasks if args.retry_failed else workspace.get_tasks
    processor_function = partial(dispatch_task, codeartifact_client, backup_manager)

    proc = thread_map(processor_function, task_generator(), max_workers=args.num_workers,
                      total=file_length, unit=' packages', desc='Applying origin control changes')

    print(f'Processed {len(proc)} tasks '
        f'({len(list(filter(lambda x: x, proc)))} successes, '
        f'{len(list(filter(lambda x: not x, proc)))} failures)')

    if not args.conserve_workspace:
        workspace.delete_workspace()


def dispatch_task(codeartifact_client, backup_manager, task):
    task_id, result, success = run_individual_task(codeartifact_client, task, backup_manager)
    if args.trace:
        print(f'{threading.get_ident()} | Coordinate {task_id} success: {success} ({result})')
    return success


def validate_file():
    with open(args.input, "r") as f:
        reader = csv.DictReader(f)
        validate_header(f, reader)
        for line in reader:
            validate_line(line, reader.line_num)
    print('File is valid and contains {} lines'.format(reader.line_num))
    return reader.line_num

def validate_header(f, reader):
    # Verify the header is present
    sniffer = csv.Sniffer()
    has_header = sniffer.has_header(f.read(2048))
    if not has_header:
        raise Exception("No header detected!")
    f.seek(0)
    # Verify that it has all necessary fields
    if Counter(reader.fieldnames) != Counter(CSV_HEADER):
        raise Exception("Check your header, some fields are missing!")


def validate_line(line, line_number):
    if line['domain'] != args.domain:
        raise Exception('[{}] Domain {} is different from expected value of {}'
                        .format(line_number, line['domain'], args.domain))
    if line['repository'] != args.repository:
        raise Exception('[{}] Repository {} is different from expected value of {}'
                        .format(line_number, line['repository'], args.repository))
    if line['upstream'] not in ALLOWED_FLAG_VALUES:
        raise Exception('[{}] \"upstream\" must be either ALLOW or BLOCK, cannot be {}'
                        .format(line_number, line['upstream']))
    if line['publish'] not in ALLOWED_FLAG_VALUES:
        raise Exception('[{}] \"publish\" must be either ALLOW or BLOCK, cannot be {}'
                        .format(line_number, line['upstream']))


def parse_args(input_args):
    parser = \
        argparse.ArgumentParser(description="This command takes in a ")
    parser.add_argument("--profile", help="AWS profile to be used (if environment variable not set).")
    parser.add_argument("--region", required=True, help="AWS region.")
    parser.add_argument("--domain", required=True, help="CodeArtifact domain.")
    parser.add_argument("--repository", required=True, help="Repository name.")
    parser.add_argument('--input', required=True,  help="Input CSV file.")
    parser.add_argument('--validate-only', action='store_true', help="Validate the input file and exit.")
    parser.add_argument('--conserve-workspace', action='store_true', help="Do not delete the workspace after"
                                                                          "terminating successfully.")
    parser.add_argument('--delete-workspace', action='store_true', help="Delete the workspace associated with "
                                                                        "the provided input file.")
    parser.add_argument('--list-failed', action='store_true', help="In case of failure, list packages that have failed"
                                                                   "to update the origin control configuration.")
    parser.add_argument('--retry-failed', action='store_true', help="Retry to set the origin control configuration"
                                                                    "for packages that have failed to do so.")
    parser.add_argument('--trace', action='store_true', help="Output information about every task.")
    parser.add_argument('--dry-run', action='store_true', help="Do not actually make calls to CodeArtifact.")
    parser.add_argument('--ask-confirmation', action='store_true', help="Require step-by-step confirmation for "
                                                                        "all write actions.")
    parser.add_argument('--no-backup', action='store_true', help="Do not make calls to CodeArtifact to back up "
                                                                 "the current origin control state.")
    parser.add_argument('--restore', action='store_true', help="If a valid backup file was created for the input"
                                                               "at hand, restore packages to that state.")
    parser.add_argument('--num-workers', type=int, default=4, help="Number of parallel threads making requests to CA.")
    parsed_args = parser.parse_args(input_args)
    return parsed_args


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    file_length = validate_file()
    if not args.validate_only:
        dispatch_work(file_length)
