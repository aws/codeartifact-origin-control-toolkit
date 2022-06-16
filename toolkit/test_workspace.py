# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import unittest
from unittest.mock import patch, mock_open, call
from toolkit.workspace import WorkspaceManager

SAMPLE_CSV = "domain,repository,namespace,format,package,upstream,publish\n" \
             "davide-test,nerv,,pypi,internetarchive,ALLOW,ALLOW"


class WorkspaceManagerTest(unittest.TestCase):
    def setUp(self):
        self.mock_file_path = "builtins.open"

    @unittest.mock.patch('os.mkdir')
    def test_empty_input(self, os_makedir):
        with patch('os.listdir') as mocked_listdir:
            mocked_listdir.return_value = []
            with patch(self.mock_file_path, mock_open(read_data="")) as f:
                WorkspaceManager(self.mock_file_path)

        expected_calls = [
            call('workspaces'),
            call('workspaces/d41d8cd98f00b204e9800998ecf8427e'),
            call('workspaces/d41d8cd98f00b204e9800998ecf8427e/todo'),
            call('workspaces/d41d8cd98f00b204e9800998ecf8427e/error'),
            call('workspaces/d41d8cd98f00b204e9800998ecf8427e/done')
        ]

        os_makedir.assert_has_calls(expected_calls)

    @unittest.mock.patch('os.mkdir')
    def test_files_are_created(self, os_makedir):
        with patch('os.listdir') as mocked_listdir:
            mocked_listdir.return_value = []
            with patch(self.mock_file_path, mock_open(read_data=SAMPLE_CSV)) as f:
                WorkspaceManager(self.mock_file_path)

        expected_mkdir_calls = [call('workspaces'),
            call('workspaces/ca1bfe01f702a9dd407620814dc0b475'),
            call('workspaces/ca1bfe01f702a9dd407620814dc0b475/todo'),
            call('workspaces/ca1bfe01f702a9dd407620814dc0b475/error'),
            call('workspaces/ca1bfe01f702a9dd407620814dc0b475/done')
        ]
        os_makedir.assert_has_calls(expected_mkdir_calls)

        f.assert_has_calls([
            call('builtins.open', 'r'),
            call().__enter__(),
            call().read(),
            call().__exit__(None, None, None),
            call('builtins.open', 'r'),
            call().__enter__(),
            call().__iter__(),
            call('workspaces/ca1bfe01f702a9dd407620814dc0b475/todo/davide-test-nerv-pypi--internetarchive', 'w+'),
            call().__enter__(),
            call().write('davide-test,nerv,pypi,,internetarchive,ALLOW,ALLOW\r\n'),
            call().__exit__(None, None, None),
            call().__exit__(None, None, None)
        ]

)
