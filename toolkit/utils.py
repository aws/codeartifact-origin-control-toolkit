# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import hashlib
from copy import copy

ALLOWED_FLAG_VALUES = ['ALLOW', 'BLOCK']
FLAG_NAMES = ['upstream', 'publish']
PACKAGE_TYPES = ['npm', 'pypi', 'maven', 'nuget']
CSV_HEADER = ['domain', 'repository', 'format', 'namespace', 'package', 'upstream', 'publish']


def parse_poc_flags(origin_configuration):
    """
    This function provides parsing and validation of origin control flag strings.
    """
    parsed_configuration = dict.fromkeys(FLAG_NAMES, None)
    yet_unparsed_flags = copy(FLAG_NAMES)

    split_string = origin_configuration.strip(" ").split(',')
    if len(split_string) != 2:
        raise Exception(f'Check number of arguments provided (should be 2)')

    for section in split_string:
        attribute_name, value = section.strip(" ").split('=')
        if attribute_name in yet_unparsed_flags:
            if value in ALLOWED_FLAG_VALUES:
                parsed_configuration[attribute_name] = value
                yet_unparsed_flags.remove(attribute_name)
            else:
                raise Exception(f'{value} not allowed for {attribute_name}')
        else:
            raise Exception(f'{attribute_name} not a valid origin control flag')

    return parsed_configuration


def generate_hash(input_file_path):
    with open(input_file_path, "r") as f:
        return hashlib.md5(f.read().encode()).hexdigest()


def get_user_confirmation():
    user_choice = input()
    if user_choice not in ['Y', 'y', 'N', 'n']:
        raise Exception(" {} is not a valid input.".format(user_choice))
    if user_choice in ['N', 'n']:
        return False
    return True
