# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import csv
import os
import sys
import argparse
from collections import defaultdict
from toolkit.codeartifact_client import CodeArtifactClient
from toolkit.utils import PACKAGE_TYPES, CSV_HEADER, parse_poc_flags

DEFAULT_RESTRICTIONS = {'upstream': 'ALLOW', 'publish': 'ALLOW'}
RESTRICTIONS_WITH_UPSTREAM_VERSIONS_BLOCKED = {'upstream': 'BLOCK', 'publish': 'ALLOW'}


def generate_package_configuration_entries(args, packages, origin_configuration):
    """
    This generator function takes in a list of packages and an origin configuration pattern, and returns a fully-formed
    package record by interpolating with args parameters.
    """
    for package in packages:
        line = {
            'domain': args.domain,
            'repository': args.repository,
            'namespace': args.namespace if args.namespace else '',
            'format': package['format'],
            'package': package['package'],
            'upstream': origin_configuration['upstream'],
            'publish': origin_configuration['publish']
        }
        yield line


def block_where_possible(packages_map, current_repo_ecs):
    """
    Decides whether or not acquiring new versions from upstreams can be safely restricted for each provided package
    in the target repository.
    We block acquisition of new versions from upstreams if and only if the target repository doesn't have direct access
    to an external connection, i.e., a public repository AND no versions of the package are available via any of
    the upstreams, either because the target repository doesn't have any upstreams or because none of the upstreams
    have the package. Falls back to allowing acquisition of new versions from the upstream by default.
    """
    packages_with_no_restrictions = []
    packages_with_upstream_versions_restricted = []

    for package_coordinate, upstreams in packages_map.items():
        package_format, package_namespace, package_name = package_coordinate
        package_entry = {'repository': args.repository,
                         'format': package_format,
                         'package': package_name,
                         'namespace': package_namespace}

        # First verify whether there is an immediate EC available for this package type in the current repository
        if next((external_connection for external_connection in current_repo_ecs
                 if external_connection.get('packageFormat') == package_format), False):
            packages_with_no_restrictions.append(package_entry)
            continue

        # If no EC, then let's look at upstreams: if the package is present in any of the upstreams, then we can't block
        if len(upstreams) != 0:
            packages_with_no_restrictions.append(package_entry)
            continue

        # Otherwise, we can BLOCK
        packages_with_upstream_versions_restricted.append(package_entry)
    return packages_with_no_restrictions, packages_with_upstream_versions_restricted


def get_candidate_package_list(codeartifact_client):
    """
    This function produces a suitable packages list (in the form {'name': package name, 'format': package format})
    from either the locally-supplied list or from a query against a repository.
    """
    if args.from_list:
        packages_from_list = []
        with open(args.from_list, "r") as input_file:
            reader = csv.reader(input_file)
            for package in reader:
                packages_from_list.append({'format': args.format, 'package': package[0]})
        return packages_from_list
    else:
        return codeartifact_client.list_packages_in_repository(repository=args.repository,
                                                               package_format=args.format,
                                                               package_namespace=args.namespace,
                                                               package_prefix=args.prefix)


def collect_repository_and_package_information(codeartifact_client):
    """
    This function gathers information about the current repository's package list, External Connections (ECs)
    and upstreams. If upstreams are present, it checks and records whether any target packages are present.
    """
    package_to_repositories_with_package_map = defaultdict(list)
    current_repo_upstream, current_repo_ecs = codeartifact_client\
        .get_repo_upstreams_and_external_connections(args.repository)
    upstream_graph = [] if current_repo_upstream is None else codeartifact_client.get_upstream_graph(args.repository)
    candidate_packages = get_candidate_package_list(codeartifact_client)

    for upstream in upstream_graph:
        packages_list = codeartifact_client.list_packages_in_repository(repository=upstream,
                                                                        package_format=args.format,
                                                                        package_namespace=args.namespace,
                                                                        package_prefix=args.prefix)
        for package in packages_list:
            if package in candidate_packages:
                package_to_repositories_with_package_map[
                    (package['format'], package.get('namespace'), package['package'])
                ].append(upstream)

    return package_to_repositories_with_package_map, current_repo_ecs


def generate_default_package_configuration_entries(codeartifact_client):
    """
    This function implements "automatic mode" and  is responsible for orchestrating the process of deciding
    what packages we can block the upstream configuration for, and for writing the result to the output file.
    """
    packages_map, current_repo_ecs = collect_repository_and_package_information(codeartifact_client)
    packages_with_no_restrictions, packages_with_upstream_versions_restricted = \
        block_where_possible(packages_map, current_repo_ecs)
    with open(args.output_file, 'w+') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_HEADER)
        writer.writeheader()
        for line in generate_package_configuration_entries(args, packages_with_no_restrictions,
                                                           DEFAULT_RESTRICTIONS):
            writer.writerow(line)
        for line in generate_package_configuration_entries(args, packages_with_upstream_versions_restricted,
                                                           RESTRICTIONS_WITH_UPSTREAM_VERSIONS_BLOCKED):
            writer.writerow(line)


def write_from_query(codeartifact_client):
    """
    This function implements "manual mode", where a set of query parameters for ListPackages has been
    supplied, as well as an explicit origin configuration to be applied to all of them. Here we simply
    execute the query and then write the desired origin configuration in the output file.
    """
    packages_list = codeartifact_client.list_packages_in_repository(repository=args.repository,
                                                                    package_format=args.format,
                                                                    package_namespace=args.namespace,
                                                                    package_prefix=args.prefix)
    iterator = generate_package_configuration_entries(args, packages_list, parse_poc_flags(args.configuration))
    with open(args.output_file, "w+") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_HEADER)
        writer.writeheader()
        for entry in iterator:
            writer.writerow(entry)


def generate_from_provided_list():
    """
    This function simply reads the user-supplied list and simply transforms it into a well-formed CSV
    without making any calls to AWS CodeArtifact, nor performing any verification. It expects the input
    to be well-formed as well.
    """
    origin_configuration = parse_poc_flags(args.configuration)
    with open(args.from_list, "r") as input_file:
        reader = csv.reader(input_file)
        with open(args.output_file, "w+") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=CSV_HEADER)
            writer.writeheader()
            for package in reader:
                package_entry = {
                    'domain': args.domain,
                    'repository': args.repository,
                    'namespace': args.namespace if args.namespace else '',
                    'package': package[0],
                    'format': args.format,
                    'upstream': origin_configuration['upstream'],
                    'publish': origin_configuration['publish']
                }
                writer.writerow(package_entry)


def main():
    """
    Main entry point. This function routes execution towards the appropriate sub-functions depending
    on the supplied arguments combination.
    """
    # The only mode that doesn't invoke the CA API is operating from a user-provided list
    # where origin configuration is also provided per-package.
    if args.from_list and args.configuration:
        return generate_from_provided_list()

    codeartifact_client = CodeArtifactClient(profile=args.profile if args.profile else os.environ.get('AWS_PROFILE'),
                                             region_name=args.region,
                                             codeartifact_domain=args.domain)

    if args.configuration:
        # Manual mode: this code path just applies the same configuration to all packages matching the query parameters
        write_from_query(codeartifact_client)
    else:
        # Auto mode: this code path tries to resolve where it's possible to block upstreams
        generate_default_package_configuration_entries(codeartifact_client)


def get_parser():
    command_description = "This command generates a list (file?) of package configurations containing " \
                          "the restrictions on origin of new versions for packages in the provided repository. " \
                          "We use a default heuristic that only blocks acquisition of new versions " \
                          "from upstreams if no versions of the package are available via any of your " \
                          "upstreams and otherwise allows both publishing new versions of the package " \
                          "into the repository and acquiring new versions of the package from upstreams. " \
                          "If you wish to supply your own origin restrictions instead, please use the " \
                          "--set-restrictions option. Please see usage examples further below.  " \
                          "By default, all packages in the repository will be considered, but you may use " \
                          "the various filtering arguments to scope down to packages of a specific format, " \
                          "under a specific namespace or following a specific prefix. " \
                          "You can combine the various filtering options, but note that the format " \
                          "is required in order to use the namespace filtering option"
    parser = argparse.ArgumentParser(description=command_description)
    parser.add_argument("--profile", help="AWS profile to be used (if environment variable not set).")
    parser.add_argument('--domain', required=True, help="CodeArtifact domain")
    parser.add_argument('--repository', required=True, help="Repository name")
    parser.add_argument('--region', required=True, help="AWS region")
    parser.add_argument('--namespace', help='Package namespace')
    parser.add_argument('--format', choices=PACKAGE_TYPES, help='Package format')
    parser.add_argument('--prefix', help='Package name search prefix')
    parser.add_argument('--set-restrictions', dest='configuration', help='A string describing the origin configuration.'
                                                                         'It should be supplied in the follwing form:'
                                                                         '\"publish=[BLOCK|ALLOW],'
                                                                         'upstream=[BLOCK|ALLOW]\". For example, to '
                                                                         'block all upstream for a package in a '
                                                                         'repository, you would assert '
                                                                         '\"publish=ALLOW,upstream=BLOCK\". See the '
                                                                         'CodeArtifact documentation for more info.')
    parser.add_argument('--from-list', help='Name of a file containing a list of packages. If this parameter is '
                                            'supplied the script creates a well-formed CSV file. This option '
                                            'requires \"--set-restrictions\"')
    parser.add_argument('--output-file', help='The file name to output to. If not supplied, it defaults to '
                                              '\"origin_configuration_[domain]_[repository].csv\"')
    return parser


def parse_args(input_args):
    """
    Additional argument processing outside of argparse scope
    """
    parser = get_parser()
    parsed_args = parser.parse_args(input_args)
    if not parsed_args.output_file:
        parsed_args.output_file = f'origin_configuration_{parsed_args.domain}_{parsed_args.repository}.csv'
    print(f"Result is going to be written to {parsed_args.output_file}")
    if parsed_args.from_list and parsed_args.format is None:
        parser.error("List-only requires package format")
    if parsed_args.namespace is not None and parsed_args.format is None:
        parser.error("Namespace requires package format")
    return parsed_args


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    main()
