# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

__author__ = "Davide Semenzin (davidesn@amazon.com)"
__version__ = "1.0.0"

import boto3
from botocore.config import Config


class CodeArtifactClient:
    DOMAIN = None
    AWS_CONFIG = None
    BOTO3_SESSION = None
    codeartifact_client = None

    def __init__(self, profile, region_name, codeartifact_domain):
        self.configure(profile, region_name, codeartifact_domain)
        self.codeartifact_client = self.BOTO3_SESSION.client('codeartifact', config=self.AWS_CONFIG)

    def configure(self, profile_name, region_name, domain):
        self.DOMAIN = domain
        self.AWS_CONFIG = Config(region_name=region_name, retries={'max_attempts': 10, 'mode': 'standard'})
        self.BOTO3_SESSION = boto3.session.Session(profile_name=profile_name)
        print(f"Authenticated using account {self.BOTO3_SESSION.client('sts').get_caller_identity().get('Account')}")

    def get_restrictions(self, domain, repository, format, namespace, package, **kwargs):
        if domain != self.DOMAIN:
            raise Exception(f'Domain mismatch! You supplied {domain} but we expected {self.DOMAIN}.')
        input = {
            'domain': domain,
            'repository': repository,
            'format': format,
            'package': package
        }
        if namespace:
            input['namespace'] = namespace
        res = self.codeartifact_client.describe_package(**input) \
            .get('package').get('originConfiguration').get('restrictions')
        return res

    def apply_restrictions(self, domain, repository, format, namespace, package, upstream, publish):
        if domain != self.DOMAIN:
            raise Exception('Domain mismatch!')
        input = {
            'domain': domain,
            'repository': repository,
            'format': format,
            'package': package,
            'restrictions': {
                'upstream': upstream,
                'publish': publish,
            }
        }
        if namespace:
            input['namespace'] = namespace
        res = self.codeartifact_client.put_package_origin_configuration(**input)
        return res

    def list_packages_in_repository(self, repository, package_format=None, package_namespace=None, package_prefix=None):
        input = {
            "domain": self.DOMAIN,
            "repository": repository
        }
        if package_prefix:
            input['packagePrefix'] = package_prefix
        if package_namespace:
            input['namespace'] = package_namespace
        if package_format:
            input['format'] = package_format
        paginator = self.codeartifact_client.get_paginator("list_packages")
        page_iterator = paginator.paginate(**input)
        ret = []
        for page in page_iterator:
             ret += page.get("packages")
        return ret

    def get_repo_upstreams_and_external_connections(self, repository, domain=None):
        repo = self.codeartifact_client.describe_repository(
            domain=self.DOMAIN if not domain else domain,
            repository=repository)
        upstreams = [upstream['repositoryName'] for upstream in repo['repository']['upstreams']]
        external_connections = repo['repository']['externalConnections']
        if repository in upstreams:
            upstreams.remove(repository)
        return upstreams, external_connections

    # DFS from a given repository
    def get_upstream_graph(self, repository):
        visited = set()
        upstream_graph = self._recursive_get_upstreams(visited, {}, repository)
        return upstream_graph

    # DFS helper
    def _recursive_get_upstreams(self, visited, graph, repository):
        upstreams, ecs = self.get_repo_upstreams_and_external_connections(repository)
        graph[repository] = upstreams
        if repository not in visited:
            visited.add(repository)
            for upstream in upstreams:
                self._recursive_get_upstreams(visited, graph, upstream)
        return graph
