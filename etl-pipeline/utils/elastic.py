import os
from pathlib import Path

from utils.utils import read_env


def build_hosts(scheme, host, port, user=None, pswd=None):
    creds = "{}:{}@".format(user, pswd) if user and pswd else ""

    # allows `host` to be a comma separated list of hosts
    hosts = []
    for _host in host.split(","):
        hosts.append("{}://{}{}:{}".format(scheme, creds, _host.strip(), port))

    return hosts


def load_hosts(cluster_prefix, scheme=None, host=None, port=None, user=None, pswd=None):
    """
    Load ES connection params from the environment.
    If scheme, host, port, or user is passed, it's value will override its associated env var.

    Params:
        cluster_prefix: used to construct env vars in the format
            `<cluster_prefix>_ELASTIC_SEARCH_<varname>`.
    """

    scheme = scheme or read_env(f"{cluster_prefix}_ELASTICSEARCH_SCHEME")
    # NOTE: user and password are not required bc a host str can be constructed without them.
    user = user or read_env(f"{cluster_prefix}_ELASTICSEARCH_USER", require=False)
    pswd = pswd or read_env(f"{cluster_prefix}_ELASTICSEARCH_PSWD", require=False)
    host = host or read_env(f"{cluster_prefix}_ELASTICSEARCH_HOST")
    port = port or read_env(f"{cluster_prefix}_ELASTICSEARCH_PORT")

    hosts = build_hosts(scheme, host, port, user, pswd)

    return {
        "hosts": hosts,
    }


def get_or_create_default_connection(cluster_prefix="VRA", es_version=9, **kwargs):
    """

    `es_version` determines which connection parameters are loaded from the environment and used to create the default connection. ....
    All other kwargs passed to managers.elasticsearch.load_connection_config().
    Arguments are ignored if the default connection already exists, and the
    already configured default connection is returned.
    """
    if es_version == 9:
        from elasticsearch.dsl import (
            connections,
        )  # ES9.2 SDK installed from patched github branch
        # from elasticsearch9.dsl import connections
    elif es_version == 7:
        from elasticsearch7_dsl import connections
    else:
        raise ValueError("only elasticsearch 9.X and 7.X supported")

    if "default" not in connections.connections._conns.keys():
        # register global default client
        client = connections.create_connection(
            **load_hosts(cluster_prefix, es_version=es_version, **kwargs)
        )
        return client
    else:
        return connections.get_connection("default")
