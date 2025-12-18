import os
from pathlib import Path


def read_env(var, require=True):
    """Read env var with an informative error.

    Optionally return None if var does not exist.
    """
    try:
        return os.environ[var]
    except KeyError:
        if require:
            raise ValueError(f'Environment variable "{var}" must be available.')


def build_hosts(scheme, host, port, user=None, pswd=None):
    creds = "{}:{}@".format(user, pswd) if user and pswd else ""

    # Allowing multiple hosts for a ES connection
    hosts = []
    for _host in host.split(","):
        hosts.append("{}://{}{}:{}".format(scheme, creds, _host.strip(), port))

    return hosts


# TODO: fix different units in ES7 vs >ES7
def load_connection_config(timeout=None, es_version=9):
    scheme = read_env("ELASTICSEARCH_SCHEME")
    user = read_env("ELASTICSEARCH_USER", require=False)
    pswd = read_env("ELASTICSEARCH_PSWD", require=False)
    host = read_env("ELASTICSEARCH_HOST")
    port = read_env("ELASTICSEARCH_PORT")

    hosts = build_hosts(scheme, host, port, user, pswd)

    # TODO: add the retry keys etx..
    return {
        "hosts": hosts,
        "request_timeout" if es_version >= 8 else "timeout": timeout,
    }


def get_or_create_default_connection(*args, **kwargs):
    """All arguments passed to managers.elasticsearch.load_connection_config().
    Arguments are ignored if the default connection already exists.
    """
    assert kwargs.get("es_version", 9) == 9, "only elasticsearch 9.X supported"
    from elasticsearch.dsl import connections
    # from elasticsearch_dsl import connections

    if "default" not in connections.connections._conns.keys():
        # register global default client
        client = connections.create_connection(
            **load_connection_config(*args, **kwargs)
        )
        return client
    else:
        return connections.get_connection("default")


def delete_index(index_name):
    from elasticsearch.dsl import Index
    # from elasticsearch_dsl import Index

    get_or_create_default_connection()

    index = Index(index_name)
    print(f"Deleting index {index_name}...")
    index.delete()
    if index.exists() is not False:
        raise Exception(f"Failed to delete index {index_name}")
    print(f"Deleted index {index_name}.")
