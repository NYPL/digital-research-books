import textwrap
import os

from sqlalchemy import create_engine


def batched(generator, batch_size):
    """
    A generator function that yields batches from a generator of unknown length.
    """
    batch = []
    for item in generator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def read_env(var, require=True):
    """Read env var with an informative error.

    Optionally return None if var does not exist.
    """
    try:
        return os.environ[var]
    except KeyError:
        if require:
            raise ValueError(f'Environment variable "{var}" must be available.')


def wrap(s, max_width=80):
    wrapper = textwrap.TextWrapper(
        width=max_width, tabsize=4, replace_whitespace=False, drop_whitespace=True
    )
    return "\n".join([wrapper.fill(l) for l in s.splitlines()])


def load_db_env_vars():
    """Load PostgreSQL connection parameters from environment variables."""
    return {
        "user": read_env("POSTGRES_USER"),
        "pswd": read_env("POSTGRES_PSWD"),
        "host": read_env("POSTGRES_HOST"),
        "port": read_env("POSTGRES_PORT"),
        "db": read_env("POSTGRES_NAME"),
    }


# TODO: replace DBManager().generate_engine() with this
def create_sql_engine():
    conn_params = load_db_env_vars()
    connection_url = "postgresql://{user}:{pswd}@{host}:{port}/{db}".format(
        **conn_params
    )
    sql_engine = create_engine(  # MAYBE: do not assign to self to emphasis that this is only needed to construct session obj
        connection_url,
    )
    return sql_engine
