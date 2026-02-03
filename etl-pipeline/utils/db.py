from .common import require_env

from sqlalchemy import create_engine


def load_db_env_vars():
    """Load PostgreSQL connection parameters from environment variables."""
    return {
        "user": require_env("POSTGRES_USER"),
        "pswd": require_env("POSTGRES_PSWD"),
        "host": require_env("POSTGRES_HOST"),
        "port": require_env("POSTGRES_PORT"),
        "db": require_env("POSTGRES_NAME"),
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
