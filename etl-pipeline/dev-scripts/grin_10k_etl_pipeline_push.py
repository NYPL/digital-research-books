from model.postgres.record import Record
from sqlalchemy import select, Table, MetaData

from scripts.addHasPartToGRINRecords import main
from managers import DBManager
from load_env import load_env_file
from logger import configure_loggers

load_env_file("production", file_string="config/{}.yaml")
configure_loggers()

engine = DBManager().generate_engine()
grin_public_domain_10k = Table(
    "grin_public_domain_10k", MetaData(), autoload_with=engine
)


stmt = select(Record).join(
    grin_public_domain_10k, grin_public_domain_10k.c.record_id == Record.id
)

main(stmt=stmt)
