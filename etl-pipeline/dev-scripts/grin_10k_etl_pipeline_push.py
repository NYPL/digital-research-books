from model.postgres.record import Record
from model.postgres.item import Item
from sqlalchemy import select, text, Table, MetaData

from scripts.addHasPartToGRINRecords import main
from managers import DBManager
from utils.load_env import load_env
from logger import configure_loggers

load_env("config/.env.production")
configure_loggers()

engine = DBManager().generate_engine()

grin_public_domain_10k = Table(
    "grin_public_domain_10k", MetaData(), autoload_with=engine
)

## all 10k books
# stmt = select(Record).join(
#     grin_public_domain_10k, grin_public_domain_10k.c.record_id == Record.id
# )

# 10k not in Items
stmt = (
    select(Record)
    .join(grin_public_domain_10k, grin_public_domain_10k.c.record_id == Record.id)
    .outerjoin(Item, Item.record_id == Record.id)
    .where(Item.id.is_(None))
)


main(stmt=stmt)
