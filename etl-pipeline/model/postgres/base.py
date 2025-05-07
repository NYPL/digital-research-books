from datetime import datetime, timezone
from sqlalchemy import Column, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Core(object):
    """A mixin for other SQLAlchemy ORM classes. Includes a date_created and
    date_updated field for all database tables."""

    date_created = Column(
        DateTime, default=datetime.now(timezone.utc).replace(tzinfo=None), index=True
    )

    date_modified = Column(
        DateTime,
        default=datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=datetime.now(timezone.utc).replace(tzinfo=None),
        index=True,
    )

    def __iter__(self):
        for key in dir(self):
            yield (key, getattr(self, key))
