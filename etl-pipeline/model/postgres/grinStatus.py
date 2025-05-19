from enum import Enum
from sqlalchemy.dialects.postgresql import ENUM, INTEGER
from sqlalchemy import ForeignKey, Unicode, Integer, Column
from sqlalchemy.orm import relationship
from datetime import datetime

from .base import Base, Core


class GRINState(Enum):
    PENDING_CONVERSION = "pending_conversion"
    CONVERTING = "converting"
    CONVERTED = "converted"
    DOWNLOADED = "downloaded"


class GRINStatus(Base, Core):
    __tablename__ = "grin_statuses"
    barcode = Column(Unicode, primary_key=True)  # Uses the GRIN bardcode
    record_id = Column(Integer, ForeignKey("records.id"), unique=True)
    failed_download = Column(
        INTEGER, default=0
    )  # Tracks how many times have we failed to download a specific file
    state = Column(
        Unicode,
        ENUM(
            "pending_conversion",
            "converting",
            "converted",
            "downloaded",
            name="state",
            create_type=False,
        ),
        nullable=False,
    )
    record = relationship("Record", back_populates="grin_status")
    
    def historical_timestamp():
        return datetime(1991, 8, 25)

    def __repr__(self):
        return "<GRINStatus(record_id={}, failed_download={}, state={})>".format(
            self.record_id, self.failed_download, self.state
        )

    def __dir__(self):
        return ["record_id", "failed_download", "state"]
