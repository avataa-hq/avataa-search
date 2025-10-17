import enum

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

from settings.config import DB_SCHEMA

Base = declarative_base()


class LoadStatus(enum.Enum):
    IN_PROGRESS = "in progress"
    NOT_IN_PROGRESS = "not in progress"


class InventoryObjIndexLoadOrder(Base):
    __tablename__ = "inventory_obj_index_load_order"
    __table_args__ = {"schema": DB_SCHEMA}
    id: int = Column("id", Integer, primary_key=True)
    tmo_id: int = Column("tmo_id", Integer, nullable=False)
    load_status: str = Column("load_status", String(32), nullable=False)
