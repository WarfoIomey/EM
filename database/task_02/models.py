import sys
from pathlib import Path
from sqlalchemy import Column, Date, DateTime, Integer, String
from sqlalchemy.sql import func

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from database.task_01.db_config import db_config

Base = db_config.Base


class SpimexTraidingResult(Base):
    __tablename__ = 'spimex_trading_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(String, nullable=False)
    exchange_product_name = Column(String, nullable=False)
    oil_id = Column(String(4), nullable=False)
    delivery_basis_id = Column(String(3), nullable=False)
    delivery_basis_name = Column(String, nullable=False)
    delivery_type_id = Column(String(1), nullable=False)
    volume = Column(Integer, nullable=False)
    total = Column(Integer, nullable=False)
    count = Column(Integer, nullable=False)
    date = Column(Date)
    created_on = Column(DateTime, default=func.now())
    updated_on = Column(
        DateTime,
        default=func.now(),
        onupdate=func.now()
    )
