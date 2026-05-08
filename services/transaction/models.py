"""
models.py – Transaction Service

WHY a separate models file?
  Keeps DB schema in one place. If we add a field
  (e.g. `geo_location`), we only touch this file.
"""
import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, DateTime, Enum as SAEnum
from database import Base
import enum


class RiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    UNKNOWN = "UNKNOWN"


class Transaction(Base):
    __tablename__ = "transactions"

    # UUID primary key: avoids sequential ID guessing attacks
    # and works across distributed systems without coordination
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    amount = Column(Float, nullable=False)
    currency = Column(String(3), nullable=False, default="USD")
    description = Column(String, nullable=True)
    risk_level = Column(SAEnum(RiskLevel), default=RiskLevel.UNKNOWN)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
