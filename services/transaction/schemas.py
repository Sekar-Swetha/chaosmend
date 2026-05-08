"""
schemas.py – Transaction Service

WHY Pydantic schemas separate from SQLAlchemy models?
  - Models describe DB shape; schemas describe API shape.
  - We may expose less data than we store (e.g. hide internal IDs).
  - Pydantic validates incoming JSON for free (wrong type → 422 error).
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class TransactionCreate(BaseModel):
    """What the client sends when creating a transaction."""
    user_id: str = Field(..., description="User identifier", example="user_42")
    amount: float = Field(..., gt=0, description="Transaction amount (must be positive)")
    currency: str = Field(default="USD", max_length=3, description="ISO 4217 currency code")
    description: Optional[str] = Field(None, description="Optional memo/note")


class TransactionResponse(BaseModel):
    """What we send back — includes server-generated fields."""
    id: str
    user_id: str
    amount: float
    currency: str
    description: Optional[str]
    risk_level: str
    created_at: datetime

    # model_config tells Pydantic to read from ORM objects
    # (not just plain dicts) — required when returning SQLAlchemy rows
    model_config = {"from_attributes": True}
