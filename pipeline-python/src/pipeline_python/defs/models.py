from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID


@dataclass(frozen=True)
class RawFileRecord:
    """Row model for catalog.raw_files."""

    id: UUID
    source: str
    dataset: str
    date: date
    s3_key: str


@dataclass(frozen=True)
class CuratedDataRecord:
    """Row model for catalog.curated_files."""

    id: UUID
    raw_file_id: UUID
    variable: str
    unit: str
    timestamp: datetime
