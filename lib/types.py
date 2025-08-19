from __future__ import annotations
from typing import Any, Dict, List, Optional, TypedDict


class FilterDict(TypedDict, total=False):
    """Type definition for filter objects."""
    column: str
    op: str
    value: Any


class SortDict(TypedDict, total=False):
    """Type definition for sort objects."""
    column: str
    direction: str


# Type aliases for common data structures
Record = Dict[str, Any]
Records = List[Record]
Filters = Optional[List[FilterDict]]
SortKeys = Optional[List[SortDict]]
SelectColumns = Optional[List[str]]