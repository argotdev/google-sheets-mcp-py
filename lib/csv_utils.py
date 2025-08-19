from __future__ import annotations
import csv
import io
from typing import Any, Dict, List


def normalize_header(header_row: List[str]) -> List[str]:
    """Normalize CSV headers, ensuring non-empty names."""
    out = []
    for i, name in enumerate(header_row):
        name = (name or "").strip()
        out.append(name if name else f"col{i+1}")
    return out


def records_from_csv(text: str, header_row: int = 1) -> List[Dict[str, Any]]:
    """Convert CSV text to list of dictionaries with normalized headers."""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return []
    
    idx = max(1, header_row) - 1
    if idx >= len(rows):
        return []
    
    header = normalize_header(rows[idx])
    data = rows[idx+1:]
    out = []
    
    for r in data:
        r = list(r) + [None] * (len(header) - len(r))
        out.append({header[i]: r[i] for i in range(len(header))})
    
    return out


def to_csv(records: List[Dict[str, Any]], include_header=True) -> str:
    """Convert records to CSV string format."""
    if not records:
        return ""
    
    buf = io.StringIO()
    fieldnames = list(records[0].keys())
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    
    if include_header:
        w.writeheader()
    w.writerows(records)
    
    return buf.getvalue()