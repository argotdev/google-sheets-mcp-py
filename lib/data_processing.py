from __future__ import annotations
from typing import Any, Dict, List


def try_cast(v: Any):
    """Attempt to cast a value to its appropriate type (bool, int, float, or str)."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    
    s = str(v).strip()
    if s.lower() in ("true", "false"):
        return s.lower() == "true"
    
    try:
        return int(s)
    except:
        pass
    
    try:
        return float(s)
    except:
        pass
    
    return s


def apply_filters(records, filters=None, case_insensitive=True):
    """Apply filter conditions to records."""
    if not filters:
        return records
    
    def match(rec):
        for f in filters:
            # Support shorthand format: {"column_name": "value"}
            if "column" not in f and "op" not in f and "value" not in f:
                # Assume it's shorthand format
                if len(f) == 1:
                    col = list(f.keys())[0]
                    val = f[col]
                    op = "=="
                else:
                    continue  # Skip malformed filters
            else:
                # Standard format: {"column": "col", "op": "==", "value": "val"}
                col = f.get("column")
                op = (f.get("op") or "==").lower()
                val = f.get("value")
            
            L = rec.get(col)
            R = val
            Lc = try_cast(L)
            Rc = try_cast(R)
            
            if isinstance(Lc, str) and isinstance(Rc, str) and case_insensitive:
                Lc, Rc = Lc.casefold(), Rc.casefold()
            
            if op in ("==", "eq"):
                ok = Lc == Rc
            elif op in ("!=", "ne"):
                ok = Lc != Rc
            elif op in (">", "gt"):
                ok = (Lc is not None and Rc is not None and Lc > Rc)
            elif op in (">=", "ge"):
                ok = (Lc is not None and Rc is not None and Lc >= Rc)
            elif op in ("<", "lt"):
                ok = (Lc is not None and Rc is not None and Lc < Rc)
            elif op in ("<=", "le"):
                ok = (Lc is not None and Rc is not None and Lc <= Rc)
            elif op == "contains":
                if not isinstance(Lc, str) or not isinstance(Rc, str):
                    return False
                a = Lc.casefold() if case_insensitive else Lc
                b = Rc.casefold() if case_insensitive else Rc
                ok = b in a
            elif op == "in":
                arr = [try_cast(x) for x in (val or [])]
                if isinstance(Lc, str) and case_insensitive:
                    arr = [x.casefold() if isinstance(x, str) else x for x in arr]
                    ok = Lc.casefold() in arr
                else:
                    ok = Lc in arr
            else:
                return False
            
            if not ok:
                return False
        return True
    
    return [r for r in records if match(r)]


def apply_select(records, select=None):
    """Apply column selection to records."""
    if not select:
        return records
    
    cols = [c for c in select if c]
    return [{c: r.get(c) for c in cols} for r in records]


def apply_sort(records, sort=None):
    """Apply sorting to records."""
    if not sort:
        return records
    
    out = records[:]
    for key in reversed(sort):
        col = key.get("column")
        reverse = (key.get("direction") or "asc").lower() == "desc"
        out.sort(key=lambda rec: ((rec.get(col) is None), try_cast(rec.get(col))), reverse=reverse)
    
    return out


def page(records, offset: int, limit: int):
    """Apply pagination to records."""
    if offset < 0:
        offset = 0
    if limit <= 0:
        return []
    return records[offset: offset + limit]


def apply_paging_pipeline(base, filters, select, sort, limit, offset, case_insensitive):
    """Apply complete data processing pipeline with filters, sort, select, and paging."""
    filtered = apply_filters(base, filters, case_insensitive)
    sorted_ = apply_sort(filtered, sort)
    proj = apply_select(sorted_, select)
    return page(proj, offset, limit)