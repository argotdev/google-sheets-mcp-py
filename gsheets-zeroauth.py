from __future__ import annotations
import csv, io, json, logging, asyncio
from typing import Any, Dict, List, Optional
import httpx
from mcp.server.fastmcp import FastMCP

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gsheets-zeroauth")
mcp = FastMCP("gsheets-zeroauth")

# -------- helpers --------
def _normalize_header(header_row: List[str]) -> List[str]:
    out = []
    for i, name in enumerate(header_row):
        name = (name or "").strip()
        out.append(name if name else f"col{i+1}")
    return out

def _try_cast(v: Any):
    if v is None: return None
    if isinstance(v, (int, float, bool)): return v
    s = str(v).strip()
    if s.lower() in ("true", "false"): return s.lower() == "true"
    try: return int(s)
    except: pass
    try: return float(s)
    except: pass
    return s

def _records_from_csv(text: str, header_row: int = 1) -> List[Dict[str, Any]]:
    # parse CSV text -> list of dicts
    rows = list(csv.reader(io.StringIO(text)))
    if not rows: return []
    idx = max(1, header_row) - 1
    if idx >= len(rows): return []
    header = _normalize_header(rows[idx])
    data = rows[idx+1:]
    out = []
    for r in data:
        r = list(r) + [None] * (len(header) - len(r))
        out.append({header[i]: r[i] for i in range(len(header))})
    return out

def _apply_filters(records, filters=None, case_insensitive=True):
    if not filters: return records
    def match(rec):
        for f in filters:
            col = f.get("column"); op = (f.get("op") or "==").lower(); val = f.get("value")
            L = rec.get(col); R = val
            Lc = _try_cast(L); Rc = _try_cast(R)
            if isinstance(Lc, str) and isinstance(Rc, str) and case_insensitive:
                Lc, Rc = Lc.casefold(), Rc.casefold()
            if op in ("==","eq"):   ok = Lc == Rc
            elif op in ("!=","ne"): ok = Lc != Rc
            elif op in (">","gt"):  ok = (Lc is not None and Rc is not None and Lc >  Rc)
            elif op in (">=","ge"): ok = (Lc is not None and Rc is not None and Lc >= Rc)
            elif op in ("<","lt"):  ok = (Lc is not None and Rc is not None and Lc <  Rc)
            elif op in ("<=","le"): ok = (Lc is not None and Rc is not None and Lc <= Rc)
            elif op == "contains":
                if not isinstance(L, str) or not isinstance(R, str): return False
                a = L.casefold() if case_insensitive else L
                b = R.casefold() if case_insensitive else R
                ok = b in a
            elif op == "in":
                arr = [_try_cast(x) for x in (val or [])]
                if isinstance(Lc, str) and case_insensitive:
                    arr = [x.casefold() if isinstance(x, str) else x for x in arr]
                    ok = Lc.casefold() in arr
                else:
                    ok = Lc in arr
            else:
                return False
            if not ok: return False
        return True
    return [r for r in records if match(r)]

def _apply_select(records, select=None):
    if not select: return records
    cols = [c for c in select if c]
    return [{c: r.get(c) for c in cols} for r in records]

def _apply_sort(records, sort=None):
    if not sort: return records
    out = records[:]
    for key in reversed(sort):
        col = key.get("column")
        reverse = (key.get("direction") or "asc").lower() == "desc"
        out.sort(key=lambda rec: ((rec.get(col) is None), _try_cast(rec.get(col))), reverse=reverse)
    return out

def _page(records, offset: int, limit: int):
    if offset < 0: offset = 0
    if limit <= 0: return []
    return records[offset: offset + limit]

async def _fetch_published_csv(pub_id: str, gid: str = "0") -> str:
    url = f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?gid={gid}&single=true&output=csv"
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text

def _to_csv(records: List[Dict[str, Any]], include_header=True) -> str:
    if not records: return ""
    buf = io.StringIO()
    fieldnames = list(records[0].keys())
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    if include_header: w.writeheader()
    w.writerows(records)
    return buf.getvalue()

# -------- tools (zero-auth using published CSV) --------
@mcp.tool()
async def list_rows_pub(pub_id: str, gid: str = "0", header_row: int = 1, limit: int = 100, offset: int = 0) -> str:
    """List rows from a *published* Google Sheet tab (no auth). Args: pub_id (2PACX... from 'Publish to web'), gid (sheet tab id)."""
    try:
        csv_text = await _fetch_published_csv(pub_id, gid)
        records = _records_from_csv(csv_text, header_row)
        return json.dumps(_page(records, offset, limit), indent=2, ensure_ascii=False)
    except Exception as e:
        return f"Error fetching published CSV: {e}"

@mcp.tool()
async def query_rows_pub(
    pub_id: str,
    gid: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    select: Optional[List[str]] = None,
    sort: Optional[List[Dict[str, str]]] = None,
    header_row: int = 1,
    limit: int = 100,
    offset: int = 0,
    case_insensitive: bool = True,
) -> str:
    """Query rows (filters/select/sort/paging) from a *published* sheet tab (no auth)."""
    try:
        csv_text = await _fetch_published_csv(pub_id, gid)
        base = _records_from_csv(csv_text, header_row)
        out = _apply_paging_pipeline(base, filters, select, sort, limit, offset, case_insensitive)
        return json.dumps(out, indent=2, ensure_ascii=False)
    except Exception as e:
        return f"Error querying published CSV: {e}"

def _apply_paging_pipeline(base, filters, select, sort, limit, offset, ci):
    filtered = _apply_filters(base, filters, ci)
    sorted_  = _apply_sort(filtered, sort)
    proj     = _apply_select(sorted_, select)
    return _page(proj, offset, limit)

@mcp.tool()
async def export_subset_pub(
    pub_id: str,
    gid: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    select: Optional[List[str]] = None,
    header_row: int = 1,
    format: str = "csv",
    include_header: bool = True,
) -> str:
    """Export a filtered/select subset from a *published* sheet tab as CSV or JSON (returned inline)."""
    try:
        csv_text = await _fetch_published_csv(pub_id, gid)
        base = _records_from_csv(csv_text, header_row)
        subset = _apply_select(_apply_filters(base, filters, True), select)
        if (format or "csv").lower() == "json":
            return json.dumps(subset, indent=2, ensure_ascii=False)
        else:
            # CSV
            if select:
                # ensure column order as requested
                subset = [{c: r.get(c) for c in select} for r in subset]
            return _to_csv(subset, include_header)
    except Exception as e:
        return f"Error exporting published subset: {e}"

if __name__ == "__main__":
    mcp.run(transport="stdio")
