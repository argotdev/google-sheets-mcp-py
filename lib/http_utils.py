from __future__ import annotations
import httpx


async def fetch_published_csv(pub_id: str, gid: str = "0") -> str:
    """Fetch CSV data from a published Google Sheets document using a pub_id (2PACX...)."""
    if gid and gid != "0":
        url = f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?gid={gid}&single=true&output=csv"
    else:
        url = f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?single=true&output=csv"
    
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


async def fetch_published_csv_from_url(sheets_url: str) -> str:
    """Fetch CSV data directly from a published Google Sheets CSV URL."""
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(sheets_url)
        r.raise_for_status()
        return r.text