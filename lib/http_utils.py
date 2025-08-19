from __future__ import annotations
import httpx


async def fetch_published_csv(pub_id: str, gid: str = "0") -> str:
    """Fetch CSV data from a published Google Sheets document."""
    url = f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?gid={gid}&single=true&output=csv"
    
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text