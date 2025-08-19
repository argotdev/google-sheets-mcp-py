from __future__ import annotations
import httpx
import re
from urllib.parse import urlparse, parse_qs


def parse_sheets_url(url: str) -> tuple[str, str]:
    """Parse a Google Sheets URL to extract pub_id and gid.
    
    Supports both regular spreadsheet URLs and published URLs.
    
    Args:
        url: Full Google Sheets URL
        
    Returns:
        Tuple of (pub_id, gid)
        
    Raises:
        ValueError: If the URL format is not recognized
    """
    parsed = urlparse(url)
    
    # Handle published URLs: https://docs.google.com/spreadsheets/d/e/2PACX.../pub?gid=123...
    if "/pub" in parsed.path:
        match = re.search(r'/d/e/([^/]+)/', parsed.path)
        if match:
            pub_id = match.group(1)
            query_params = parse_qs(parsed.query)
            gid = query_params.get('gid', ['0'])[0]
            return pub_id, gid
    
    # Handle regular spreadsheet URLs: https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', parsed.path)
    if match:
        spreadsheet_id = match.group(1)
        # Extract gid from fragment (after #)
        if parsed.fragment:
            gid_match = re.search(r'gid=(\d+)', parsed.fragment)
            gid = gid_match.group(1) if gid_match else "0"
        else:
            # Check query parameters for gid
            query_params = parse_qs(parsed.query)
            gid = query_params.get('gid', ['0'])[0]
        
        # For regular URLs, we need to assume this is a published sheet
        # The user should provide the published version, but we'll handle it gracefully
        return spreadsheet_id, gid
    
    raise ValueError(f"Unable to parse Google Sheets URL: {url}")


async def fetch_published_csv(pub_id: str, gid: str = "0") -> str:
    """Fetch CSV data from a published Google Sheets document."""
    url = f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?gid={gid}&single=true&output=csv"
    
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


async def fetch_published_csv_from_url(sheets_url: str) -> str:
    """Fetch CSV data from a Google Sheets URL."""
    pub_id, gid = parse_sheets_url(sheets_url)
    return await fetch_published_csv(pub_id, gid)