from __future__ import annotations
import json
from typing import Optional
from mcp.server.fastmcp import FastMCP
from lib.http_utils import fetch_published_csv, fetch_published_csv_from_url
from lib.csv_utils import records_from_csv
from lib.data_processing import page


def register_tool(mcp: FastMCP):
    """Register the list_rows_pub tool with the MCP server."""
    
    @mcp.tool()
    async def list_rows_pub(
        sheets_url: Optional[str] = None, 
        pub_id: Optional[str] = None, 
        gid: str = "0", 
        header_row: int = 1, 
        limit: int = 100, 
        offset: int = 0
    ) -> str:
        """List rows from a *published* Google Sheet tab (no auth). 
        
        Args:
            sheets_url: Full published Google Sheets CSV URL (e.g., https://docs.google.com/.../pub?gid=123&single=true&output=csv)
            pub_id: The published sheet ID (2PACX... from 'Publish to web') - for backward compatibility
            gid: The sheet tab ID (default: "0") - only used with pub_id
            header_row: Row number containing headers (default: 1)
            limit: Maximum number of rows to return (default: 100)
            offset: Number of rows to skip (default: 0)
        """
        try:
            if sheets_url:
                csv_text = await fetch_published_csv_from_url(sheets_url)
            elif pub_id:
                csv_text = await fetch_published_csv(pub_id, gid)
            else:
                return "Error: Either sheets_url or pub_id must be provided"
            
            records = records_from_csv(csv_text, header_row)
            return json.dumps(page(records, offset, limit), indent=2, ensure_ascii=False)
        except Exception as e:
            return f"Error fetching published CSV: {e}"