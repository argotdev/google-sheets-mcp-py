from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP
from lib.http_utils import fetch_published_csv
from lib.csv_utils import records_from_csv, to_csv
from lib.data_processing import apply_filters, apply_select


def register_tool(mcp: FastMCP):
    """Register the export_subset_pub tool with the MCP server."""
    
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
        """Export a filtered/select subset from a *published* sheet tab as CSV or JSON (returned inline).
        
        Args:
            pub_id: The published sheet ID (2PACX... from 'Publish to web')
            gid: The sheet tab ID
            filters: List of filter conditions to apply
            select: List of column names to include in output
            header_row: Row number containing headers (default: 1)
            format: Output format, either "csv" or "json" (default: "csv")
            include_header: Whether to include header row in CSV output (default: True)
        """
        try:
            csv_text = await fetch_published_csv(pub_id, gid)
            base = records_from_csv(csv_text, header_row)
            subset = apply_select(apply_filters(base, filters, True), select)
            
            if (format or "csv").lower() == "json":
                return json.dumps(subset, indent=2, ensure_ascii=False)
            else:
                if select:
                    subset = [{c: r.get(c) for c in select} for r in subset]
                return to_csv(subset, include_header)
        except Exception as e:
            return f"Error exporting published subset: {e}"