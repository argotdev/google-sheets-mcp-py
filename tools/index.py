from __future__ import annotations
from mcp.server.fastmcp import FastMCP
from . import list_rows_pub, query_rows_pub, export_subset_pub


def register_all_tools(mcp: FastMCP):
    """Register all available tools with the MCP server."""
    list_rows_pub.register_tool(mcp)
    query_rows_pub.register_tool(mcp)
    export_subset_pub.register_tool(mcp)