from __future__ import annotations
from mcp.server.fastmcp import FastMCP
from .index import register_all_tools


def register_tools(mcp: FastMCP):
    """Register all Google Sheets tools with the MCP server."""
    register_all_tools(mcp)