from __future__ import annotations
import logging
from mcp.server.fastmcp import FastMCP
from tools.gsheets import register_tools

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("gsheets-zeroauth")
mcp = FastMCP("gsheets-zeroauth")

register_tools(mcp)

if __name__ == "__main__":
    mcp.run(transport="stdio")
