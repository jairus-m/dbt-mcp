import asyncio
import os
from typing import Literal, cast

from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp

# Cast environment variable to Literal type for FastMCP transport parameter
TransportType = Literal["stdio", "sse", "streamable-http"]
TRANSPORT = cast(TransportType, os.environ.get("MCP_TRANSPORT", "stdio"))


def main() -> None:
    config = load_config()
    server = asyncio.run(create_dbt_mcp(config))
    server.run(transport=TRANSPORT)


if __name__ == "__main__":
    main()
