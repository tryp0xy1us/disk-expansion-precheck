from mcp.server.fastmcp import FastMCP

# 创建MCP服务器
mcp = FastMCP("disk-expansion-precheck", json_response=True)

# 注册MCP工具
@mcp.tool()
def disk_expansion_precheck() -> str:
	"""执行磁盘扩容预检操作"""
	return "Hello from disk-expansion-precheck MCP Server!"

def main() -> None:
	mcp.run(transport="stdio")
