# disk-expansion-precheck

A minimal MCP Server built with the official Python MCP SDK.

## Features

- Runs locally over `stdio`
- Exposes one MCP tool: `disk_expansion_precheck`
- Can be launched with `uvx disk-expansion-precheck`

## Tool

### `disk_expansion_precheck`

Returns a simple test string message.

Inputs:

- none

Output:

- string message

## Requirements

- Python 3.12+
- `uv` installed locally

## Run Locally

Use `uvx` after publishing the package:

```bash
uvx disk-expansion-precheck
```

Or run from source in the project directory:

```bash
uv run disk-expansion-precheck
```

## Cherry Studio Config

If you want to connect this server in Cherry Studio with `stdio`, use:

- Command: `uvx`
- Arguments: `disk-expansion-precheck`

Equivalent JSON-style config:

```json
{
  "mcpServers": {
    "disk-expansion-precheck": {
      "command": "uvx",
      "args": ["disk-expansion-precheck"]
    }
  }
}
```

## Package

This project is packaged for publishing to PyPI and local installation via `uvx`.

Source code lives in:

- `src/disk_expansion_precheck`
