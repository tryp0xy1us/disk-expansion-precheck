# disk-expansion-precheck

A minimal MCP Server for disk expansion precheck experiments.

The currently registered tool is only for testing and returns a simple string message.

## Install

Use `uvx` directly:

```bash
uvx disk-expansion-precheck
```

Or install from PyPI after publishing:

```bash
pip install disk-expansion-precheck
```

## What It Does

This server currently exposes one test tool:

- `disk_expansion_precheck`: returns a simple string response

## Notes

This project is still in an early test stage. The current implementation is intended only to verify MCP Server packaging, startup, and distribution workflow.
