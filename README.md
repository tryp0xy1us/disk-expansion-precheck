# disk-expansion-precheck

An MCP Server for Alibaba Cloud ECS disk expansion precheck.

## Features

- Runs over `stdio`
- Exposes one MCP tool: `disk_expansion_precheck`
- Validates inputs before execution
- Performs six precheck groups for Alibaba Cloud ECS disk expansion:
  - instance information
  - online expansion support
  - temporary storage risk
  - disk layout and filesystem usage
  - backup management
  - resource quota

## Requirements

- Python 3.12+
- `uv`
- Alibaba Cloud credentials available as environment variables:
  - `ALIBABA_CLOUD_ACCESS_KEY_ID`
  - `ALIBABA_CLOUD_ACCESS_KEY_SECRET`
  - `ALIBABA_CLOUD_SECURITY_TOKEN` when using STS credentials

## Run From Source

```bash
uv sync
uv run disk-expansion-precheck
```

## Run After Publishing

```bash
uvx disk-expansion-precheck
```

## Tool

### `disk_expansion_precheck`

This tool returns a structured JSON precheck report.

Required inputs:

- `cloud_provider`
- `region`
- `instance_id`
- `disk_id`
- `target_size_gb`
- `os_type`
- `disk_role`

Common optional inputs:

- `dist_name`
- `current_size_gb`
- `filesystem_type`
- `partition_scheme`
- `lvm_in_use`
- `service_criticality`
- `change_window`
- `provider_constraints`
- `asset_information`
- `topology_information`
- `zone_id`
- `instance_name`
- `expected_backup_policy`
- `quota_scope_hint`
- `temporary_disk_hints`

Output structure:

- `status`
- `summary`
- `checks`
- `risk_summary`
- `recommended_next_steps`
- `error`

## Inspector

Local source run:

```bash
npx @modelcontextprotocol/inspector uv run disk-expansion-precheck
```

## Cherry Studio Config

Published package:

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

Source run:

```json
{
  "mcpServers": {
    "disk-expansion-precheck": {
      "command": "uv",
      "args": ["run", "disk-expansion-precheck"]
    }
  }
}
```

## Package Layout

Source code lives in:

- `src/disk_expansion_precheck`
