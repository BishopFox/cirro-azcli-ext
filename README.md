# cirro-azcli-ext

An Azure CLI extension that passively collects Azure resource data into a
SQLite database compatible with Cirro (https://github.com/bishopfox/cirro) schema. It also logs
all CLI invocations to a JSON-lines file.

Hooks into [knack events](https://github.com/microsoft/knack/blob/dev/docs/events.md):
- `EVENT_INVOKER_POST_PARSE_ARGS` — logs command arguments
- `EVENT_INVOKER_FILTER_RESULT` — writes resource data to SQLite

## Setup

### 1. Install the extension

```bash
az extension add --source dist/cirro_azcli_ext-*.whl
```

### 2. Disable the command index

```bash
az config set core.use_command_index=false
```

> Azure CLI lazy-loads extensions by command index. Disabling it ensures this
> extension loads for **all** commands so it can observe every invocation.

## How it works

When any `az` command returns data from a remote Azure endpoint (ARM, Graph,
Key Vault, etc.), the extension classifies each result item and writes it to
the appropriate SQLite table:

| Classification signal   | Example                        | Target table |
| ----------------------- | ------------------------------ | ------------ |
| `@odata.type` on item   | `#microsoft.graph.user`        | `users`      |
| ARM resource ID pattern | `/subscriptions/x/providers/…` | `resources`  |
| CLI command prefix      | `ad user list`                 | `users`      |

Local-only commands (`version`, `extension`, `config`, etc.) are skipped.

## Database schema (cirro-compatible)

### ARM resources
```sql
CREATE TABLE resources (
    id TEXT PRIMARY KEY, sub_id TEXT, rg_id TEXT, resource_type TEXT, data TEXT
);
```

### Key Vault items
```sql
CREATE TABLE keyVaultItems (
    id TEXT PRIMARY KEY, item_type TEXT, data TEXT
);
```

### Generic tables (18 tables, each `id TEXT PRIMARY KEY, data TEXT`)
applications · administrativeUnits · conditionalAccessPolicies · devices ·
directoryRoles · eligibleArmRBAC · eligibleRoleAssignments · groups ·
managementGroupEntities · namedLocations · organization ·
oauth2PermissionGrants · policies · roleAssignments · servicePrincipals ·
subscriptions · tenants · users

## Configuration

| Config key       | Default                     | Description             |
| ---------------- | --------------------------- | ----------------------- |
| `cirro.log_path` | `~/.azure/cli_args.log`     | Arguments log file path |
| `cirro.db_path`  | `~/.azure/cirro_collect.db` | SQLite database path    |

```bash
az config set cirro.db_path=/var/data/cirro_collect.db
```

## Commands

```bash
az cirro status          # show log stats and per-table row counts
az cirro status --tail 5 # show last 5 argument log entries
```
