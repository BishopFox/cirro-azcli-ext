import json
import os
import sys
from datetime import datetime, timezone

from azure.cli.core import AzCommandsLoader
from azure.cli.core.commands import CliCommandType
from knack.events import EVENT_INVOKER_FILTER_RESULT, EVENT_INVOKER_POST_PARSE_ARGS

DEFAULT_LOG_PATH = os.path.join(os.path.expanduser("~"), ".azure", "cli_args.log")
DEFAULT_DB_PATH = os.path.join(os.path.expanduser("~"), ".azure", "cirro_collect.db")

# Command groups that are purely local and never call remote Azure endpoints.
_LOCAL_ONLY_COMMAND_GROUPS = frozenset(
    {
        "cache",
        "cirro",
        "config",
        "configure",
        "demo",
        "extension",
        "feedback",
        "find",
        "interactive",
        "self-test",
        "survey",
        "upgrade",
        "version",
    }
)

# Map @odata.type suffixes to cirro table names.
_ODATA_TYPE_MAP = {
    "user": "users",
    "group": "groups",
    "application": "applications",
    "serviceprincipal": "servicePrincipals",
    "device": "devices",
    "directoryrole": "directoryRoles",
    "administrativeunit": "administrativeUnits",
    "conditionalaccesspolicy": "conditionalAccessPolicies",
    "namedlocation": "namedLocations",
    "ipnamedlocation": "namedLocations",
    "countrynamedlocation": "namedLocations",
    "organization": "organization",
    "oauth2permissiongrant": "oauth2PermissionGrants",
}

# Map CLI command prefixes to cirro table names.
_COMMAND_PREFIX_MAP = {
    "ad user": "users",
    "ad group": "groups",
    "ad app": "applications",
    "ad sp": "servicePrincipals",
    "ad device": "devices",
    "ad directory-role": "directoryRoles",
    "ad administrative-unit": "administrativeUnits",
    "role assignment": "roleAssignments",
    "account subscription": "subscriptions",
    "account tenant": "tenants",
    "account management-group": "managementGroupEntities",
    "keyvault secret": "keyVaultItems",
    "keyvault key": "keyVaultItems",
    "keyvault certificate": "keyVaultItems",
}


def _table_from_command(command):
    """Return command-prefix fallback table for a command string."""
    for prefix, table in _COMMAND_PREFIX_MAP.items():
        if command.startswith(prefix):
            return table
    return None


def _write_cli_args_entry(log_path, entry):
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def _write_cli_response(db_path, command, result):
    try:
        # Normalise to a list of items.
        items = result if isinstance(result, list) else [result]
        if not items:
            return

        command_table = _table_from_command(command)

        from azext_cirro_azcli_ext.db import (
            init_db,
            write_arm_batch,
            write_batch,
            write_keyvault_batch,
        )

        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = init_db(db_path)

        try:
            # Group items by target table for efficient batch writes.
            arm_items = []
            keyvault_items = []
            generic_buckets = {}

            for item in items:
                table = _classify_item(item, command_table)
                if table is None:
                    continue
                if table == "resources":
                    arm_items.append(item)
                elif table == "keyVaultItems":
                    keyvault_items.append(item)
                else:
                    generic_buckets.setdefault(table, []).append(item)

            if arm_items:
                write_arm_batch(conn, arm_items)
            if keyvault_items:
                write_keyvault_batch(conn, keyvault_items)
            for table, bucket in generic_buckets.items():
                write_batch(conn, table, bucket)
        finally:
            conn.close()
    except Exception:
        pass


def _log_cli_args(cli_ctx, **kwargs):
    """Event handler that logs CLI arguments to a JSON-lines file."""
    try:
        log_path = cli_ctx.config.get("cirro", "log_path", fallback=DEFAULT_LOG_PATH)

        parsed_args = kwargs.get("args")
        parsed_dict = {}
        if parsed_args:
            parsed_dict = {
                k: v
                for k, v in vars(parsed_args).items()
                if not k.startswith("_") and k not in ("func", "command")
            }

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "raw_args": sys.argv[:],
            "command": kwargs.get("command", "unknown"),
            "parsed_args": {k: str(v) for k, v in parsed_dict.items()},
        }

        _write_cli_args_entry(log_path, entry)
    except Exception:
        pass


def _log_cli_response(cli_ctx, **kwargs):
    """Event handler that writes command results to a cirro-compatible SQLite DB."""
    try:
        event_data = kwargs.get("event_data", {})
        result = event_data.get("result")

        if result is None:
            return

        command = "unknown"
        if hasattr(cli_ctx, "data") and cli_ctx.data:
            command = cli_ctx.data.get("command", "unknown") or "unknown"

        top_group = command.split()[0] if command else ""
        if top_group in _LOCAL_ONLY_COMMAND_GROUPS:
            return
        db_path = cli_ctx.config.get("cirro", "db_path", fallback=DEFAULT_DB_PATH)
        _write_cli_response(db_path, command, result)
    except Exception:
        pass


def _classify_item(item, command_table=None):
    """Determine which cirro table an item belongs to.

    Returns the table name or None if the item should be skipped.
    """
    if not isinstance(item, dict):
        return None

    # 1. Check @odata.type (e.g. "#microsoft.graph.user")
    odata_type = item.get("@odata.type", "")
    if odata_type:
        type_suffix = odata_type.rsplit(".", 1)[-1].lower()
        table = _ODATA_TYPE_MAP.get(type_suffix)
        if table:
            return table

    # 2. Check for Key Vault item (id or kid contains .vault.azure.net)
    item_id = item.get("id") or item.get("kid") or ""
    if isinstance(item_id, str) and ".vault.azure.net" in item_id:
        return "keyVaultItems"

    # 3. Check for ARM resource ID pattern
    if isinstance(item_id, str) and (
        "/subscriptions/" in item_id or "/providers/" in item_id
    ):
        return "resources"

    # 3. Fall back to command-derived table.
    if command_table:
        return command_table

    return None


_handler_registered = False


class CirroAzcliExtCommandsLoader(AzCommandsLoader):
    def __init__(self, cli_ctx=None):
        global _handler_registered
        custom_type = CliCommandType(operations_tmpl="azext_cirro_azcli_ext.custom#{}")
        super().__init__(cli_ctx=cli_ctx, custom_command_type=custom_type)
        if cli_ctx and not _handler_registered:
            cli_ctx.register_event(EVENT_INVOKER_POST_PARSE_ARGS, _log_cli_args)
            cli_ctx.register_event(EVENT_INVOKER_FILTER_RESULT, _log_cli_response)
            _handler_registered = True

    def load_command_table(self, args):
        with self.command_group("cirro") as g:
            g.custom_command("status", "status_command")
        return self.command_table

    def load_arguments(self, command):
        with self.argument_context("cirro status") as c:
            c.argument("tail", type=int, help="Number of recent log entries to show.")


COMMAND_LOADER_CLS = CirroAzcliExtCommandsLoader
