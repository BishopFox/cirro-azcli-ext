import json
import os

from azext_cirro_azcli_ext import DEFAULT_LOG_PATH, DEFAULT_DB_PATH


def status_command(cmd, tail=10):
    """Show the argument log file path and collected resource counts."""
    log_path = cmd.cli_ctx.config.get("cirro", "log_path", fallback=DEFAULT_LOG_PATH)
    db_path = cmd.cli_ctx.config.get("cirro", "db_path", fallback=DEFAULT_DB_PATH)

    result = {
        "args_log": {"path": log_path, "exists": os.path.isfile(log_path)},
        "database": {"path": db_path, "exists": os.path.isfile(db_path)},
    }

    if result["args_log"]["exists"]:
        with open(log_path, "r") as f:
            lines = f.readlines()
        result["args_log"]["total_entries"] = len(lines)
        result["args_log"]["recent_entries"] = []
        for line in lines[-tail:]:
            try:
                result["args_log"]["recent_entries"].append(json.loads(line))
            except json.JSONDecodeError:
                pass

    if result["database"]["exists"]:
        from azext_cirro_azcli_ext.db import init_db, table_counts
        conn = init_db(db_path)
        try:
            result["database"]["table_counts"] = table_counts(conn)
        finally:
            conn.close()

    return result
