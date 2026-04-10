import json
from urllib.parse import urlparse

import apsw

GENERIC_TABLES = [
    "applications",
    "administrativeUnits",
    "conditionalAccessPolicies",
    "devices",
    "directoryRoles",
    "eligibleArmRBAC",
    "eligibleRoleAssignments",
    "groups",
    "managementGroupEntities",
    "namedLocations",
    "organization",
    "oauth2PermissionGrants",
    "policies",
    "roleAssignments",
    "servicePrincipals",
    "subscriptions",
    "tenants",
    "users",
]

ALL_TABLES = GENERIC_TABLES + ["keyVaultItems", "resources"]

_KEYVAULT_resource_type_MAP = {
    "secrets": "Secret",
    "keys": "Key",
    "certificates": "Certificate",
}


def init_db(path):
    """Create and return an apsw connection with the cirro-compatible schema."""
    conn = apsw.Connection(path)
    conn.setbusytimeout(30000)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA automatic_index=true")
    conn.execute("PRAGMA temp_store=MEMORY")

    for table in GENERIC_TABLES:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS [{table}] (id TEXT PRIMARY KEY, data TEXT)"
        )

    conn.execute(
        "CREATE TABLE IF NOT EXISTS resources "
        "(id TEXT PRIMARY KEY, sub_id TEXT, rg_id TEXT, resource_type TEXT, data TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS keyVaultItems "
        "(id TEXT PRIMARY KEY, resource_type TEXT, data TEXT)"
    )
    return conn


def _classify_keyvault_resource_type(item_id):
    """Classify a Key Vault item id into a canonical item type."""
    if not item_id:
        return None

    parse_target = item_id if "://" in item_id else f"https://{item_id}"
    parsed = urlparse(parse_target)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return None

    base_type = _KEYVAULT_resource_type_MAP.get(parts[0].lower())
    if base_type is None:
        return None
    if len(parts) == 2:
        return base_type
    if len(parts) == 3:
        return f"{base_type}value"
    return None


def _keyvault_item_row(item):
    item_id = (item.get("id") or item.get("kid") or "").lower()
    return (
        item_id,
        _classify_keyvault_resource_type(item_id),
        json.dumps(item),
    )


def _parse_arm_id(arm_id):
    """Extract sub_id and rg_id from an ARM resource ID."""
    parts = arm_id.lower().split("/")
    sub_id = ""
    rg_id = ""
    for i, part in enumerate(parts):
        if part == "subscriptions" and i + 1 < len(parts):
            sub_id = parts[i + 1]
        elif part == "resourcegroups" and i + 1 < len(parts):
            rg_id = parts[i + 1]
    return sub_id, rg_id


def write_arm_resource(conn, item):
    """Write a single ARM resource to the resources table."""
    raw_id = item.get("id", "")
    resource_type = item.get("type", "").lower()
    sub_id, rg_id = _parse_arm_id(raw_id)
    data = json.dumps(item).encode("utf-8")
    conn.execute(
        "INSERT OR REPLACE INTO resources (id, sub_id, rg_id, resource_type, data) "
        "VALUES (?, ?, ?, ?, ?)",
        (raw_id.lower(), sub_id, rg_id, resource_type, data),
    )


def write_generic(conn, table, item):
    """Write a single item to a generic (id, data) table."""
    item_id = item.get("id", "")
    data = json.dumps(item).encode("utf-8")
    conn.execute(
        f"INSERT OR REPLACE INTO [{table}] (id, data) VALUES (?, ?)",
        (item_id.lower() if item_id else item_id, data),
    )


def write_batch(conn, table, items):
    """Write multiple items to a generic table in a transaction."""
    conn.execute("BEGIN")
    conn.executemany(
        f"INSERT OR REPLACE INTO [{table}] (id, data) VALUES (?, ?)",
        (
            (item.get("id", "").lower(), json.dumps(item).encode("utf-8"))
            for item in items
        ),
    )
    conn.execute("COMMIT")


def write_arm_batch(conn, items):
    """Write multiple ARM resources in a transaction."""
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT OR REPLACE INTO resources (id, sub_id, rg_id, resource_type, data) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            (
                raw_id.lower(),
                sub_id,
                rg_id,
                item.get("type", "").lower(),
                json.dumps(item),
            )
            for item in items
            for raw_id in [item.get("id", "")]
            for sub_id, rg_id in [_parse_arm_id(raw_id)]
        ),
    )
    conn.execute("COMMIT")


def write_keyvault_batch(conn, items):
    """Write Key Vault items in a transaction. Uses 'kid' as id for keys."""
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT OR REPLACE INTO keyVaultItems (id, resource_type, data) VALUES (?, ?, ?)",
        (_keyvault_item_row(item) for item in items),
    )
    conn.execute("COMMIT")


def table_counts(conn):
    """Return a dict of {table_name: row_count} for all tables."""
    counts = {}
    for table in ALL_TABLES:
        try:
            row = list(conn.execute(f"SELECT COUNT(*) FROM [{table}]"))
            counts[table] = row[0][0]
        except apsw.Error:
            counts[table] = 0
    return counts
