"""
db_connectors.py
Zeus DB Explorer — connector logic for Oracle, SQL Server, MongoDB, Databricks
"""

import pandas as pd
from typing import Tuple, List, Optional


# ─────────────────────────────────────────────
#  ORACLE
# ─────────────────────────────────────────────
def connect_oracle(host: str, port: int, service: str, user: str, password: str):
    """Returns an oracledb connection (thin mode — no Oracle Client needed)."""
    import oracledb
    conn = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service}",
    )
    return conn


def get_tables_oracle(conn) -> List[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
    return [row[0] for row in cursor.fetchall()]


def preview_table_oracle(conn, table: str, limit: int = 100) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} FETCH FIRST {limit} ROWS ONLY")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame([list(r) for r in rows], columns=cols)


# ─────────────────────────────────────────────
#  SQL SERVER
# ─────────────────────────────────────────────
def connect_sqlserver(host: str, port: int, database: str, user: str, password: str,
                      driver: str = "ODBC Driver 17 for SQL Server", trusted: bool = False):
    """Returns a pyodbc connection. Supports both SQL auth and Windows trusted auth."""
    import pyodbc
    server = f"{host.strip()},{port}" if port else host.strip()
    database = database.strip()

    if trusted:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
            f"Connection Timeout=30;"
            f"MARS_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Encrypt=no;"
            f"Connection Timeout=30;"
            f"MARS_Connection=yes;"
        )

    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        raise Exception(f"SQL Server connection failed: {str(e)}")


def get_tables_sqlserver(conn, database: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME"
    )
    return [row[0] for row in cursor.fetchall()]


def preview_table_sqlserver(conn, table: str, limit: int = 100) -> pd.DataFrame:
    import pandas as pd
    try:
        # Most reliable method — pandas handles row unpacking automatically
        df = pd.read_sql(f"SELECT TOP {limit} * FROM {table}", conn)
        return df
    except Exception:
        # Fallback: manual fetch with explicit tuple conversion
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP {limit} * FROM {table}")
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        data = [tuple(row) for row in rows]
        return pd.DataFrame(data, columns=cols)


# ─────────────────────────────────────────────
#  MONGODB
# ─────────────────────────────────────────────
def connect_mongo(uri: str, database: str):
    """Returns a pymongo Database object."""
    from pymongo import MongoClient
    from urllib.parse import quote_plus, urlparse, urlunparse
    uri = uri.strip().strip('"').strip("'")
    database = database.strip()

    # Auto-encode special characters in username & password (RFC 3986)
    try:
        parsed = urlparse(uri)
        if parsed.username and parsed.password:
            safe_user = quote_plus(parsed.username)
            safe_pass = quote_plus(parsed.password)
            netloc = f"{safe_user}:{safe_pass}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            uri = urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass  # if parsing fails, try original uri as-is

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")          # validate connection
    return client[database]


def get_tables_mongo(db) -> List[str]:
    """Collections act as 'tables' in MongoDB."""
    return sorted(db.list_collection_names())


def preview_table_mongo(db, collection: str, limit: int = 100) -> pd.DataFrame:
    docs = list(db[collection].find({}, {"_id": 0}).limit(limit))
    return pd.DataFrame(docs) if docs else pd.DataFrame()


# ─────────────────────────────────────────────
#  DATABRICKS
# ─────────────────────────────────────────────
def connect_databricks(server_hostname: str, http_path: str, token: str):
    """Returns a Databricks SQL connector connection."""
    from databricks import sql as dbsql
    conn = dbsql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )
    return conn


def get_catalogs_databricks(conn) -> List[str]:
    """List all Unity Catalog catalogs."""
    cursor = conn.cursor()
    cursor.execute("SHOW CATALOGS")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def get_schemas_databricks(conn, catalog: str) -> List[str]:
    """List all schemas inside a catalog."""
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS IN {catalog}")
    rows = cursor.fetchall()
    return [row[0] for row in rows]


def get_tables_databricks(conn, catalog: str, schema: str) -> List[str]:
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
    rows = cursor.fetchall()
    col_names = [d[0].lower() for d in cursor.description]
    # Try to find tableName column, fallback to last column
    if "tablename" in col_names:
        idx = col_names.index("tablename")
    elif "name" in col_names:
        idx = col_names.index("name")
    else:
        idx = len(col_names) - 1
    return [str(row[idx]) for row in rows if row]


def preview_table_databricks(conn, catalog: str, schema: str, table: str, limit: int = 100) -> pd.DataFrame:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}")
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    # Convert each row to a plain list to avoid shape mismatch with Row objects
    data = [list(row) for row in rows]
    return pd.DataFrame(data, columns=cols)
