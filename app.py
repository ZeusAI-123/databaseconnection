"""
app.py
"""

import streamlit as st
import pandas as pd
import traceback

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DB Explorer",
    page_icon="🔌",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS — dark theme ─────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Sora:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Sora', sans-serif; }

    .stApp { background: #0d0f1a; color: #e2e8f0; }

    section[data-testid="stSidebar"] {
        background: #111427;
        border-right: 1px solid #1e2340;
    }

    .block-container { padding: 2rem 2.5rem; }

    /* Header banner */
    .header {
        background: linear-gradient(135deg, #1a1f3c 0%, #0f1225 100%);
        border: 1px solid #2d3561;
        border-radius: 14px;
        padding: 1.4rem 2rem;
        margin-bottom: 1.8rem;
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    .header h1 {
        font-family: 'Space Mono', monospace;
        font-size: 1.6rem;
        color: #a78bfa;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .header p { color: #64748b; margin: 0; font-size: 0.85rem; }

    /* Status pill */
    .pill-connected {
        display: inline-block;
        background: #064e3b;
        color: #34d399;
        border: 1px solid #065f46;
        border-radius: 999px;
        padding: 2px 12px;
        font-size: 0.75rem;
        font-weight: 600;
        font-family: 'Space Mono', monospace;
    }
    .pill-disconnected {
        display: inline-block;
        background: #1f0f0f;
        color: #f87171;
        border: 1px solid #450a0a;
        border-radius: 999px;
        padding: 2px 12px;
        font-size: 0.75rem;
        font-weight: 600;
        font-family: 'Space Mono', monospace;
    }

    /* Table card */
    .table-card {
        background: #111427;
        border: 1px solid #1e2340;
        border-radius: 10px;
        padding: 0.6rem 1rem;
        margin-bottom: 0.4rem;
        font-family: 'Space Mono', monospace;
        font-size: 0.82rem;
        color: #94a3b8;
        cursor: pointer;
        transition: border-color 0.2s;
    }
    .table-card:hover { border-color: #7c3aed; color: #e2e8f0; }

    /* Section label */
    .section-label {
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: #4b5563;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }

    /* Streamlit widget overrides */
    .stSelectbox > div > div,
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        background: #0d0f1a !important;
        border: 1px solid #1e2340 !important;
        color: #e2e8f0 !important;
        border-radius: 8px !important;
    }
    .stButton > button {
        background: linear-gradient(135deg, #7c3aed, #4f46e5);
        color: white;
        border: none;
        border-radius: 8px;
        font-family: 'Space Mono', monospace;
        font-size: 0.82rem;
        padding: 0.5rem 1.4rem;
        width: 100%;
    }
    .stButton > button:hover { opacity: 0.88; }

    div[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }

    .stTabs [data-baseweb="tab"] {
        font-family: 'Space Mono', monospace;
        font-size: 0.8rem;
        color: #64748b;
    }
    .stTabs [aria-selected="true"] { color: #a78bfa !important; }
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown("""
<div class="header">
    <span style="font-size:2rem;">🔌</span>
    <div>
        <h1>DB Explorer</h1>
        <p>Connect the db</p>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Session state defaults ────────────────────────────────────────────────────
for key in ["conn", "db_type", "tables", "db_obj", "db_catalog", "db_schema"]:
    if key not in st.session_state:
        st.session_state[key] = None

# ── Sidebar — Connection Panel ────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🛰️ Connection")

    db_type = st.selectbox(
        "Database",
        ["Oracle", "SQL Server", "MongoDB", "Databricks"],
        key="db_select"
    )

    st.markdown("---")

    # ── Oracle ──────────────────────────────────────────────────────────────
    if db_type == "Oracle":
        st.markdown('<p class="section-label">Oracle Credentials</p>', unsafe_allow_html=True)
        host     = st.text_input("Host", placeholder="localhost")
        port     = st.number_input("Port", value=1521, step=1)
        service  = st.text_input("Service Name", placeholder="ORCL")
        user     = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("⚡ Connect"):
            try:
                from db_connectors import connect_oracle, get_tables_oracle  # uses oracledb (thin mode)
                conn   = connect_oracle(host, int(port), service, user, password)
                tables = get_tables_oracle(conn)
                st.session_state.conn    = conn
                st.session_state.db_type = "oracle"
                st.session_state.tables  = tables
                st.success(f"Connected! {len(tables)} tables found.")
            except Exception as e:
                st.error(f"Connection failed: {e}")

    # ── SQL Server ───────────────────────────────────────────────────────────
    elif db_type == "SQL Server":
        st.markdown('<p class="section-label">SQL Server Credentials</p>', unsafe_allow_html=True)
        host     = st.text_input("Host", placeholder="localhost or 192.168.x.x")
        port     = st.number_input("Port", value=1433, step=1)
        database = st.text_input("Database", placeholder="master")
        driver   = st.selectbox("ODBC Driver", [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server",
            "SQL Server",
        ])
        trusted  = st.checkbox("Windows Trusted Connection", value=False)
        if not trusted:
            user     = st.text_input("Username")
            password = st.text_input("Password", type="password")
        else:
            user, password = "", ""

        if st.button("⚡ Connect"):
            try:
                from db_connectors import connect_sqlserver, get_tables_sqlserver
                conn   = connect_sqlserver(host, int(port), database, user, password,
                                           driver=driver, trusted=trusted)
                tables = get_tables_sqlserver(conn, database)
                st.session_state.conn    = conn
                st.session_state.db_type = "sqlserver"
                st.session_state.tables  = tables
                st.success(f"Connected! {len(tables)} tables found.")
            except Exception as e:
                st.error(f"Connection failed: {e}")

    # ── MongoDB ──────────────────────────────────────────────────────────────
    elif db_type == "MongoDB":
        st.markdown('<p class="section-label">MongoDB Credentials</p>', unsafe_allow_html=True)
        uri      = st.text_input("URI", placeholder="mongodb://localhost:27017")
        database = st.text_input("Database", placeholder="mydb")

        if st.button("⚡ Connect"):
            try:
                from db_connectors import connect_mongo, get_tables_mongo
                db_obj = connect_mongo(uri, database)
                tables = get_tables_mongo(db_obj)
                st.session_state.conn    = True          # flag only; mongo uses db_obj
                st.session_state.db_obj  = db_obj
                st.session_state.db_type = "mongo"
                st.session_state.tables  = tables
                st.success(f"Connected! {len(tables)} collections found.")
            except Exception as e:
                st.error(f"Connection failed: {e}")

    # ── Databricks ───────────────────────────────────────────────────────────
    elif db_type == "Databricks":
        st.markdown('<p class="section-label">Databricks Credentials</p>', unsafe_allow_html=True)
        hostname  = st.text_input("Server Hostname", placeholder="adb-xxx.azuredatabricks.net")
        http_path = st.text_input("HTTP Path", placeholder="/sql/1.0/warehouses/xxx")
        token     = st.text_input("Access Token", type="password")

        # Step 1 — Connect
        if st.button("⚡ Connect"):
            try:
                from db_connectors import connect_databricks, get_catalogs_databricks
                conn     = connect_databricks(hostname, http_path, token)
                catalogs = get_catalogs_databricks(conn)
                st.session_state.conn          = conn
                st.session_state.db_type       = "databricks"
                st.session_state.db_catalogs   = catalogs
                st.session_state.db_catalog    = None
                st.session_state.db_schemas    = []
                st.session_state.db_schema     = None
                st.session_state.tables        = []
                st.success(f"Connected! {len(catalogs)} catalogs found.")
            except Exception as e:
                st.error(f"Connection failed: {e}")

        # Step 2 — Pick Catalog
        if st.session_state.get("db_catalogs"):
            catalog = st.selectbox("Catalog", st.session_state.db_catalogs, key="sel_catalog")
            if st.button("Load Schemas"):
                try:
                    from db_connectors import get_schemas_databricks
                    schemas = get_schemas_databricks(st.session_state.conn, catalog)
                    st.session_state.db_catalog = catalog
                    st.session_state.db_schemas = schemas
                    st.session_state.db_schema  = None
                    st.session_state.tables     = []
                except Exception as e:
                    st.error(f"Failed to load schemas: {e}")

        # Step 3 — Pick Schema
        if st.session_state.get("db_schemas"):
            schema = st.selectbox("Schema", st.session_state.db_schemas, key="sel_schema")
            if st.button("Load Tables"):
                try:
                    from db_connectors import get_tables_databricks
                    tables = get_tables_databricks(
                        st.session_state.conn,
                        st.session_state.db_catalog,
                        schema
                    )
                    st.session_state.db_schema = schema
                    st.session_state.tables    = tables
                    st.success(f"{len(tables)} tables found.")
                except Exception as e:
                    st.error(f"Failed to load tables: {e}")

    # ── Connection status ────────────────────────────────────────────────────
    st.markdown("---")
    if st.session_state.conn:
        st.markdown('<span class="pill-connected">● CONNECTED</span>', unsafe_allow_html=True)
        if st.button("🔌 Disconnect"):
            for key in ["conn", "db_type", "tables", "db_obj", "db_catalog", "db_schema"]:
                st.session_state[key] = None
            st.rerun()
    else:
        st.markdown('<span class="pill-disconnected">○ NOT CONNECTED</span>', unsafe_allow_html=True)


# ── Main Panel ────────────────────────────────────────────────────────────────
if not st.session_state.conn:
    st.info("👈  Fill in connection details in the sidebar and click **Connect**.")
    st.stop()

tables = st.session_state.tables or []
db_type = st.session_state.db_type

col_left, col_right = st.columns([1, 3], gap="large")

# ── Left — table list ─────────────────────────────────────────────────────────
with col_left:
    st.markdown(f'<p class="section-label">📋 Tables / Collections ({len(tables)})</p>', unsafe_allow_html=True)

    search = st.text_input("🔍 Filter", placeholder="Search table name…", label_visibility="collapsed")
    filtered = [t for t in tables if search.lower() in t.lower()] if search else tables

    selected_table = st.radio(
        "Select a table",
        filtered if filtered else ["(no results)"],
        label_visibility="collapsed",
    )

# ── Right — preview panel ─────────────────────────────────────────────────────
with col_right:
    if not selected_table or selected_table == "(no results)":
        st.info("Select a table on the left to preview its data.")
        st.stop()

    st.markdown(f'<p class="section-label">🗂 Preview — {selected_table}</p>', unsafe_allow_html=True)

    limit = st.slider("Row limit", 10, 500, 100, step=10)

    try:
        if db_type == "oracle":
            from db_connectors import preview_table_oracle
            df = preview_table_oracle(st.session_state.conn, selected_table, limit)

        elif db_type == "sqlserver":
            from db_connectors import preview_table_sqlserver
            df = preview_table_sqlserver(st.session_state.conn, selected_table, limit)

        elif db_type == "mongo":
            from db_connectors import preview_table_mongo
            df = preview_table_mongo(st.session_state.db_obj, selected_table, limit)

        elif db_type == "databricks":
            from db_connectors import preview_table_databricks
            df = preview_table_databricks(
                st.session_state.conn,
                st.session_state.db_catalog,
                st.session_state.db_schema,
                selected_table,
                limit,
            )

        if df.empty:
            st.warning("Table is empty or returned no rows.")
        else:
            tab1, tab2 = st.tabs(["📊 Data", "🧬 Schema"])

            with tab1:
                st.markdown(f"**{len(df)} rows × {len(df.columns)} columns**")
                st.dataframe(df, use_container_width=True, height=420)

            with tab2:
                schema_df = pd.DataFrame({
                    "Column":   df.columns.tolist(),
                    "Dtype":    [str(dt) for dt in df.dtypes],
                    "Non-Null": df.notna().sum().tolist(),
                    "Nulls":    df.isna().sum().tolist(),
                    "Sample":   [str(df[c].dropna().iloc[0]) if df[c].notna().any() else "—" for c in df.columns],
                })
                st.dataframe(schema_df, use_container_width=True, height=420)

    except Exception as e:
        st.error(f"Failed to load table: {e}")
        st.code(traceback.format_exc(), language="python")
