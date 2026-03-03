import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & DB CONNECTION ---
st.set_page_config(page_title="Mamanourish Sales Diagnostic", layout="wide")

try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception as e:
    st.error(f"Connection Secret Error: {e}")
    st.stop()

# UTILITIES
def clean_num(val):
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip()
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return round(float(res), 2) if res else 0.0
    except: return 0.0

def parse_date_smart(val):
    if pd.isna(val): return None
    s = str(val).split(" - ")[0].strip()
    if len(s) == 8 and s.isdigit():
        try: return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except: pass
    try: return pd.to_datetime(s).strftime("%Y-%m-%d")
    except: return None

# --- 2. AUTHENTICATION ---
if "authenticated" not in st.session_state:
    st.title("🔐 Mamanourish Sales Portal")
    role_choice = st.selectbox("Select Role", ["Select...", "Admin (Full Access)", "Viewer (View Only)"])
    pw = st.text_input("Password", type="password")
    if st.button("Login"):
        if role_choice == "Admin (Full Access)" and pw == "mamaadmin2026":
            st.session_state["authenticated"], st.session_state["role"] = True, "admin"
            st.rerun()
        elif role_choice == "Viewer (View Only)" and pw == "mamaview2026":
            st.session_state["authenticated"], st.session_state["role"] = True, "viewer"
            st.rerun()
    st.stop()

# --- 3. FETCH DATA (NO CACHING FOR TROUBLESHOOTING) ---
def get_data_fresh(table):
    """Fetches data directly with no caching to ensure we see the latest."""
    try:
        res = supabase.table(table).select("*").execute()
        return pd.DataFrame(res.data)
    except Exception as e:
        st.sidebar.error(f"DB Fetch Error ({table}): {e}")
        return pd.DataFrame()

# Global Data Loads
master_skus = get_data_fresh("master_skus")
master_chans = get_data_fresh("master_channels")
item_map_df = get_data_fresh("item_map")
history_df = get_data_fresh("sales")

# --- 4. TABS ---
tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Config"])

with tabs[0]: # ANALYTICS
    if history_df.empty:
        st.warning("Database currently empty. Upload data in the next tab.")
    else:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        
        # Filters
        st.subheader("Filters")
        c1, c2, c3 = st.columns(3)
        with c1: 
            p_range = st.selectbox("Range", ["All Time", "Last 7 Days", "Last 30 Days"])
            if p_range == "Last 7 Days": start_d = datetime.now().date() - timedelta(days=7)
            elif p_range == "Last 30 Days": start_d = datetime.now().date() - timedelta(days=30)
            else: start_d = history_df['date_dt'].min().date()
        
        with c2: sel_chans = st.multiselect("Channels", history_df['channel'].unique(), default=history_df['channel'].unique())
        with c3: sel_items = st.multiselect("Products", history_df['item_name'].unique(), default=history_df['item_name'].unique())

        # Filtering Logic
        f_df = history_df[
            (history_df['date_dt'].dt.date >= start_d) & 
            (history_df['channel'].isin(sel_chans)) & 
            (history_df['item_name'].isin(sel_items))
        ].copy()

        if not f_df.empty:
            st.metric("Filtered Revenue", f"₹{f_df['revenue'].sum():,.2f}")
            fig = px.bar(f_df.groupby(['date', 'channel'])['revenue'].sum().reset_index(), x='date', y='revenue', color='channel')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("No data matches current filters. Check the Debug Console below.")

with tabs[1]: # UPLOAD
    st.subheader("Upload New Data")
    chan = st.selectbox("Target Channel", master_chans['name'] if not master_chans.empty else [])
    up = st.file_uploader("Upload File", type=["csv", "xlsx"])
    
    if up and chan:
        df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
        st.write("Preview:", df.head(3))
        
        col_p = st.selectbox("Product Column", df.columns)
        col_q = st.selectbox("Qty Column", df.columns)
        col_r = st.selectbox("Revenue Column", df.columns)
        col_d = st.selectbox("Date Column (Optional)", ["None"] + list(df.columns))
        m_date = st.date_input("Fallback Date")

        if st.button("🚀 Push to Cloud"):
            rows = []
            for _, r in df.iterrows():
                dt = parse_date_smart(r[col_d]) if col_d != "None" else str(m_date)
                rows.append({
                    "date": dt, "channel": chan, "item_name": str(r[col_p]),
                    "qty_sold": clean_num(r[col_q]), "revenue": clean_num(r[col_r])
                })
            
            # Batch Upload
            res = supabase.table("sales").insert(rows).execute()
            if res.data:
                st.success(f"Successfully inserted {len(res.data)} rows into Supabase!")
                st.rerun()
            else:
                st.error("Upload failed. No data returned from Supabase.")

# --- 5. THE DEBUG CONSOLE (STAYS AT BOTTOM) ---
st.divider()
st.subheader("🔍 Troubleshooting / Debug Console")
d1, d2, d3 = st.columns(3)

with d1:
    st.write("**Database Status**")
    st.write(f"Total Rows in `sales`: {len(history_df)}")
    if not history_df.empty:
        st.write(f"Latest Date Found: {history_df['date'].max()}")

with d2:
    st.write("**Permission Check**")
    # This checks if RLS is blocking the read
    try:
        check = supabase.table("sales").select("count", count="exact").execute()
        st.write(f"Supabase Count API says: {check.count} rows")
    except Exception as e:
        st.error(f"RLS/Permission Error: {e}")

with d3:
    st.write("**Filter Diagnostics**")
    if 'start_d' in locals():
        st.write(f"Current Start Date Filter: {start_d}")
        st.write(f"Rows matching Date: {len(history_df[history_df['date_dt'].dt.date >= start_d]) if not history_df.empty else 0}")

if st.button("🗑 Force Clear All App Cache"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()
