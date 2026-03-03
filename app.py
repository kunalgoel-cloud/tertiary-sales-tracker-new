import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & DB CONNECTION ---
st.set_page_config(page_title="Mamanourish Executive Tracker", layout="wide")

try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception:
    st.error("Missing Supabase Secrets! Please check your Streamlit Cloud Settings.")
    st.stop()

# --- 2. UTILITY FUNCTIONS ---
def clean_num(val):
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return round(float(res), 2) if res else 0.0
    except: return 0.0

def parse_date_smart(val):
    if pd.isna(val) or str(val).lower() in ["none", "nan"]: return None
    s = str(val).split(" - ")[0].strip()
    if len(s) == 8 and s.isdigit():
        try: return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except: pass
    try: return pd.to_datetime(s).strftime("%Y-%m-%d")
    except: return None

# --- 3. AUTHENTICATION ---
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
        else: st.error("Invalid credentials.")
    st.stop()

role = st.session_state["role"]

# --- 4. DATA FETCHING ---
def get_data_fresh(table, default_cols):
    try:
        res = supabase.table(table).select("*").execute()
        df = pd.DataFrame(res.data)
        return df if not df.empty else pd.DataFrame(columns=default_cols)
    except: return pd.DataFrame(columns=default_cols)

master_skus = get_data_fresh("master_skus", ["name"])
master_chans = get_data_fresh("master_channels", ["name"])
item_map_df = get_data_fresh("item_map", ["raw_name", "master_name"])
history_df = get_data_fresh("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])

# --- 5. TABS ---
tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Config"]) if role == "admin" else st.tabs(["📊 Analytics"])

with tabs[0]: # ANALYTICS
    if history_df.empty:
        st.info("No data found. Upload records in 'Smart Upload' to begin.")
    else:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        st.subheader("Dashboard Filters")
        f1, f2, f3 = st.columns(3)
        with f1:
            p_range = st.radio("Timeframe", ["All Time", "Last 7 Days", "Last 30 Days"], horizontal=True)
            if p_range == "Last 7 Days": start_d = datetime.now().date() - timedelta(days=7)
            elif p_range == "Last 30 Days": start_d = datetime.now().date() - timedelta(days=30)
            else: start_d = history_df['date_dt'].min().date()
        with f2:
            sel_chans = st.multiselect("Channels", sorted(history_df['channel'].unique()), default=history_df['channel'].unique())
        with f3:
            sel_items = st.multiselect("Master SKUs", sorted(history_df['item_name'].unique()), default=history_df['item_name'].unique())

        f_df = history_df[(history_df['date_dt'].dt.date >= start_d) & (history_df['channel'].isin(sel_chans)) & (history_df['item_name'].isin(sel_items))].copy()
        if not f_df.empty:
            st.metric("Total Revenue", f"₹{f_df['revenue'].sum():,.2f}")
            chart_data = f_df.groupby(['date', 'channel'])['revenue'].sum().reset_index().sort_values('date')
            st.plotly_chart(px.bar(chart_data, x='date', y='revenue', color='channel', barmode='stack', height=500), use_container_width=True)
        else: st.warning("No data matches selected filters.")

if role == "admin":
    with tabs[1]: # SMART UPLOAD
        st.subheader("Upload Sales Report")
        u1, u2 = st.columns([1, 2])
        with u1:
            sel_ch = st.selectbox("Select Target Channel", master_chans['name'].tolist())
            up = st.file_uploader("Upload CSV", type=["csv"])
        
        if up and sel_ch:
            raw_df = pd.read_csv(up)
            cols = raw_df.columns.tolist()
            st.markdown("---")
            mc1, mc2, mc3 = st.columns(3)
            p_idx = next((i for i, c in enumerate(cols) if c in ["Product title", "sku_description"]), 0)
            q_idx = next((i for i, c in enumerate(cols) if c in ["Net items sold", "total_quantity"]), 0)
            r_idx = next((i for i, c in enumerate(cols) if c in ["Total sales", "total_sales"]), 0)
            
            p_col = mc1.selectbox("Product Col", cols, index=p_idx)
            q_col = mc2.selectbox("Qty Col", cols, index=q_idx)
            r_col = mc2.selectbox("Revenue Col", cols, index=r_idx)
            d_col = mc3.selectbox("Date Col", ["None"] + cols)
            man_date = mc3.date_input("Manual Date")

            st.markdown("#### 🛠 Mapping & Preview")
            sku_map = {name: st.selectbox(f"Map: {name}", master_skus['name'].tolist(), index=0) for name in raw_df[p_col].unique() if str(name).lower() not in ["total", "grand total"]}
            
            temp_rows = []
            for _, row in raw_df.iterrows():
                if str(row[p_col]).lower() in ["total", "grand total"]: continue
                dt = parse_date_smart(row[d_col]) if d_col != "None" else str(man_date)
                if dt: temp_rows.append({"date": dt, "channel": sel_ch, "item_name": sku_map[row[p_col]], "qty_sold": clean_num(row[q_col]), "revenue": clean_num(row[r_col])})
            
            if temp_rows:
                preview_df = pd.DataFrame(temp_rows).groupby(['date', 'channel', 'item_name']).agg({'qty_sold':'sum', 'revenue':'sum'}).reset_index()
                st.dataframe(preview_df, hide_index=True)
                if st.button("🚀 Push to Cloud"):
                    for raw, master in sku_map.items(): supabase.table("item_map").upsert({"raw_name": raw, "master_name": master}).execute()
                    supabase.table("sales").upsert(preview_df.to_dict(orient='records'), on_conflict="date,channel,item_name").execute()
                    st.success("Sync Complete!"); st.rerun()

    with tabs[2]: # CONFIG & DANGER ZONE
        st.subheader("Configuration")
        sc1, sc2 = st.columns(2)
        with sc1:
            st.write("**Master SKUs**")
            n_sku = st.text_input("New SKU")
            if st.button("Add SKU") and n_sku: supabase.table("master_skus").insert({"name": n_sku}).execute(); st.rerun()
            st.dataframe(master_skus, hide_index=True)
        with sc2:
            st.write("**Channels**")
            n_ch = st.text_input("New Channel")
            if st.button("Add Channel") and n_ch: supabase.table("master_channels").insert({"name": n_ch}).execute(); st.rerun()
            st.dataframe(master_chans, hide_index=True)
        
        st.markdown("---")
        st.subheader("🚨 Danger Zone")
        st.error("Actions here are permanent and cannot be undone.")
        
        col_wipe, col_all = st.columns(2)
        with col_wipe:
            wipe_ch = st.selectbox("Wipe Channel Data", ["Select..."] + master_chans['name'].tolist())
            if st.button("🗑️ Wipe Channel") and wipe_ch != "Select...":
                supabase.table("sales").delete().eq("channel", wipe_ch).execute()
                st.success(f"Cleared all data for {wipe_ch}"); st.rerun()
        
        with col_all:
            st.write("Full Database Reset")
            if st.button("💀 DELETE ALL SALES DATA"):
                # Safety check via text input could be added here
                supabase.table("sales").delete().neq("id", -1).execute() # Deletes everything
                st.success("Database cleared successfully."); st.rerun()
