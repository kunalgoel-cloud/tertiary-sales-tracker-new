import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & DB CONNECTION ---
st.set_page_config(page_title="Mamanourish Sales Tracker", layout="wide")

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
    # Removes currency symbols and commas, handles brackets for negative numbers
    s = str(val).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return round(float(res), 2) if res else 0.0
    except: return 0.0

def parse_date_smart(val):
    if pd.isna(val) or str(val).lower() == "none": return None
    # Handles Big Basket "20260219 - 20260219" format
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

# --- 4. DATA FETCHING (No Cache for Accuracy) ---
def get_data_fresh(table, default_cols):
    try:
        res = supabase.table(table).select("*").execute()
        df = pd.DataFrame(res.data)
        return df if not df.empty else pd.DataFrame(columns=default_cols)
    except: return pd.DataFrame(columns=default_cols)

# Load context lists
master_skus = get_data_fresh("master_skus", ["name"])
master_chans = get_data_fresh("master_channels", ["name"])
item_map_df = get_data_fresh("item_map", ["raw_name", "master_name"])
history_df = get_data_fresh("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])

# --- 5. TABS ---
tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Config"]) if role == "admin" else st.tabs(["📊 Analytics"])

with tabs[0]: # ANALYTICS
    if history_df.empty:
        st.info("No data found. Please upload records in the 'Smart Upload' tab.")
    else:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        
        # FILTERS
        st.subheader("Dashboard Filters")
        f1, f2, f3 = st.columns(3)
        with f1:
            p_range = st.radio("Timeframe", ["All Time", "Last 7 Days", "Last 30 Days"], horizontal=True)
            if p_range == "Last 7 Days": start_d = datetime.now().date() - timedelta(days=7)
            elif p_range == "Last 30 Days": start_d = datetime.now().date() - timedelta(days=30)
            else: start_d = history_df['date_dt'].min().date()
        
        with f2:
            avail_chans = sorted(history_df['channel'].unique().tolist())
            sel_chans = st.multiselect("Channels", avail_chans, default=avail_chans)
        
        with f3:
            avail_items = sorted(history_df['item_name'].unique().tolist())
            sel_items = st.multiselect("Master SKUs", avail_items, default=avail_items)

        # Filtering Logic
        mask = (history_df['date_dt'].dt.date >= start_d) & \
               (history_df['channel'].isin(sel_chans)) & \
               (history_df['item_name'].isin(sel_items))
        f_df = history_df[mask].copy()

        if not f_df.empty:
            st.metric("Total Filtered Revenue", f"₹{f_df['revenue'].sum():,.2f}")
            
            # Aggregate for cleaner charting
            chart_data = f_df.groupby(['date', 'channel'])['revenue'].sum().reset_index().sort_values('date')
            fig = px.bar(chart_data, x='date', y='revenue', color='channel', barmode='stack', height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("View Filtered Raw Data"):
                st.dataframe(f_df.sort_values('date', ascending=False).drop(columns=['date_dt']), hide_index=True)
        else:
            st.warning("No data matches the current filters.")

if role == "admin":
    with tabs[1]: # SMART UPLOAD WITH DEDUPLICATION & AGGREGATION
        st.subheader("Upload Sales Report")
        
        c1, c2 = st.columns([1, 2])
        with c1:
            sel_ch = st.selectbox("Channel", master_chans['name'].tolist())
            up = st.file_uploader("Upload CSV", type=["csv"])
        
        if up and sel_ch:
            raw_df = pd.read_csv(up)
            cols = raw_df.columns.tolist()
            
            st.markdown("---")
            st.write("🔧 **Column Selection**")
            mc1, mc2, mc3 = st.columns(3)
            # Auto-detection for Big Basket
            p_col = mc1.selectbox("Product Col", cols, index=cols.index("sku_description") if "sku_description" in cols else 0)
            q_col = mc2.selectbox("Quantity Col", cols, index=cols.index("total_quantity") if "total_quantity" in cols else 0)
            r_col = mc2.selectbox("Revenue Col", cols, index=cols.index("total_sales") if "total_sales" in cols else 0)
            d_col = mc3.selectbox("Date Col", ["None"] + cols, index=(cols.index("date_range")+1 if "date_range" in cols else 0))
            man_date = mc3.date_input("Manual Date (if Date Col is None)")

            # ITEM MAPPING TOOL
            st.markdown("### 🛠 Map Items to Master SKUs")
            unique_raw_names = raw_df[p_col].unique()
            sku_mapping = {}
            masters = master_skus['name'].tolist()
            
            for raw_name in unique_raw_names:
                existing = item_map_df[item_map_df['raw_name'] == raw_name]
                default_idx = masters.index(existing['master_name'].iloc[0]) if not existing.empty and existing['master_name'].iloc[0] in masters else 0
                sku_mapping[raw_name] = st.selectbox(f"Map: {raw_name}", masters, index=default_idx)

            if st.button("🚀 Aggregated Sync to Cloud"):
                with st.spinner("Processing & Overwriting Duplicates..."):
                    # 1. Update Mapping Database
                    for raw, master in sku_mapping.items():
                        supabase.table("item_map").upsert({"raw_name": raw, "master_name": master}).execute()
                    
                    # 2. Process File Rows
                    temp_rows = []
                    for _, row in raw_df.iterrows():
                        final_date = parse_date_smart(row[d_col]) if d_col != "None" else str(man_date)
                        if final_date:
                            temp_rows.append({
                                "date": final_date,
                                "channel": sel_ch,
                                "item_name": sku_mapping[row[p_col]],
                                "qty_sold": clean_num(row[q_col]),
                                "revenue": clean_num(row[r_col])
                            })
                    
                    if temp_rows:
                        # 3. AGGREGATION (Combine city-level rows into one Master SKU total)
                        final_upload_df = pd.DataFrame(temp_rows).groupby(['date', 'channel', 'item_name']).agg({
                            'qty_sold': 'sum',
                            'revenue': 'sum'
                        }).reset_index()
                        
                        # 4. UPSERT (Prevent duplicates using unique SQL constraint)
                        res = supabase.table("sales").upsert(
                            final_upload_df.to_dict(orient='records'),
                            on_conflict="date,channel,item_name"
                        ).execute()
                        
                        if res.data:
                            st.success(f"Successfully synced {len(res.data)} daily SKU totals!")
                            st.cache_data.clear()
                            st.rerun()

    with tabs[2]: # CONFIG
        st.subheader("Manage Master Lists")
        sc1, sc2 = st.columns(2)
        with sc1:
            st.markdown("#### 📦 Master SKUs")
            new_sku = st.text_input("New SKU Name")
            if st.button("Add SKU") and new_sku:
                supabase.table("master_skus").insert({"name": new_sku.strip()}).execute()
                st.rerun()
            st.dataframe(master_skus, hide_index=True)
            
        with sc2:
            st.markdown("#### 🏢 Channels")
            new_chan = st.text_input("New Channel Name")
            if st.button("Add Channel") and new_chan:
                supabase.table("master_channels").insert({"name": new_chan.strip()}).execute()
                st.rerun()
            st.dataframe(master_chans, hide_index=True)
