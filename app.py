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
    """Strips currency, commas, and handles Shopify/BigBasket number formats."""
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return round(float(res), 2) if res else 0.0
    except: return 0.0

def parse_date_smart(val):
    """Handles various date formats including Big Basket ranges."""
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

# Load context
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

        mask = (history_df['date_dt'].dt.date >= start_d) & \
               (history_df['channel'].isin(sel_chans)) & \
               (history_df['item_name'].isin(sel_items))
        f_df = history_df[mask].copy()

        if not f_df.empty:
            m1, m2 = st.columns(2)
            m1.metric("Total Revenue", f"₹{f_df['revenue'].sum():,.2f}")
            m2.metric("Total Units", f"{f_df['qty_sold'].sum():,.0f}")
            
            chart_data = f_df.groupby(['date', 'channel'])['revenue'].sum().reset_index().sort_values('date')
            fig = px.bar(chart_data, x='date', y='revenue', color='channel', barmode='stack', height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("View Data Table"):
                st.dataframe(f_df.sort_values('date', ascending=False).drop(columns=['date_dt']), hide_index=True)
        else:
            st.warning("No data matches the selected filters.")

if role == "admin":
    with tabs[1]: # SMART UPLOAD
        st.subheader("Upload Sales Report")
        
        u1, u2 = st.columns([1, 2])
        with u1:
            sel_ch = st.selectbox("Select Target Channel", master_chans['name'].tolist())
            up = st.file_uploader("Upload CSV (Shopify, Big Basket, etc.)", type=["csv"])
        
        if up and sel_ch:
            raw_df = pd.read_csv(up)
            cols = raw_df.columns.tolist()
            
            # --- AUTO HEADER DETECTION ---
            def find_col(possibilities, default_idx=0):
                for p in possibilities:
                    if p in cols: return cols.index(p)
                return default_idx

            st.markdown("---")
            st.markdown("#### ⚙️ Data Mapping & Cleaning")
            mc1, mc2, mc3 = st.columns(3)
            # Detect Shopify: "Product title", "Net items sold", "Total sales"
            # Detect Big Basket: "sku_description", "total_quantity", "total_sales"
            p_idx = find_col(["Product title", "sku_description", "Item Name"])
            q_idx = find_col(["Net items sold", "total_quantity", "Qty"])
            r_idx = find_col(["Total sales", "total_sales", "Revenue", "Net sales"])
            d_idx = find_col(["date_range", "Day", "Date"], -1)

            p_col = mc1.selectbox("Product Name Column", cols, index=p_idx)
            q_col = mc2.selectbox("Quantity Column", cols, index=q_idx)
            r_col = mc2.selectbox("Revenue Column", cols, index=r_idx)
            d_col = mc3.selectbox("Date Column", ["None"] + cols, index=d_idx + 1)
            man_date = mc3.date_input("Fallback Date (if Date Col is 'None')")

            # --- ITEM MAPPING ---
            st.markdown("#### 🛠 Master SKU Mapping")
            unique_raw = raw_df[p_col].unique()
            sku_map = {}
            masters = master_skus['name'].tolist()
            
            for raw_name in unique_raw:
                # Skip summary rows often found in Shopify exports
                if str(raw_name).lower() in ["total", "grand total"]: continue
                ex = item_map_df[item_map_df['raw_name'] == raw_name]
                d_idx = masters.index(ex['master_name'].iloc[0]) if not ex.empty and ex['master_name'].iloc[0] in masters else 0
                sku_map[raw_name] = st.selectbox(f"Map: {raw_name}", masters, index=d_idx)

            # --- PRE-SYNC AGGREGATION ---
            temp_rows = []
            for _, row in raw_df.iterrows():
                if str(row[p_col]).lower() in ["total", "grand total"]: continue
                dt = parse_date_smart(row[d_col]) if d_col != "None" else str(man_date)
                if dt:
                    temp_rows.append({
                        "date": dt, "channel": sel_ch, "item_name": sku_map[row[p_col]],
                        "qty_sold": clean_num(row[q_col]), "revenue": clean_num(row[r_col])
                    })
            
            if temp_rows:
                preview_df = pd.DataFrame(temp_rows).groupby(['date', 'channel', 'item_name']).agg({
                    'qty_sold': 'sum', 'revenue': 'sum'
                }).reset_index()
                
                st.markdown("#### 👀 Data Preview (Calculated Totals)")
                st.dataframe(preview_df, hide_index=True)
                
                st.warning(f"Total Revenue to be synced: ₹{preview_df['revenue'].sum():,.2f}")
                
                if st.button("🚀 Push to Cloud"):
                    with st.spinner("Syncing..."):
                        # Save new mappings
                        for raw, master in sku_map.items():
                            supabase.table("item_map").upsert({"raw_name": raw, "master_name": master}).execute()
                        
                        # Sync data using UPSERT
                        res = supabase.table("sales").upsert(
                            preview_df.to_dict(orient='records'),
                            on_conflict="date,channel,item_name"
                        ).execute()
                        
                        if res.data:
                            st.success(f"Successfully synced {len(res.data)} unique records!")
                            st.rerun()

    with tabs[2]: # CONFIG
        st.subheader("Manage Master SKU & Channel Lists")
        sc1, sc2 = st.columns(2)
        with sc1:
            st.markdown("#### 📦 Master SKUs")
            n_sku = st.text_input("New SKU Name")
            if st.button("Add SKU") and n_sku:
                supabase.table("master_skus").insert({"name": n_sku.strip()}).execute(); st.rerun()
            st.dataframe(master_skus, hide_index=True)
        with sc2:
            st.markdown("#### 🏢 Sales Channels")
            n_chan = st.text_input("New Channel Name")
            if st.button("Add Channel") and n_chan:
                supabase.table("master_channels").insert({"name": n_chan.strip()}).execute(); st.rerun()
            st.dataframe(master_chans, hide_index=True)
