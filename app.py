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

# DATA CLEANING LOGIC
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
        else: st.error("Invalid credentials.")
    st.stop()

role = st.session_state["role"]

@st.cache_data(ttl=0)
def get_data_safe(table, default_cols):
    try:
        res = supabase.table(table).select("*").execute()
        df = pd.DataFrame(res.data)
        return df if not df.empty else pd.DataFrame(columns=default_cols)
    except: return pd.DataFrame(columns=default_cols)

# Load context lists
master_skus = get_data_safe("master_skus", ["name"])
master_chans = get_data_safe("master_channels", ["name"])
item_map_df = get_data_safe("item_map", ["raw_name", "master_name"])

# --- 3. SIDEBAR (CORRECTION TOOLS) ---
with st.sidebar:
    st.header(f"👤 {role.upper()}")
    if role == "admin":
        st.divider()
        with st.expander("🛠 Data Correction Tool"):
            del_date = st.date_input("Date to Wipe")
            del_chan = st.selectbox("Channel to Wipe", ["Select..."] + master_chans['name'].tolist())
            if st.button("Clear Records"):
                if del_chan != "Select...":
                    supabase.table("sales").delete().eq("date", str(del_date)).eq("channel", del_chan).execute()
                    st.cache_data.clear() 
                    st.success(f"Deleted {del_chan} for {del_date}"); st.rerun()
    if st.button("Logout"):
        del st.session_state["authenticated"]; st.rerun()

# --- 4. TABS ---
tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Config"]) if role == "admin" else st.tabs(["📊 Analytics"])

with tabs[0]: # ANALYTICS
    history_df = get_data_safe("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])
    if history_df.empty:
        st.info("No data found in database. Please upload data in the 'Smart Upload' tab.")
    else:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        
        # Header Metrics Choice
        v1, v2 = st.columns([2, 1])
        with v1: metric_choice = st.radio("Display By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
        with v2: show_labels = st.checkbox("Show Data Labels", value=True)
        target = "revenue" if "Revenue" in metric_choice else "qty_sold"
        
        # --- RESTORED FILTERS ---
        st.subheader("Filters")
        
        # 1. Date Filter
        preset = st.radio("Date Range:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time"], horizontal=True, index=3)
        end_d = datetime.now().date()
        if preset == "Last 7 Days": start_d = end_d - timedelta(days=6)
        elif preset == "Last 30 Days": start_d = end_d - timedelta(days=29)
        elif preset == "Month to Date": start_d = end_d.replace(day=1)
        else: start_d = history_df['date_dt'].min().date()

        date_mask = (history_df['date_dt'].dt.date >= start_d) & (history_df['date_dt'].dt.date <= end_d)
        range_df = history_df[date_mask].copy()

        # 2. Channel & Product Filters
        f1, f2 = st.columns(2)
        avail_chans = sorted(range_df['channel'].unique().tolist())
        with f1: sel_chans = st.multiselect("Select Channels", avail_chans, default=avail_chans)
        
        chan_mask = range_df['channel'].isin(sel_chans)
        avail_items = sorted(range_df[chan_mask]['item_name'].unique().tolist())
        with f2: sel_items = st.multiselect("Select Products", avail_items, default=avail_items)

        # Apply final filtering
        f_df = range_df[chan_mask & range_df['item_name'].isin(sel_items)].copy()

        if not f_df.empty:
            num_days = max((end_d - start_d).days + 1, 1)
            total_val = f_df[target].sum()
            
            m1, m2 = st.columns(2)
            m1.metric(f"Total {metric_choice}", f"₹{total_val:,.2f}" if "Revenue" in metric_choice else f"{total_val:,.0f}")
            m2.metric("Daily Run Rate (DRR)", f"₹{total_val/num_days:,.2f}" if "Revenue" in metric_choice else f"{total_val/num_days:,.1f}")
            
            # Chart - grouped to ensure clean stacks
            chart_data = f_df.groupby(['date', 'channel'])[target].sum().reset_index().sort_values('date')
            fig = px.bar(chart_data, x="date", y=target, color="channel", barmode="stack", height=500)
            if show_labels: fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("View Filtered Data Table"):
                st.dataframe(f_df.sort_values('date', ascending=False).drop(columns=['date_dt', 'id']), hide_index=True)
        else:
            st.warning("No data matches the selected filters.")

if role == "admin":
    with tabs[1]: # SMART UPLOAD
        st.subheader("Upload Sales Data")
        channels = master_chans['name'].tolist()
        sel_ch = st.selectbox("Select Target Channel", channels)
        up = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
        
        if up and sel_ch:
            df = pd.read_csv(up) if up.name.endswith('.csv') else pd.read_excel(up)
            cols = ["None"] + df.columns.tolist()
            c1, c2, c3 = st.columns(3)
            p_col = c1.selectbox("Product Description", cols)
            v_col = c1.selectbox("Variant (Optional)", cols)
            q_col = c2.selectbox("Quantity Column", cols)
            r_col = c2.selectbox("Revenue Column", cols)
            d_col = c3.selectbox("Date Column", cols)
            manual_date = c3.date_input("Manual Date (if Date Col is 'None')")

            if p_col != "None":
                df = df[~df[p_col].astype(str).str.contains("Total|Grand Total|Sub-total", case=False, na=False)]
                df['m_key'] = df[p_col].astype(str) + ((" | " + df[v_col].astype(str)) if v_col != "None" else "")
                u_keys = df['m_key'].unique()
                sku_map, masters = {}, master_skus['name'].tolist()
                
                st.write("---")
                st.markdown("### 🛠 Map Items to Master SKUs")
                for k in u_keys:
                    ex = item_map_df[item_map_df['raw_name'] == k]
                    idx = masters.index(ex['master_name'].iloc[0]) if not ex.empty and ex['master_name'].iloc[0] in masters else 0
                    sku_map[k] = st.selectbox(f"Map: {k}", masters, index=idx)

                if st.button("🚀 Sync and Update Dashboard"):
                    with st.spinner("Processing..."):
                        for k, v in sku_map.items():
                            supabase.table("item_map").upsert({"raw_name": k, "master_name": v}).execute()
                        
                        temp_rows = []
                        for _, row in df.iterrows():
                            final_dt = parse_date_smart(row[d_col]) if d_col != "None" else str(manual_date)
                            if final_dt:
                                temp_rows.append({
                                    "date": final_dt, "channel": sel_ch, "item_name": sku_map[row['m_key']],
                                    "qty_sold": clean_num(row[q_col]), "revenue": clean_num(row[r_col])
                                })
                        
                        if temp_rows:
                            final_df = pd.DataFrame(temp_rows).groupby(['date', 'channel', 'item_name']).agg({'qty_sold':'sum', 'revenue':'sum'}).reset_index()
                            final_df['revenue'] = final_df['revenue'].round(2)
                            supabase.table("sales").insert(final_df.to_dict(orient='records')).execute()
                            st.cache_data.clear()
                            st.success(f"Uploaded ₹{final_df['revenue'].sum():,.2f}!")
                            st.rerun()

    with tabs[2]: # CONFIG
        st.subheader("Manage System Settings")
        sc1, sc2 = st.columns(2)
        with sc1:
            st.markdown("#### 📦 Master SKU List")
            n_sku = st.text_input("Add New SKU")
            if st.button("Add SKU") and n_sku:
                supabase.table("master_skus").insert({"name": n_sku.strip()}).execute(); st.rerun()
            st.dataframe(master_skus, hide_index=True)
        with sc2:
            st.markdown("#### 🏢 Sales Channels")
            n_ch = st.text_input("Add New Channel")
            if st.button("Add Channel") and n_ch:
                supabase.table("master_channels").insert({"name": n_ch.strip()}).execute(); st.rerun()
            st.dataframe(master_chans, hide_index=True)
