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
    st.error("Missing Supabase Secrets!")
    st.stop()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# NEW: Smart Date Parser to handle "20260219 - 20260219"
def parse_date_smart(val):
    s = str(val).split(" - ")[0].strip() # Take the first date if it's a range
    try:
        # Try standard formats
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except:
        try:
            # Try YYYYMMDD (Common in Blinkit/QuickCommerce reports)
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except:
            return None

# --- 2. AUTHENTICATION ---
if "authenticated" not in st.session_state:
    st.title("🔐 Mamanourish Sales Portal")
    role_choice = st.selectbox("I am a...", ["Select Role", "Admin (Full Access)", "Viewer (View Only)"])
    pw = st.text_input("Enter Password", type="password")
    if st.button("Login"):
        if role_choice == "Admin (Full Access)" and pw == "mamaadmin2026":
            st.session_state["authenticated"], st.session_state["role"] = True, "admin"
            st.rerun()
        elif role_choice == "Viewer (View Only)" and pw == "mamaview2026":
            st.session_state["authenticated"], st.session_state["role"] = True, "viewer"
            st.rerun()
        else: st.error("Incorrect password.")
    st.stop()

role = st.session_state["role"]

def get_data_safe(table, default_cols):
    try:
        res = supabase.table(table).select("*").execute()
        df = pd.DataFrame(res.data)
        return df if not df.empty else pd.DataFrame(columns=default_cols)
    except: return pd.DataFrame(columns=default_cols)

# Load Data
history_df = get_data_safe("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])
master_skus = get_data_safe("master_skus", ["name"])
master_chans = get_data_safe("master_channels", ["name"])
item_map_df = get_data_safe("item_map", ["raw_name", "master_name"])

# --- 3. SIDEBAR ---
with st.sidebar:
    st.header(f"👤 {role.upper()}")
    if role == "admin":
        st.divider()
        st.subheader("🛠 Data Correction")
        with st.expander("Delete Specific Entry"):
            del_date = st.date_input("Date to Clear", value=datetime.now().date())
            del_chan = st.selectbox("Channel to Clear", ["Select..."] + master_chans['name'].tolist())
            if st.button("🗑️ Delete Selection"):
                if del_chan != "Select...":
                    supabase.table("sales").delete().eq("date", str(del_date)).eq("channel", del_chan).execute()
                    st.success(f"Deleted {del_chan} for {del_date}"); st.rerun()
    if st.button("Logout"):
        del st.session_state["authenticated"]; st.rerun()

# --- 4. TABS ---
tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Configuration"]) if role == "admin" else st.tabs(["📊 Analytics"])

with tabs[0]: # Analytics Logic
    if history_df.empty:
        st.info("No data in cloud. Admin needs to upload.")
    else:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        v1, v2 = st.columns([2, 1])
        with v1:
            view_metric = st.radio("Show By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
            target = "revenue" if "Revenue" in view_metric else "qty_sold"
        with v2: show_labels = st.checkbox("Data Labels", value=True)

        # Filters
        time_preset = st.radio("Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
        today = datetime.now().date()
        if time_preset == "Last 7 Days": start, end = today - timedelta(days=6), today
        elif time_preset == "Last 30 Days": start, end = today - timedelta(days=29), today
        elif time_preset == "All Time": start, end = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
        else: start, end = today.replace(day=1), today # Default MTD

        mask = (history_df['date_dt'].dt.date >= start) & (history_df['date_dt'].dt.date <= end)
        f_df = history_df[mask].copy()
        
        # Plotting
        if not f_df.empty:
            total = f_df[target].sum()
            days = (end - start).days + 1
            st.metric(f"Total {view_metric}", f"₹{total:,.0f}" if "Revenue" in view_metric else f"{total:,.0f}")
            st.metric("DRR", f"₹{total/days:,.0f}" if "Revenue" in view_metric else f"{total/days:,.0f}")
            
            fig = px.bar(f_df, x="date", y=target, color="channel", barmode="stack")
            if show_labels: fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(f_df.drop(columns=['date_dt', 'id']), hide_index=True)

if role == "admin":
    with tabs[1]: # Upload Tab
        st.subheader("Upload Report")
        sel_chan = st.selectbox("Channel", master_chans['name'].tolist())
        up_file = st.file_uploader("CSV/Excel", type=["csv", "xlsx"])
        if up_file and sel_chan:
            df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
            cols = ["None"] + df.columns.tolist()
            c1, c2, c3 = st.columns(3)
            p_col = c1.selectbox("Product Col", cols); v_col = c1.selectbox("Var Col", cols)
            q_col = c2.selectbox("Qty Col", cols); r_col = c2.selectbox("Rev Col", cols)
            d_col = c3.selectbox("Date Col", cols); man_date = c3.date_input("Manual Date")

            if p_col != "None":
                df['m_key'] = df[p_col].astype(str) + ((" | " + df[v_col].astype(str)) if v_col != "None" else "")
                u_keys = df['m_key'].unique()
                sku_map, masters = {}, master_skus['name'].tolist()
                for k in u_keys:
                    ex = item_map_df[item_map_df['raw_name'] == k]
                    idx = masters.index(ex['master_name'].iloc[0]) if not ex.empty and ex['master_name'].iloc[0] in masters else 0
                    sku_map[k] = st.selectbox(f"Map: {k}", masters, index=idx)

                if st.button("🚀 Sync to Cloud"):
                    for k, v in sku_map.items():
                        supabase.table("item_map").upsert({"raw_name": k, "master_name": v}).execute()
                    batch = []
                    for _, row in df.iterrows():
                        final_date = parse_date_smart(row[d_col]) if d_col != "None" else str(man_date)
                        if final_date:
                            batch.append({
                                "date": final_date, "channel": sel_chan, 
                                "item_name": sku_map[row['m_key']],
                                "qty_sold": clean_num(row[q_col]), "revenue": clean_num(row[r_col])
                            })
                    if batch:
                        supabase.table("sales").insert(batch).execute()
                        st.success("Success!"); st.rerun()

    with tabs[2]: # Config Tab
        st.subheader("System Configuration")
        cc1, cc2 = st.columns(2)
        with cc1:
            n_sku = st.text_input("New SKU")
            if st.button("Add SKU") and n_sku:
                supabase.table("master_skus").insert({"name": n_sku}).execute(); st.rerun()
            st.dataframe(master_skus, hide_index=True)
        with cc2:
            n_ch = st.text_input("New Channel")
            if st.button("Add Channel") and n_ch:
                supabase.table("master_channels").insert({"name": n_ch}).execute(); st.rerun()
            st.dataframe(master_chans, hide_index=True)
