import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & PERMANENT DB CONNECTION ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")

# Connect to Supabase
try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception:
    st.error("Missing Supabase Secrets! Please add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
    st.stop()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. MULTI-USER LOGIN SYSTEM ---
def check_auth():
    if "authenticated" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        role_choice = st.selectbox("I am a...", ["Select Role", "Admin (Full Access)", "Viewer (View Only)"])
        pw = st.text_input("Enter Password", type="password")
        
        if st.button("Login"):
            if role_choice == "Admin (Full Access)" and pw == "mamaadmin2026":
                st.session_state["authenticated"] = True
                st.session_state["role"] = "admin"
                st.rerun()
            elif role_choice == "Viewer (View Only)" and pw == "mamaview2026":
                st.session_state["authenticated"] = True
                st.session_state["role"] = "viewer"
                st.rerun()
            else:
                st.error("Incorrect password for the selected role.")
        return False
    return True

if check_auth():
    role = st.session_state["role"]
    
    # --- 3. DATA FETCHING (From Cloud) ---
    def get_data(table):
        res = supabase.table(table).select("*").execute()
        return pd.DataFrame(res.data)

    history_df = get_data("sales")
    master_skus = get_data("master_skus")
    master_chans = get_data("master_channels")
    item_map_df = get_data("item_map")

    st.title("📈 Tertiary Sales Executive Dashboard")
    
    with st.sidebar:
        st.write(f"Logged in as: **{role.upper()}**")
        if st.button("Logout"):
            del st.session_state["authenticated"]
            st.rerun()

    # --- 4. TABS (Restricted) ---
    if role == "admin":
        tab1, tab2, tab3 = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Configuration"])
    else:
        tab1 = st.tabs(["📊 Analytics"])[0]
        st.info("💡 Viewer Mode: Upload and Configuration are disabled.")

    # --- TAB 1: ANALYTICS ---
    with tab1:
        if history_df.empty:
            st.info("No sales history found. Admin needs to upload data.")
        else:
            # Reusing your metric & plotting logic
            view_metric = st.radio("Display By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
            target = "revenue" if "Revenue" in view_metric else "qty_sold"
            
            history_df['date'] = pd.to_datetime(history_df['date'])
            fig = px.bar(history_df, x='date', y=target, color='channel', title="Sales Trend")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(history_df, use_container_width=True)

    # --- TAB 2: SMART UPLOAD (Admin Only) ---
    if role == "admin":
        with tab2:
            st.subheader("Process New Sales Report")
            channel = st.selectbox("Select Channel", master_chans['name'].tolist() if not master_chans.empty else [])
            up_file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])

            if up_file and channel:
                df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                cols = ["None"] + df.columns.tolist()

                c1, c2, c3 = st.columns(3)
                p_col = c1.selectbox("Product Name Column", cols)
                v_col = c1.selectbox("Variant Column (Optional)", cols)
                q_col = c2.selectbox("Qty Column", cols)
                r_col = c2.selectbox("Revenue Column", cols)
                d_col = c3.selectbox("Date Column", cols)
                fixed_date = c3.date_input("Manual Date (if no Date Column)")

                if p_col != "None":
                    # Mapping Logic (Combining Product + Variant)
                    df['mapping_key'] = df[p_col].astype(str) + ((" | " + df[v_col].astype(str)) if v_col != "None" else "")
                    unique_keys = df['mapping_key'].unique()
                    
                    st.markdown("### 🛠 Map to Master SKUs")
                    sku_map = {}
                    masters = master_skus['name'].tolist() if not master_skus.empty else []
                    
                    for key in unique_keys:
                        existing = item_map_df[item_map_df['raw_name'] == key]
                        idx = masters.index(existing['master_name'].iloc[0]) if not existing.empty and existing['master_name'].iloc[0] in masters else 0
                        sku_map[key] = st.selectbox(f"Map: {key}", masters, index=idx)

                    if st.button("🚀 Sync to Permanent Cloud"):
                        # 1. Update Mapping
                        for k, v in sku_map.items():
                            supabase.table("item_map").upsert({"raw_name": k, "master_name": v}).execute()
                        
                        # 2. Prepare Data
                        final_rows = []
                        for _, row in df.iterrows():
                            dt = pd.to_datetime(row[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                            final_rows.append({
                                "date": dt, "channel": channel,
                                "item_name": sku_map[row['mapping_key']],
                                "qty_sold": clean_num(row[q_col]), "revenue": clean_num(row[r_col])
                            })
                        
                        # 3. Save
                        supabase.table("sales").insert(final_rows).execute()
                        st.success("✅ Saved to Cloud!")
                        st.rerun()

    # --- TAB 3: CONFIGURATION (Admin Only) ---
    if role == "admin":
        with tab3:
            st.subheader("Add Master SKUs")
            new_sku = st.text_input("Enter SKU Name")
            if st.button("Add SKU") and new_sku:
                supabase.table("master_skus").insert({"name": new_sku}).execute()
                st.rerun()
            st.dataframe(master_skus)
