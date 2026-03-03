import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & PERMANENT DB CONNECTION ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")

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

# --- 2. AUTHENTICATION SYSTEM ---
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
            else: st.error("Incorrect password.")
        return False
    return True

if check_auth():
    role = st.session_state["role"]
    
    # Helper to fetch data from Supabase
    def get_data(table):
        try:
            res = supabase.table(table).select("*").execute()
            return pd.DataFrame(res.data)
        except:
            return pd.DataFrame()

    # --- 3. SIDEBAR (DANGER ZONE) ---
    with st.sidebar:
        st.header(f"👤 {role.upper()}")
        if role == "admin":
            st.divider()
            st.subheader("⚙️ Data Management")
            if st.checkbox("Unlock Danger Zone"):
                st.warning("Actions below cannot be undone.")
                if st.button("🗑️ Flush All Sales History"):
                    supabase.table("sales").delete().neq("id", -1).execute()
                    st.success("Sales History Cleared!")
                    st.rerun()
                
                if st.button("🔄 Reset Item Mappings"):
                    supabase.table("item_map").delete().neq("master_name", "dummy").execute()
                    st.success("Mappings Reset!")
                    st.rerun()
        
        st.divider()
        if st.button("Logout"):
            del st.session_state["authenticated"]
            st.rerun()

    # Load All Global Data
    history_df = get_data("sales")
    master_skus = get_data("master_skus")
    master_chans = get_data("master_channels")
    item_map_df = get_data("item_map")

    # --- 4. TABS ---
    if role == "admin":
        tab1, tab2, tab3 = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Configuration"])
    else:
        tab1 = st.tabs(["📊 Analytics"])[0]

    # --- TAB 1: ANALYTICS ---
    with tab1:
        if history_df.empty:
            st.info("No sales history found. Admin needs to upload data.")
        else:
            view_metric = st.radio("Display By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
            target = "revenue" if "Revenue" in view_metric else "qty_sold"
            
            history_df['date'] = pd.to_datetime(history_df['date'])
            fig = px.bar(history_df, x='date', y=target, color='channel', 
                         title=f"Daily {view_metric}", barmode="stack")
            
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(history_df.sort_values('date', ascending=False), hide_index=True, width=1200)

    # --- TAB 2: SMART UPLOAD (ADMIN ONLY) ---
    if role == "admin":
        with tab2:
            st.subheader("Upload Sales Report")
            channels = master_chans['name'].tolist() if not master_chans.empty else []
            selected_channel = st.selectbox("Select Channel", channels)
            
            up_file = st.file_uploader("Upload File (CSV or Excel)", type=["csv", "xlsx"])

            if up_file and selected_channel:
                df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                cols = ["None"] + df.columns.tolist()

                c1, c2, c3 = st.columns(3)
                p_col = c1.selectbox("Product Name Column", cols)
                v_col = c1.selectbox("Variant Column (Optional)", cols)
                q_col = c2.selectbox("Qty Column", cols)
                r_col = c2.selectbox("Revenue Column", cols)
                d_col = c3.selectbox("Date Column", cols)
                fixed_date = c3.date_input("Manual Date (if Date Col is 'None')")

                if p_col != "None":
                    st.divider()
                    st.markdown("### 🛠 Step 2: Mapping to Master SKUs")
                    
                    # Create the composite key
                    df['mapping_key'] = df[p_col].astype(str) + ((" | " + df[v_col].astype(str)) if v_col != "None" else "")
                    unique_keys = df['mapping_key'].unique()
                    
                    sku_map = {}
                    masters = master_skus['name'].tolist() if not master_skus.empty else []
                    
                    for key in unique_keys:
                        existing = item_map_df[item_map_df['raw_name'] == key]
                        idx = masters.index(existing['master_name'].iloc[0]) if not existing.empty and existing['master_name'].iloc[0] in masters else 0
                        sku_map[key] = st.selectbox(f"Map: {key}", masters, index=idx)

                    if st.button("🚀 Sync to Cloud"):
                        with st.spinner("Processing..."):
                            # 1. Update Mapping Logic
                            for k, v in sku_map.items():
                                supabase.table("item_map").upsert({"raw_name": k, "master_name": v}).execute()
                            
                            # 2. Format Sales Rows
                            final_rows = []
                            for _, row in df.iterrows():
                                dt = pd.to_datetime(row[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                                final_rows.append({
                                    "date": dt, 
                                    "channel": selected_channel,
                                    "item_name": sku_map[row['mapping_key']],
                                    "qty_sold": clean_num(row[q_col]), 
                                    "revenue": clean_num(row[r_col])
                                })
                            
                            supabase.table("sales").insert(final_rows).execute()
                            st.success(f"✅ Data for {selected_channel} uploaded successfully!")
                            st.rerun()

    # --- TAB 3: CONFIGURATION (ADMIN ONLY) ---
    if role == "admin":
        with tab3:
            st.subheader("⚙️ System Lists")
            c1, c2 = st.columns(2)
            
            with c1:
                st.markdown("#### 📦 Master SKUs")
                new_sku = st.text_input("New SKU Name")
                if st.button("Add SKU") and new_sku:
                    supabase.table("master_skus").insert({"name": new_sku.strip()}).execute()
                    st.rerun()
                st.dataframe(master_skus, hide_index=True)

            with c2:
                st.markdown("#### 🏢 Sales Channels")
                new_ch = st.text_input("New Channel Name")
                if st.button("Add Channel") and new_ch:
                    supabase.table("master_channels").insert({"name": new_ch.strip()}).execute()
                    st.rerun()
                st.dataframe(master_chans, hide_index=True)
