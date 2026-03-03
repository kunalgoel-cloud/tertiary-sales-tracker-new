import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import re
import plotly.express as px
from datetime import datetime
import io

# --- 1. CONFIG & AUTH ---
st.set_page_config(page_title="Mamanourish Portal", layout="wide")

def check_password():
    if "authenticated" not in st.session_state:
        st.title("🔐 Mamanourish Login")
        pw = st.text_input("Password", type="password")
        if st.button("Login"):
            if pw in ["mamaadmin2026", "mamaview2026"]:
                st.session_state["authenticated"] = True
                st.session_state["is_admin"] = (pw == "mamaadmin2026")
                st.rerun()
            else: st.error("Wrong password")
        return False
    return True

if check_password():
    # --- 2. DATA CONNECTION ---
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"
    conn = st.connection("gsheets", type=GSheetsConnection)

    def load_data(tab, cols):
        try:
            df = conn.read(spreadsheet=SHEET_URL, worksheet=tab, ttl=0)
            return df if df is not None and not df.empty else pd.DataFrame(columns=cols)
        except: return pd.DataFrame(columns=cols)

    # Load All Tabs
    sales_history = load_data("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
    item_map_df = load_data("item_map", ["raw_name", "master_name"])
    master_skus = load_data("master_skus", ["name"])
    channels_df = load_data("master_channels", ["name"])

    # Robust Channel List (Pulls from Column A regardless of header)
    master_chans = channels_df.iloc[:, 0].dropna().unique().tolist() if not channels_df.empty else []

    st.title("📈 Mamanourish Sales Intelligence")
    tabs = st.tabs(["📊 Analytics", "📤 Smart Upload"]) if st.session_state["is_admin"] else st.tabs(["📊 Analytics"])

    # --- ANALYTICS ---
    with tabs[0]:
        if sales_history.empty: st.info("No data yet.")
        else:
            sales_history['date'] = pd.to_datetime(sales_history['date'])
            fig = px.bar(sales_history, x='date', y='revenue', color='channel', title="Sales Over Time")
            st.plotly_chart(fig, use_container_width=True)

    # --- SMART UPLOAD ---
    if st.session_state["is_admin"]:
        with tabs[1]:
            st.subheader("1. Selection")
            target_chan = st.selectbox("Select Channel", master_chans)
            up_file = st.file_uploader("Upload File", type=['csv', 'xlsx'])

            if up_file:
                raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                cols = ["None"] + raw_df.columns.tolist()

                st.subheader("2. Map Columns")
                c1, c2, c3 = st.columns(3)
                p_col = c1.selectbox("Product Name", cols)
                v_col = c1.selectbox("Variant (Optional)", cols)
                q_col = c2.selectbox("Quantity", cols)
                r_col = c2.selectbox("Revenue", cols)
                d_col = c3.selectbox("Date", cols)
                fixed_date = c3.date_input("Manual Date (If no Date col)")

                if p_col != "None" and not master_skus.empty:
                    st.subheader("3. Map to Master SKUs")
                    # Create Unique Raw Keys
                    unique_rows = raw_df[[p_col, v_col]].drop_duplicates() if v_col != "None" else raw_df[[p_col]].drop_duplicates()
                    sku_mapping = {}
                    m_list = master_skus.iloc[:, 0].tolist()

                    for _, row in unique_rows.iterrows():
                        raw_key = f"{row[p_col]} | {row[v_col]}" if v_col != "None" else str(row[p_col])
                        # Suggest existing mapping
                        existing = item_map_df[item_map_df['raw_name'] == raw_key]
                        idx = m_list.index(existing['master_name'].iloc[0]) if not existing.empty and existing['master_name'].iloc[0] in m_list else 0
                        sku_mapping[raw_key] = st.selectbox(f"Map: {raw_key}", m_list, index=idx)

                    if st.button("🚀 Upload to Sheets"):
                        with st.spinner("Processing..."):
                            # A. Update Mapping Memory
                            for k, v in sku_mapping.items():
                                item_map_df = pd.concat([item_map_df[item_map_df['raw_name'] != k], pd.DataFrame([[k, v]], columns=["raw_name", "master_name"])])
                            conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=item_map_df)

                            # B. Transform Sales Data
                            processed = []
                            for _, r in raw_df.iterrows():
                                dt = pd.to_datetime(r[d_col]).date() if d_col != "None" else fixed_date
                                key = f"{r[p_col]} | {r[v_col]}" if v_col != "None" else str(r[p_col])
                                rev = re.sub(r'[^\d.]', '', str(r[r_col]))
                                qty = re.sub(r'[^\d.]', '', str(r[q_col]))
                                
                                processed.append([str(dt), target_chan, sku_mapping[key], float(qty or 0), float(rev or 0)])
                            
                            new_df = pd.DataFrame(processed, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                            final_sales = pd.concat([sales_history, new_df], ignore_index=True)
                            
                            # C. Save
                            try:
                                conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=final_sales)
                                st.success("✅ Upload Complete!")
                                st.cache_data.clear()
                            except Exception as e:
                                st.error(f"Permission Error: Please ensure the Google Sheet is shared with your Service Account email as an 'Editor'.")
