import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import re
import plotly.express as px
from datetime import datetime, timedelta
import io

# --- 1. SETUP & AUTH ---
st.set_page_config(page_title="Mamanourish Sales Portal", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        pw = st.text_input("Enter Access Password", type="password")
        if st.button("Login"):
            if pw in ["mamaadmin2026", "mamaview2026"]:
                st.session_state["password_correct"] = True
                st.session_state["user_role"] = "admin" if pw == "mamaadmin2026" else "viewer"
                st.rerun()
            else: st.error("😕 Password incorrect")
        return False
    return True

if check_password():
    # --- 2. CONNECTION ---
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"
    conn = st.connection("gsheets", type=GSheetsConnection)

    def load_sheet(name, columns):
        try:
            df = conn.read(spreadsheet=SHEET_URL, worksheet=name, ttl=0)
            return df if df is not None and not df.empty else pd.DataFrame(columns=columns)
        except: return pd.DataFrame(columns=columns)

    # Load Data
    history_df = load_sheet("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
    item_map_df = load_sheet("item_map", ["raw_name", "master_name"])
    master_skus = load_sheet("master_skus", ["name"])
    channels_df = load_sheet("master_channels", ["name"])
    
    # Robust Channel List Pull (looks at first column)
    master_chans = channels_df.iloc[:, 0].dropna().unique().tolist() if not channels_df.empty else []

    st.title("📈 Mamanourish Sales Intelligence")

    tabs = st.tabs(["📊 Analytics", "📤 Smart Upload"]) if st.session_state["user_role"] == "admin" else st.tabs(["📊 Analytics"])

    # --- TAB 1: ANALYTICS ---
    with tabs[0]:
        if history_df.empty: st.info("No data found.")
        else:
            history_df['date'] = pd.to_datetime(history_df['date'])
            fig = px.bar(history_df, x='date', y='revenue', color='channel', title="Revenue Trend")
            st.plotly_chart(fig, use_container_width=True)

    # --- TAB 2: SMART UPLOAD (Restored Logic) ---
    if st.session_state["user_role"] == "admin":
        with tabs[1]:
            st.subheader("1. Select Channel")
            if not master_chans:
                st.error("⚠️ No channels found in 'master_channels' tab. Please add them to your sheet first.")
                st.stop()
            
            target_chan = st.selectbox("Channel", master_chans)
            up_file = st.file_uploader("Upload Report", type=["csv", "xlsx"])
            
            if up_file:
                raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                cols = ["None"] + raw_df.columns.tolist()
                
                st.subheader("2. Map Columns")
                c1, c2, c3 = st.columns(3)
                p_col = c1.selectbox("Product Name Column", cols)
                v_col = c1.selectbox("Variant Column (Optional)", cols)
                q_col = c2.selectbox("Quantity Column", cols)
                r_col = c2.selectbox("Revenue Column", cols)
                d_col = c3.selectbox("Date Column", cols)
                fixed_date = c3.date_input("Manual Date (if Date Column is 'None')")

                if p_col != "None" and not master_skus.empty:
                    st.subheader("3. Map to Master SKUs")
                    # Create Raw Keys (Product + Variant)
                    unique_raw = raw_df[[p_col, v_col]].drop_duplicates() if v_col != "None" else raw_df[[p_col]].drop_duplicates()
                    
                    sku_mapping = {}
                    m_list = master_skus.iloc[:, 0].tolist()
                    
                    for _, row in unique_raw.iterrows():
                        raw_label = f"{row[p_col]} | {row[v_col]}" if v_col != "None" else str(row[p_col])
                        # Look for existing mapping
                        existing = item_map_df[item_map_df['raw_name'] == raw_label]
                        idx = m_list.index(existing['master_name'].iloc[0]) if not existing.empty and existing['master_name'].iloc[0] in m_list else 0
                        sku_mapping[raw_label] = st.selectbox(f"Map: {raw_label}", m_list, index=idx)

                    if st.button("🚀 Process & Upload"):
                        with st.spinner("Updating Cloud..."):
                            # Update Mapping Memory
                            for k, v in sku_mapping.items():
                                item_map_df = pd.concat([item_map_df[item_map_df['raw_name'] != k], pd.DataFrame([[k, v]], columns=["raw_name", "master_name"])])
                            conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=item_map_df)

                            # Transform Data
                            new_entries = []
                            for _, r in raw_df.iterrows():
                                dt = pd.to_datetime(r[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                                raw_key = f"{r[p_col]} | {r[v_col]}" if v_col != "None" else str(r[p_col])
                                
                                # Clean numbers
                                rev = re.sub(r'[^\d.]', '', str(r[r_col]))
                                qty = re.sub(r'[^\d.]', '', str(r[q_col]))
                                
                                new_entries.append([dt, target_chan, sku_mapping[raw_key], float(qty) if qty else 0, float(rev) if rev else 0])
                            
                            new_sales = pd.DataFrame(new_entries, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                            final_history = pd.concat([history_df, new_sales], ignore_index=True)
                            
                            conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=final_history)
                            st.cache_data.clear()
                            st.success("Success! Sales data and mappings updated.")
                            st.rerun()
