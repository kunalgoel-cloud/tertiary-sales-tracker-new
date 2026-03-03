import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import plotly.express as px
from datetime import datetime
import io

# --- 1. SETUP ---
st.set_page_config(page_title="Mamanourish Sales", layout="wide")

def check_password():
    if "authenticated" not in st.session_state:
        st.title("🔐 Login")
        pw = st.text_input("Password", type="password")
        if st.button("Login"):
            if pw in ["mamaadmin2026", "mamaview2026"]:
                st.session_state["authenticated"] = True
                st.session_state["is_admin"] = (pw == "mamaadmin2026")
                st.rerun()
            else: st.error("Incorrect password")
        return False
    return True

if check_password():
    # --- 2. DATA CONNECTION ---
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"
    conn = st.connection("gsheets", type=GSheetsConnection)

    def load_data(tab_name):
        try:
            return conn.read(spreadsheet=SHEET_URL, worksheet=tab_name, ttl=0)
        except Exception:
            return pd.DataFrame()

    # Load master lists you updated
    master_skus = load_data("master_skus")['name'].tolist() if "name" in load_data("master_skus") else []
    master_chans = load_data("master_channels")['name'].tolist() if "name" in load_data("master_channels") else []
    sales_history = load_data("sales")

    st.title("📈 Mamanourish Sales Portal")

    # --- 3. TABS ---
    tab_list = ["📊 Analytics", "📤 Upload Sales"] if st.session_state["is_admin"] else ["📊 Analytics"]
    tabs = st.tabs(tab_list)

    # --- ANALYTICS TAB ---
    with tabs[0]:
        if sales_history.empty:
            st.info("No data found in the 'sales' tab.")
        else:
            sales_history['date'] = pd.to_datetime(sales_history['date'])
            
            c1, c2 = st.columns(2)
            with c1:
                sel_chan = st.multiselect("Channels", master_chans, default=master_chans)
            with c2:
                days = st.slider("Days Lookback", 7, 90, 30)
            
            start_date = datetime.now() - pd.Timedelta(days=days)
            filtered = sales_history[(sales_history['date'] >= start_date) & (sales_history['channel'].isin(sel_chan))]
            
            if not filtered.empty:
                st.metric("Total Revenue", f"₹{filtered['revenue'].sum():,.2f}")
                fig = px.bar(filtered, x='date', y='revenue', color='channel', title="Revenue Trend")
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(filtered.sort_values('date', ascending=False), use_container_width=True)

    # --- UPLOAD TAB (ADMIN ONLY) ---
    if st.session_state["is_admin"]:
        with tabs[1]:
            st.subheader("Upload New Sales Data")
            chosen_chan = st.selectbox("Select Channel", master_chans)
            up_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
            
            if up_file:
                raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                st.write("File Preview:", raw_df.head(3))
                
                cols = raw_df.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                p_col = c1.selectbox("Product Name", cols)
                q_col = c2.selectbox("Quantity", cols)
                r_col = c3.selectbox("Revenue", cols)
                d_col = c4.selectbox("Date", ["Manual"] + cols)
                
                manual_date = st.date_input("Manual Date Selection") if d_col == "Manual" else None

                if st.button("Confirm & Append to Cloud"):
                    # Process rows
                    new_rows = []
                    for _, row in raw_df.iterrows():
                        row_date = manual_date if d_col == "Manual" else pd.to_datetime(row[d_col]).date()
                        new_rows.append({
                            "date": str(row_date),
                            "channel": chosen_chan,
                            "item_name": row[p_col],
                            "qty_sold": row[q_col],
                            "revenue": row[r_col]
                        })
                    
                    new_data = pd.DataFrame(new_rows)
                    updated_sales = pd.concat([sales_history, new_data], ignore_index=True)
                    
                    try:
                        conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=updated_sales)
                        st.success("✅ Successfully updated the 'sales' tab!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Failed to write to sheet: {e}")
