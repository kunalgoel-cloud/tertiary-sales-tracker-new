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
            df = conn.read(spreadsheet=SHEET_URL, worksheet=tab_name, ttl=0)
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    # Load data from your sheet
    sales_history = load_data("sales")
    
    # IMPROVED: Robust Channel Pulling
    channels_df = load_data("master_channels")
    master_chans = []
    if not channels_df.empty:
        # This pulls data from the FIRST column found, regardless of its header name
        master_chans = channels_df.iloc[:, 0].dropna().unique().tolist()

    st.title("📈 Mamanourish Sales Portal")

    # --- 3. TABS ---
    tab_titles = ["📊 Analytics", "📤 Upload Sales"] if st.session_state["is_admin"] else ["📊 Analytics"]
    tabs = st.tabs(tab_titles)

    # --- ANALYTICS TAB ---
    with tabs[0]:
        if sales_history.empty:
            st.info("No sales data found yet. Use the Upload tab to add data.")
        else:
            sales_history['date'] = pd.to_datetime(sales_history['date'])
            available_chans = sales_history['channel'].unique().tolist()
            
            c1, c2 = st.columns([2, 1])
            with c1:
                sel_chan = st.multiselect("Filter Channels", available_chans, default=available_chans)
            with c2:
                days = st.number_input("Days Lookback", value=30)
            
            start_date = datetime.now() - pd.Timedelta(days=days)
            filtered = sales_history[(sales_history['date'] >= start_date) & (sales_history['channel'].isin(sel_chan))]
            
            if not filtered.empty:
                st.metric("Total Revenue", f"₹{filtered['revenue'].sum():,.2f}")
                fig = px.bar(filtered, x='date', y='revenue', color='channel', title="Revenue Trend")
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(filtered.sort_values('date', ascending=False), hide_index=True)

    # --- UPLOAD TAB (ADMIN ONLY) ---
    if st.session_state["is_admin"]:
        with tabs[1]:
            st.subheader("Step 1: Select Channel")
            
            # If the list is still empty, provide a manual entry box so the user isn't blocked
            if not master_chans:
                st.warning("⚠️ Could not find a list in 'master_channels'. Please enter manually:")
                chosen_chan = st.text_input("Channel Name (e.g., Blinkit, Amazon)")
            else:
                chosen_chan = st.selectbox("Select Channel from Sheet", master_chans)

            st.subheader("Step 2: Upload File")
            up_file = st.file_uploader("Upload Excel/CSV", type=['csv', 'xlsx'])
            
            if up_file and chosen_chan:
                raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                st.write("Preview:", raw_df.head(3))
                
                st.subheader("Step 3: Map Columns")
                cols = raw_df.columns.tolist()
                c1, c2, c3, c4 = st.columns(4)
                p_col = c1.selectbox("Product Name", cols)
                q_col = c2.selectbox("Quantity", cols)
                r_col = c3.selectbox("Revenue", cols)
                d_col = c4.selectbox("Date Column", ["Use Manual Date"] + cols)
                
                manual_date = st.date_input("Select Date") if d_col == "Use Manual Date" else None

                if st.button("🚀 Upload to Google Sheets"):
                    with st.spinner("Syncing..."):
                        new_rows = []
                        for _, row in raw_df.iterrows():
                            # Clean currency and comma formatting
                            rev = str(row[r_col]).replace('₹','').replace(',','')
                            qty = str(row[q_col]).replace(',','')
                            row_date = manual_date if d_col == "Use Manual Date" else pd.to_datetime(row[d_col]).date()
                            
                            new_rows.append({
                                "date": str(row_date),
                                "channel": chosen_chan,
                                "item_name": row[p_col],
                                "qty_sold": float(qty) if qty else 0,
                                "revenue": float(rev) if rev else 0
                            })
                        
                        new_data = pd.DataFrame(new_rows)
                        updated_sales = pd.concat([sales_history, new_data], ignore_index=True)
                        
                        try:
                            conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=updated_sales)
                            st.success(f"✅ Data for {chosen_chan} uploaded successfully!")
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Write failed: {e}")
