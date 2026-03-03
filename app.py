import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import re
import plotly.express as px
from datetime import datetime, timedelta
import io

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Mamanourish Sales Portal", layout="wide", page_icon="📈")

# --- 2. AUTHENTICATION SYSTEM ---
def check_password():
    """Returns True if the user had the correct password."""
    USER_PASSWORDS = {
        "admin": "mamaadmin2026",    # Can upload & configure
        "viewer": "mamaview2026"     # Can only see analytics
    }

    if "password_correct" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        pw = st.text_input("Enter Access Password", type="password")
        if st.button("Login"):
            if pw == USER_PASSWORDS["admin"]:
                st.session_state["password_correct"] = True
                st.session_state["user_role"] = "admin"
                st.rerun()
            elif pw == USER_PASSWORDS["viewer"]:
                st.session_state["password_correct"] = True
                st.session_state["user_role"] = "viewer"
                st.rerun()
            else:
                st.error("😕 Password incorrect")
        return False
    return True

# --- 3. MAIN APP LOGIC ---
if check_password():
    # Google Sheets Settings
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"
    
    # Initialize Connection
    conn = st.connection("gsheets", type=GSheetsConnection)

    # Helper: Clean numbers from strings (e.g., "₹ 1,200" -> 1200.0)
    def clean_num(val):
        if pd.isna(val): return 0.0
        res = re.sub(r'[^\d.]', '', str(val))
        return float(res) if res else 0.0

    # Helper: Safe Sheet Loading (Prevents APIError crashes)
    def load_sheet(name, columns):
        try:
            # ttl=0 ensures we always get the freshest data from the cloud
            df = conn.read(spreadsheet=SHEET_URL, worksheet=name, ttl=0)
            if df is None or df.empty:
                return pd.DataFrame(columns=columns)
            return df
        except Exception as e:
            st.sidebar.error(f"⚠️ Error accessing tab: {name}")
            return pd.DataFrame(columns=columns)

    # Sidebar Navigation
    with st.sidebar:
        st.header(f"👤 {st.session_state['user_role'].upper()} MODE")
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        st.divider()
        if st.button("🚪 Logout"):
            del st.session_state["password_correct"]
            st.rerun()

    # Load All Data Tables
    history_df = load_sheet("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
    col_map_df = load_sheet("col_map", ["channel", "item_col", "qty_col", "rev_col", "date_col", "var_col"])
    item_map_df = load_sheet("item_map", ["raw_name", "master_name"])
    master_skus = load_sheet("master_skus", ["name"])
    master_chans = load_sheet("master_channels", ["name"])

    st.title("📈 Mamanourish Sales Intelligence")

    # --- TAB NAVIGATION ---
    if st.session_state["user_role"] == "admin":
        tabs = st.tabs(["📊 Analytics", "📤 Smart Upload", "🛠 Configuration"])
    else:
        tabs = st.tabs(["📊 Analytics"])

    # --- TAB 1: ANALYTICS ---
    with tabs[0]:
        if history_df.empty:
            st.info("👋 Welcome! Please ask an Admin to upload the first sales file.")
        else:
            # Metrics Selection
            view_metric = st.radio("Display Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
            target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
            currency_symbol = "₹" if "Revenue" in view_metric else ""

            history_df['date_dt'] = pd.to_datetime(history_df['date'])
            
            # Global Filters
            c1, c2, c3 = st.columns(3)
            with c1:
                chans = sorted(history_df['channel'].unique())
                sel_chan = st.multiselect("Filter Channels", chans, default=chans)
            with c2:
                items = sorted(history_df['item_name'].unique())
                sel_item = st.multiselect("Filter Products", items)
            with c3:
                days = st.slider("Historical Window (Days)", 7, 180, 30)
                start_date = datetime.now() - timedelta(days=days)

            # Filtering Data
            mask = (history_df['date_dt'] >= start_date) & (history_df['channel'].isin(sel_chan))
            if sel_item:
                mask &= (history_df['item_name'].isin(sel_item))
            
            f_df = history_df[mask].copy()

            # KPI Cards
            total_val = f_df[target_col].sum()
            drr = total_val / days
            
            kpi1, kpi2 = st.columns(2)
            kpi1.metric(f"Total {view_metric}", f"{currency_symbol}{total_val:,.2f}")
            kpi2.metric("Daily Run Rate (DRR)", f"{currency_symbol}{drr:,.2f}")

            # Trend Chart
            if not f_df.empty:
                plot_df = f_df.groupby(['date', 'channel'])[target_col].sum().reset_index()
                fig = px.bar(plot_df, x='date', y=target_col, color='channel', 
                             title=f"Daily {view_metric} Trend", barmode='stack')
                fig.add_hline(y=drr, line_dash="dash", line_color="red", annotation_text="Avg DRR")
                st.plotly_chart(fig, use_container_width=True)

                # Excel Export
                towrite = io.BytesIO()
                f_df.drop(columns=['date_dt']).to_excel(towrite, index=False, engine='xlsxwriter')
                st.download_button("📥 Download Excel Report", towrite.getvalue(), "sales_report.xlsx")

    # --- ADMIN ONLY TABS ---
    if st.session_state["user_role"] == "admin":
        
        # TAB 2: SMART UPLOAD
        with tabs[1]:
            st.subheader("Process New Sales File")
            if master_chans.empty:
                st.warning("Go to Configuration and add a Channel first.")
            else:
                target_chan = st.selectbox("Select Channel", master_chans['name'].tolist())
                up_file = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx"])

                if up_file:
                    raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                    st.write("Preview:", raw_df.head(3))
                    
                    # Column Mapping Logic
                    cols = ["None"] + raw_df.columns.tolist()
                    m_data = col_map_df[col_map_df['channel'] == target_chan]
                    
                    def get_def(key):
                        return cols.index(m_data[key].iloc[0]) if not m_data.empty and m_data[key].iloc[0] in cols else 0

                    st.divider()
                    c1, c2, c3 = st.columns(3)
                    p_col = c1.selectbox("Product Name Column", cols, index=get_def("item_col"))
                    q_col = c1.selectbox("Quantity Column", cols, index=get_def("qty_col"))
                    r_col = c2.selectbox("Revenue Column", cols, index=get_def("rev_col"))
                    d_col = c2.selectbox("Date Column", cols, index=get_def("date_col"))
                    v_col = c3.selectbox("Variant Column (Opt)", cols, index=get_def("var_col"))
                    fixed_date = c3.date_input("Manual Date (if no Date Column)")

                    if p_col != "None" and not master_skus.empty:
                        st.subheader("Match Products to Master SKUs")
                        unique_raw = raw_df[[p_col, v_col]].drop_duplicates() if v_col != "None" else raw_df[[p_col]].drop_duplicates()
                        sku_mapping = {}
                        m_list = master_skus['name'].tolist()

                        for _, row in unique_raw.iterrows():
                            label = f"{row[p_col]} | {row[v_col]}" if v_col != "None" else str(row[p_col])
                            prev = item_map_df[item_map_df['raw_name'] == label]
                            idx = m_list.index(prev['master_name'].iloc[0]) if not prev.empty and prev['master_name'].iloc[0] in m_list else 0
                            sku_mapping[label] = st.selectbox(f"Map '{label}' to:", m_list, index=idx)

                        if st.button("🚀 Process & Sync to Cloud"):
                            with st.spinner("Syncing to Google Sheets..."):
                                # 1. Update Column Maps
                                new_cmap = pd.DataFrame([[target_chan, p_col, q_col, r_col, d_col, v_col]], columns=col_map_df.columns)
                                col_map_df = pd.concat([col_map_df[col_map_df['channel'] != target_chan], new_cmap])
                                conn.update(spreadsheet=SHEET_URL, worksheet="col_map", data=col_map_df)

                                # 2. Update Item Maps
                                for k, v in sku_mapping.items():
                                    item_map_df = pd.concat([item_map_df[item_map_df['raw_name'] != k], pd.DataFrame([[k, v]], columns=["raw_name", "master_name"])])
                                conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=item_map_df)

                                # 3. Append Sales
                                p_rows = []
                                for _, r in raw_df.iterrows():
                                    dt = pd.to_datetime(r[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                                    key = f"{r[p_col]} | {r[v_col]}" if v_col != "None" else str(r[p_col])
                                    p_rows.append([dt, target_chan, sku_mapping[key], clean_num(r[q_col]), clean_num(r[r_col])])
                                
                                new_sales = pd.DataFrame(p_rows, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                                history_df = pd.concat([history_df, new_sales]).drop_duplicates()
                                conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=history_df)
                                
                                st.cache_data.clear()
                                st.success("Data successfully synced to Cloud!")
                                st.rerun()

        # TAB 3: CONFIGURATION
        with tabs[2]:
            st.subheader("Manage Master Lists")
            c1, c2 = st.columns(2)
            with c1:
                st.write("📦 Master SKUs")
                new_sku = st.text_input("Add SKU")
                if st.button("Save SKU") and new_sku:
                    master_skus = pd.concat([master_skus, pd.DataFrame([[new_sku]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_skus", data=master_skus)
                    st.rerun()
                st.dataframe(master_skus, use_container_width=True)
            with c2:
                st.write("🏢 Channels")
                new_ch = st.text_input("Add Channel")
                if st.button("Save Channel") and new_ch:
                    master_chans = pd.concat([master_chans, pd.DataFrame([[new_ch]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_channels", data=master_chans)
                    st.rerun()
                st.dataframe(master_chans, use_container_width=True)
