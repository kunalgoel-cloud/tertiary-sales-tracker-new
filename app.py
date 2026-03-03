import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import re
import plotly.express as px
from datetime import datetime, timedelta
import io

# --- 1. SETTINGS & AUTHENTICATION ---
st.set_page_config(page_title="Mamanourish Sales Portal", layout="wide")

def check_password():
    """Returns True if the user had the correct password and sets their role."""
    USER_PASSWORDS = {
        "admin": "mamaadmin2026",    # Full Access
        "viewer": "mamaview2026"     # View Only
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

if check_password():
    # --- 2. GOOGLE SHEETS CONNECTION ---
    # The connection automatically uses the [connections.gsheets] from your Secrets
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"
    conn = st.connection("gsheets", type=GSheetsConnection)

    # Sidebar
    with st.sidebar:
        st.header(f"👤 {st.session_state['user_role'].upper()} MODE")
        if st.button("🔄 Refresh Cloud Data"):
            st.cache_data.clear()
            st.rerun()
        st.divider()
        if st.button("🚪 Logout"):
            del st.session_state["password_correct"]
            st.rerun()

    # Data Loading Helper with Error Handling
    def load_sheet(name, columns):
        try:
            df = conn.read(spreadsheet=SHEET_URL, worksheet=name, ttl=0) 
            return df if df is not None and not df.empty else pd.DataFrame(columns=columns)
        except Exception as e:
            # Silently return empty DF if tab doesn't exist yet, or warn admin
            if st.session_state["user_role"] == "admin":
                st.sidebar.warning(f"Tab '{name}' not found or inaccessible.")
            return pd.DataFrame(columns=columns)

    def clean_num(val):
        if pd.isna(val): return 0.0
        res = re.sub(r'[^\d.]', '', str(val))
        return float(res) if res else 0.0

    # Load Data
    history_df = load_sheet("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
    col_map_df = load_sheet("col_map", ["channel", "item_col", "qty_col", "rev_col", "date_col", "var_col"])
    item_map_df = load_sheet("item_map", ["raw_name", "master_name"])
    master_skus = load_sheet("master_skus", ["name"])
    master_chans = load_sheet("master_channels", ["name"])

    st.title("📈 Mamanourish Sales Intelligence")

    # --- 3. TAB MANAGEMENT ---
    if st.session_state["user_role"] == "admin":
        tabs = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])
    else:
        tabs = st.tabs(["📊 Trend Analytics"])
        st.info("💡 You are in View-Only mode.")

    # --- TAB 1: ANALYTICS ---
    with tabs[0]:
        if history_df.empty:
            st.info("No data found. Admin needs to upload sales data first.")
        else:
            view_metric = st.radio("Display Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
            target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
            currency = "₹" if "Revenue" in view_metric else ""

            history_df['date_dt'] = pd.to_datetime(history_df['date'])
            today = datetime.now().date()
            
            # Filters
            c1, c2, c3 = st.columns([2, 2, 2])
            with c1:
                time_preset = st.selectbox("Timeframe:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"])
            
            if time_preset == "Last 7 Days": start_d, end_d = today - timedelta(days=6), today
            elif time_preset == "Last 30 Days": start_d, end_d = today - timedelta(days=29), today
            elif time_preset == "Month to Date": start_d, end_d = today.replace(day=1), today
            elif time_preset == "All Time": start_d, end_d = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
            else:
                dr = st.date_input("Custom Range", value=(today - timedelta(days=7), today))
                start_d, end_d = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

            mask = (history_df['date_dt'].dt.date >= start_d) & (history_df['date_dt'].dt.date <= end_d)
            filtered = history_df[mask].copy()

            with c2:
                chans = sorted(filtered['channel'].unique())
                sel_chan = st.multiselect("Channels", chans, default=chans)
            with c3:
                items = sorted(filtered[filtered['channel'].isin(sel_chan)]['item_name'].unique())
                sel_item = st.multiselect("Products", items)

            # Final Filter apply
            f_mask = filtered['channel'].isin(sel_chan)
            if sel_item: f_mask &= filtered['item_name'].isin(sel_item)
            final_df = filtered[f_mask]

            # Metrics
            num_days = (end_d - start_d).days + 1
            total_val = final_df[target_col].sum()
            drr = total_val / num_days if num_days > 0 else 0
            
            m1, m2 = st.columns(2)
            m1.metric(f"Total {view_metric}", f"{currency}{total_val:,.2f}")
            m2.metric("Daily Run Rate (DRR)", f"{currency}{drr:,.2f}")

            # Chart
            if not final_df.empty:
                color_group = "item_name" if sel_item else "channel"
                plot_df = final_df.groupby(['date', color_group])[target_col].sum().reset_index()
                fig = px.bar(plot_df, x="date", y=target_col, color=color_group, barmode="stack", height=500)
                fig.add_hline(y=drr, line_dash="dash", line_color="red", annotation_text="Avg DRR")
                st.plotly_chart(fig, use_container_width=True)

                # Export
                towrite = io.BytesIO()
                final_df.drop(columns=['date_dt']).to_excel(towrite, index=False, engine='xlsxwriter')
                towrite.seek(0)
                st.download_button("📥 Download Excel Snapshot", towrite, f"mamanourish_export_{datetime.now().strftime('%Y%m%d')}.xlsx")

    # --- ADMIN TABS ---
    if st.session_state["user_role"] == "admin":
        # TAB 2: SMART UPLOAD
        with tabs[1]:
            if master_chans.empty: 
                st.warning("⚠️ Go to Configuration and add at least one Channel first.")
            else:
                target_chan = st.selectbox("Select Channel for Upload", master_chans['name'].tolist())
                up_file = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx"])
                
                if up_file:
                    raw_df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                    cols = ["None"] + raw_df.columns.tolist()
                    
                    # Get saved mapping for this channel
                    mem = col_map_df[col_map_df['channel'] == target_chan]
                    def get_idx(key):
                        val = mem[key].iloc[0] if not mem.empty and mem[key].iloc[0] in cols else "None"
                        return cols.index(val)

                    st.subheader("Map Columns")
                    c1, c2, c3 = st.columns(3)
                    ci = c1.selectbox("Product Name", cols, index=get_idx("item_col"))
                    cq = c1.selectbox("Quantity", cols, index=get_idx("qty_col"))
                    cr = c2.selectbox("Revenue", cols, index=get_idx("rev_col"))
                    cd = c2.selectbox("Date", cols, index=get_idx("date_col"))
                    cv = c3.selectbox("Variant (Optional)", cols, index=get_idx("var_col"))
                    manual_date = c3.date_input("Manual Date (if date column is None)")

                    if ci != "None" and not master_skus.empty:
                        st.subheader("Map Products to Master SKUs")
                        unique_raw = raw_df[[ci, cv]].drop_duplicates() if cv != "None" else raw_df[[ci]].drop_duplicates()
                        sku_mapping = {}
                        m_skus = master_skus['name'].tolist()
                        
                        for _, row in unique_raw.iterrows():
                            raw_label = f"{row[ci]} | {row[cv]}" if cv != "None" else str(row[ci])
                            existing = item_map_df[item_map_df['raw_name'] == raw_label]
                            start_idx = m_skus.index(existing['master_name'].iloc[0]) if not existing.empty else 0
                            sku_mapping[raw_label] = st.selectbox(f"Map: {raw_label}", m_skus, index=start_idx)

                        if st.button("🚀 Process & Sync to Cloud"):
                            # 1. Update Column Maps
                            new_c_map = pd.DataFrame([[target_chan, ci, cq, cr, cd, cv]], columns=col_map_df.columns)
                            col_map_df = pd.concat([col_map_df[col_map_df['channel'] != target_chan], new_c_map])
                            conn.update(spreadsheet=SHEET_URL, worksheet="col_map", data=col_map_df)

                            # 2. Update Item Mappings
                            for k, v in sku_mapping.items():
                                item_map_df = pd.concat([item_map_df[item_map_df['raw_name'] != k], pd.DataFrame([[k, v]], columns=["raw_name", "master_name"])])
                            conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=item_map_df)

                            # 3. Transform Sales Data
                            processed_rows = []
                            for _, r in raw_df.iterrows():
                                date_val = pd.to_datetime(r[cd]).strftime("%Y-%m-%d") if cd != "None" else str(manual_date)
                                raw_key = f"{r[ci]} | {r[cv]}" if cv != "None" else str(r[ci])
                                processed_rows.append([date_val, target_chan, sku_mapping[raw_key], clean_num(r[cq]), clean_num(r[cr])])
                            
                            new_sales = pd.DataFrame(processed_rows, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                            # Deduplicate (Remove old data for the same dates/channel)
                            dates_to_clear = new_sales['date'].unique()
                            history_df = pd.concat([history_df[~((history_df['date'].isin(dates_to_clear)) & (history_df['channel'] == target_chan))], new_sales])
                            
                            conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=history_df)
                            st.cache_data.clear()
                            st.success("Cloud Data Updated!")
                            st.rerun()

        # TAB 3: CONFIGURATION
        with tabs[2]:
            st.info("Define your Master lists here to ensure clean data mapping.")
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("📦 Master SKU List")
                new_sku = st.text_input("Add New SKU")
                if st.button("Save SKU") and new_sku:
                    master_skus = pd.concat([master_skus, pd.DataFrame([[new_sku.strip()]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_skus", data=master_skus)
                    st.rerun()
                st.dataframe(master_skus, use_container_width=True)
            with c2:
                st.subheader("🏢 Master Channels")
                new_chan = st.text_input("Add New Channel")
                if st.button("Save Channel") and new_chan:
                    master_chans = pd.concat([master_chans, pd.DataFrame([[new_chan.strip()]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_channels", data=master_chans)
                    st.rerun()
                st.dataframe(master_chans, use_container_width=True)
