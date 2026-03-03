import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. SETUP & CONNECTION ---
st.set_page_config(page_title="Executive Sales Tracker (Cloud)", layout="wide")
st.title("📈 Cloud-Synced Sales Dashboard")

# Your specific Google Sheet Link
SHEET_URL = "https://docs.google.com/spreadsheets/d/1V2KD7IU7BaHZnkXH96GhGiYp-nW_wFdTFTaNPGM5DX0/edit?usp=sharing"

# Establish Google Sheets Connection
conn = st.connection("gsheets", type=GSheetsConnection)

# Helper function to read sheets safely with fallback to empty dataframes
def load_sheet(name, columns):
    try:
        df = conn.read(spreadsheet=SHEET_URL, worksheet=name, ttl=0) # ttl=0 ensures live data
        if df is None or df.empty: 
            return pd.DataFrame(columns=columns)
        return df
    except Exception:
        return pd.DataFrame(columns=columns)

# Load Live Data from Cloud
history_df = load_sheet("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
col_map_df = load_sheet("col_map", ["channel", "item_col", "qty_col", "rev_col", "date_col", "var_col"])
item_map_df = load_sheet("item_map", ["raw_name", "master_name"])
master_skus = load_sheet("master_skus", ["name"])
master_chans = load_sheet("master_channels", ["name"])

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. NAVIGATION TABS ---
tab1, tab2, tab3 = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])

# --- TAB 1: ANALYTICS ---
with tab1:
    view_metric = st.radio("Display By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
    target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
    metric_label = "Revenue" if "Revenue" in view_metric else "Qty"
    currency = "₹" if "Revenue" in view_metric else ""

    if not history_df.empty:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        today = datetime.now().date()
        
        # Filters Header
        q1, q2 = st.columns([3, 1])
        with q1:
            time_preset = st.radio("Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
        with q2:
            show_labels = st.checkbox("Show Data Labels", value=True)

        # Time Logic
        if time_preset == "Last 7 Days": start_d, end_d = today - timedelta(days=6), today
        elif time_preset == "Last 30 Days": start_d, end_d = today - timedelta(days=29), today
        elif time_preset == "Month to Date": start_d, end_d = today.replace(day=1), today
        elif time_preset == "All Time": start_d, end_d = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
        else:
            dr = st.date_input("Select Custom Range", value=(today - timedelta(days=7), today))
            start_d, end_d = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

        num_days = (end_d - start_d).days + 1
        mask = (history_df['date_dt'].dt.date >= start_d) & (history_df['date_dt'].dt.date <= end_d)
        range_df = history_df[mask].copy()

        # Dynamic Filters
        c1, c2 = st.columns(2)
        with c1: 
            chans = sorted(range_df['channel'].unique())
            sel_chan = st.multiselect("Filter Channels", chans, default=chans)
        with c2:
            items = sorted(range_df[range_df['channel'].isin(sel_chan)]['item_name'].unique())
            sel_item = st.multiselect("Filter Products", items)

        f_mask = range_df['channel'].isin(sel_chan)
        if sel_item: f_mask &= range_df['item_name'].isin(sel_item)
        filtered = range_df[f_mask]

        # Top Metric Cards
        total = filtered[target_col].sum()
        drr = total / num_days if num_days > 0 else 0
        m1, m2 = st.columns(2)
        m1.metric(f"Total {metric_label}", f"{currency}{total:,.2f}")
        m2.metric("Average DRR", f"{currency}{drr:,.2f}", help=f"Average per day over {num_days} days")

        # Visualization
        if not filtered.empty:
            color = "item_name" if sel_item else "channel"
            plot_df = filtered.groupby(['date', color])[target_col].sum().reset_index()
            # Timeline Zero-Fill
            all_dates = pd.date_range(start_d, end_d).strftime('%Y-%m-%d').tolist()
            groups = plot_df[color].unique()
            template = pd.MultiIndex.from_product([all_dates, groups], names=['date', color]).to_frame(index=False)
            plot_df = pd.merge(template, plot_df, on=['date', color], how='left').fillna(0)

            fig = px.bar(plot_df, x="date", y=target_col, color=color, barmode="stack", height=500)
            fig.add_hline(y=drr, line_dash="dash", line_color="red", annotation_text="Avg DRR Line")
            if show_labels:
                fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(filtered.drop(columns=['date_dt']), use_container_width=True)
    else:
        st.info("The database is currently empty. Please use the 'Smart Upload' tab to add sales data.")

# --- TAB 2: SMART UPLOAD ---
with tab2:
    if master_chans.empty:
        st.warning("Please add Sales Channels in the 'Configuration' tab first.")
    else:
        channel = st.selectbox("Select Channel for Upload", master_chans['name'].tolist())
        uploaded_file = st.file_uploader("Upload Sales File (CSV or Excel)", type=["csv", "xlsx"])

        if uploaded_file:
            df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
            cols = ["None"] + df.columns.tolist()
            
            # Auto-load previous column mapping for this channel
            mem = col_map_df[col_map_df['channel'] == channel]
            saved = {k: (cols.index(mem[k].iloc[0]) if not mem.empty and mem[k].iloc[0] in cols else 0) 
                     for k in ["item_col", "qty_col", "rev_col", "date_col", "var_col"]}

            st.markdown("### 🛠 Step 1: Confirm Column Headers")
            c1, c2, c3 = st.columns(3)
            ci = c1.selectbox("Product Name Column", cols, index=saved["item_col"])
            cq = c1.selectbox("Quantity Column", cols, index=saved["qty_col"])
            cr = c2.selectbox("Revenue Column", cols, index=saved["rev_col"])
            cd = c2.selectbox("Order Date Column", cols, index=saved["date_col"])
            cv = c3.selectbox("Variant Column (Optional)", cols, index=saved["var_col"])
            md = c3.date_input("Manual Date (If no date column exists)")

            if ci != "None" and not master_skus.empty:
                st.markdown("### 🛠 Step 2: Map to Master SKUs")
                df_unique = df[[ci, cv]].drop_duplicates() if cv != "None" else df[[ci]].drop_duplicates()
                mapping_updates = {}
                m_list = master_skus['name'].tolist()
                
                for _, row in df_unique.iterrows():
                    key = f"{row[ci]} | {row[cv]}" if cv != "None" else str(row[ci])
                    known = item_map_df[item_map_df['raw_name'] == key]
                    mapping_updates[key] = st.selectbox(f"Map '{key}' to:", m_list, 
                                                       index=(m_list.index(known['master_name'].iloc[0]) if not known.empty and known['master_name'].iloc[0] in m_list else 0),
                                                       key=f"map_{key}")

                if st.button("🚀 Sync Data to Cloud"):
                    with st.spinner("Writing to Google Sheets..."):
                        # 1. Update Column Mapping Table
                        new_col_row = pd.DataFrame([[channel, ci, cq, cr, cd, cv]], columns=col_map_df.columns)
                        updated_col_map = pd.concat([col_map_df[col_map_df['channel'] != channel], new_col_row])
                        conn.update(spreadsheet=SHEET_URL, worksheet="col_map", data=updated_col_map)

                        # 2. Update Item Mapping Memory
                        new_mapping_rows = []
                        for k, v in mapping_updates.items():
                            new_mapping_rows.append([k, v])
                        new_map_df = pd.DataFrame(new_mapping_rows, columns=["raw_name", "master_name"])
                        updated_item_map = pd.concat([item_map_df, new_map_df]).drop_duplicates(subset=['raw_name'], keep='last')
                        conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=updated_item_map)

                        # 3. Process and Append Sales
                        final_rows = []
                        for _, row in df.iterrows():
                            d_val = pd.to_datetime(row[cd]).strftime("%Y-%m-%d") if cd != "None" else str(md)
                            key = f"{row[ci]} | {row[cv]}" if cv != "None" else str(row[ci])
                            final_rows.append([d_val, channel, mapping_updates[key], clean_num(row[cq]), clean_num(row[cr])])
                        
                        new_sales = pd.DataFrame(final_rows, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                        # Logic: Remove existing records for the same dates+channel to prevent duplicates on re-upload
                        updated_history = pd.concat([history_df[~((history_df['date'].isin(new_sales['date'])) & (history_df['channel'] == channel))], new_sales])
                        conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=updated_history)
                        
                        st.success("Cloud Sync Complete! Data is now safe in Google Sheets."); st.rerun()
            elif master_skus.empty:
                st.error("Please add Master SKUs in the Configuration tab before mapping.")

# --- TAB 3: CONFIGURATION ---
with tab3:
    st.info("Add your global products and sales channels here. These lists are stored in Google Sheets.")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📦 Master SKU Management")
        new_sku = st.text_input("New Master SKU Name")
        if st.button("Add SKU") and new_sku:
            updated_skus = pd.concat([master_skus, pd.DataFrame([[new_sku.strip()]], columns=["name"])]).drop_duplicates()
            conn.update(spreadsheet=SHEET_URL, worksheet="master_skus", data=updated_skus)
            st.success(f"Added {new_sku}"); st.rerun()
        st.dataframe(master_skus, use_container_width=True, height=400)
    with c2:
        st.subheader("🏢 Sales Channels")
        new_ch = st.text_input("New Channel Name (e.g. Blinkit)")
        if st.button("Add Channel") and new_ch:
            updated_chans = pd.concat([master_chans, pd.DataFrame([[new_ch.strip()]], columns=["name"])]).drop_duplicates()
            conn.update(spreadsheet=SHEET_URL, worksheet="master_channels", data=updated_chans)
            st.success(f"Added {new_ch}"); st.rerun()
        st.dataframe(master_chans, use_container_width=True, height=400)
