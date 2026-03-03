import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px

# --- 1. DATABASE SETUP ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()

# Tables: Sales, Column Memory, Item Mapping, Master SKUs, and Custom Channels
c.execute('CREATE TABLE IF NOT EXISTS sales (date TEXT, channel TEXT, item_name TEXT, qty_sold REAL, revenue REAL)')
c.execute('CREATE TABLE IF NOT EXISTS col_map (channel TEXT PRIMARY KEY, item_col TEXT, qty_col TEXT, rev_col TEXT, date_col TEXT)')
c.execute('CREATE TABLE IF NOT EXISTS item_map (raw_name TEXT PRIMARY KEY, master_name TEXT)')
c.execute('CREATE TABLE IF NOT EXISTS master_skus (name TEXT PRIMARY KEY)')
c.execute('CREATE TABLE IF NOT EXISTS master_channels (name TEXT PRIMARY KEY)')
conn.commit()

# Seed initial channels if empty
if not c.execute("SELECT name FROM master_channels").fetchall():
    initial_chans = [("Blinkit",), ("Swiggy",), ("Amazon Seller",), ("Amazon Vendor",), ("Big Basket",)]
    c.executemany("INSERT INTO master_channels VALUES (?)", initial_chans)
    conn.commit()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. ADMIN SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Admin Controls")
    if st.checkbox("Show Data Management"):
        st.warning("Danger Zone")
        if st.button("🗑️ Flush All Sales History"):
            c.execute("DELETE FROM sales"); conn.commit()
            st.success("History Cleared!"); st.rerun()
        if st.button("🔄 Reset All Mappings"):
            c.execute("DELETE FROM col_map"); c.execute("DELETE FROM item_map"); conn.commit()
            st.success("Mappings Reset!"); st.rerun()

# --- 3. DASHBOARD TABS ---
st.title("📈 Tertiary Sales Executive Dashboard")
view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
metric_label = "Revenue" if "Revenue" in view_metric else "Qty"

tab1, tab2, tab3 = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])

# --- TAB 1: ANALYTICS (TOTALS AT TOP) ---
with tab1:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    if not history_df.empty:
        # 1. Filters at Top
        f1, f2 = st.columns(2)
        with f1: sel_chan = st.multiselect("Filter Channels", sorted(history_df['channel'].unique()), default=history_df['channel'].unique())
        with f2: sel_item = st.multiselect("Filter Products", sorted(history_df['item_name'].unique()))

        mask = history_df['channel'].isin(sel_chan)
        if sel_item: mask &= history_df['item_name'].isin(sel_item)
        
        filtered = history_df[mask].copy()
        filtered['date_dt'] = pd.to_datetime(filtered['date'])
        filtered = filtered.sort_values('date_dt')
        
        # 2. GRAND TOTALS AT TOP
        total_val = filtered[target_col].sum()
        st.markdown(f"### Grand Total {metric_label}: :green[{total_val:,.2f}]")
        st.divider()

        # 3. STACKED BAR CHART
        color_theme = "item_name" if sel_item else "channel"
        chart_data = filtered.groupby(['date', color_theme])[target_col].sum().reset_index()
        
        fig = px.bar(chart_data, x="date", y=target_col, color=color_theme, barmode="stack", height=500)
        fig.update_xaxes(type='category', categoryorder='array', categoryarray=filtered['date'].unique(), tickangle=-45)
        fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
        
        totals = filtered.groupby('date')[target_col].sum().reset_index()
        fig.add_scatter(x=totals['date'], y=totals[target_col], text=totals[target_col].apply(lambda x: f'{x:,.0f}'),
                        mode='text', textposition='top center', showlegend=False)

        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(filtered.drop(columns=['date_dt']), use_container_width=True)
    else:
        st.info("No data found. Upload a file in the 'Smart Upload' tab to begin.")

# --- TAB 2: SMART UPLOAD ---
with tab2:
    chans = [r[0] for r in c.execute("SELECT name FROM master_channels ORDER BY name ASC").fetchall()]
    channel = st.selectbox("Select Channel", chans)
    uploaded_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        c_names = ["None"] + df.columns.tolist()
        
        res = c.execute("SELECT * FROM col_map WHERE channel = ?", (channel,)).fetchone()
        saved = {"item": 0, "qty": 0, "rev": 0, "date": 0}
        if res:
            saved = {k: (c_names.index(v) if v in c_names else 0) for k, v in zip(["item", "qty", "rev", "date"], res[1:])}

        st.markdown("### 🛠 Step 1: Confirm Columns")
        col1, col2 = st.columns(2)
        with col1:
            col_item = st.selectbox("Product Name Column", c_names, index=saved["item"])
            col_qty = st.selectbox("Quantity Column", c_names, index=saved["qty"])
        with col2:
            col_rev = st.selectbox("Revenue Column", c_names, index=saved["rev"])
            col_date = st.selectbox("Order Date Column", c_names, index=saved["date"])
        
        manual_date = st.date_input("Manual Date (if Date Column is 'None')")

        if col_item != "None":
            st.markdown("### 🛠 Step 2: Mapping")
            masters = [r[0] for r in c.execute("SELECT name FROM master_skus ORDER BY name ASC").fetchall()]
            raw_items = df[col_item].unique()
            mapping_updates = {}
            missing_masters = False

            for item in raw_items:
                known = c.execute("SELECT master_name FROM item_map WHERE raw_name = ?", (item,)).fetchone()
                if known:
                    mapping_updates[item] = known[0]
                else:
                    if not masters:
                        st.error("Go to 'Configuration' tab to define your Master SKU list first!")
                        missing_masters = True
                        break
                    mapping_updates[item] = st.selectbox(f"Map '{item}' to Master SKU:", masters, key=f"up_{item}")

            if st.button("🚀 Process & Save Data") and not missing_masters:
                c.execute("INSERT OR REPLACE INTO col_map VALUES (?, ?, ?, ?, ?)", (channel, col_item, col_qty, col_rev, col_date))
                for raw, master in mapping_updates.items():
                    c.execute("INSERT OR REPLACE INTO item_map VALUES (?, ?)", (raw, master))
                conn.commit()

                final_data = []
                for _, row in df.iterrows():
                    row_date = str(manual_date)
                    if col_date != "None":
                        try: row_date = pd.to_datetime(row[col_date]).strftime("%Y-%m-%d")
                        except: pass
                    final_data.append({'date': row_date, 'channel': channel, 'item_name': mapping_updates.get(row[col_item], row[col_item]), 'qty_sold': clean_num(row[col_qty]), 'revenue': clean_num(row[col_rev])})
                
                upload_df = pd.DataFrame(final_data)
                for d in upload_df['date'].unique():
                    c.execute("DELETE FROM sales WHERE date = ? AND channel = ?", (d, channel))
                upload_df.to_sql('sales', conn, if_exists='append', index=False)
                conn.commit()
                st.success("History Updated!"); st.rerun()

# --- TAB 3: CONFIGURATION (SKUs & CHANNELS) ---
with tab3:
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("📦 Master SKU List")
        new_sku = st.text_input("Add New SKU")
        if st.button("Add SKU"):
            if new_sku: 
                try: 
                    c.execute("INSERT INTO master_skus VALUES (?)", (new_sku.strip(),))
                    conn.commit()
                    st.rerun()
                except: st.error("Exists!")
        
        m_list = [r[0] for r in c.execute("SELECT name FROM master_skus ORDER BY name ASC").fetchall()]
        st.dataframe(pd.DataFrame(m_list, columns=["Product Name"]), height=300, use_container_width=True)

    with c2:
        st.subheader("🏢 Sales Channels")
        new_chan = st.text_input("Add New Channel (e.g. Zepto)")
        if st.button("Add Channel"):
            if new_chan:
                try:
                    c.execute("INSERT INTO master_channels VALUES (?)", (new_chan.strip(),))
                    conn.commit()
                    st.rerun()
                except: st.error("Exists!")
        
        c_list = [r[0] for r in c.execute("SELECT name FROM master_channels ORDER BY name ASC").fetchall()]
        st.dataframe(pd.DataFrame(c_list, columns=["Channel Name"]), height=300, use_container_width=True)
