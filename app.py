import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px

# --- 1. DATABASE SETUP ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS sales 
             (date TEXT, channel TEXT, item_name TEXT, qty_sold REAL, revenue REAL)''')
c.execute('''CREATE TABLE IF NOT EXISTS col_map 
             (channel TEXT PRIMARY KEY, item_col TEXT, qty_col TEXT, rev_col TEXT, date_col TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS item_map 
             (raw_name TEXT PRIMARY KEY, master_name TEXT)''')
conn.commit()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. ADMIN SIDEBAR ---
with st.sidebar:
    st.header("Admin Controls")
    if st.checkbox("Show Data Management"):
        st.warning("Danger Zone")
        if st.button("🗑️ Flush All Sales History"):
            c.execute("DELETE FROM sales"); conn.commit()
            st.success("History Cleared!"); st.rerun()
        if st.button("🔄 Clear All Mappings"):
            c.execute("DELETE FROM col_map"); c.execute("DELETE FROM item_map"); conn.commit()
            st.success("Memory Reset!"); st.rerun()

# --- 3. HEADER & METRIC TOGGLE ---
st.title("📊 Tertiary Sales Executive Dashboard")
view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
metric_label = "Revenue" if "Revenue" in view_metric else "Qty"

tab1, tab2 = st.tabs(["📤 Smart Upload & Mapping", "📊 Trend Analytics"])

# --- 4. SMART UPLOAD & MAPPING ---
with tab1:
    channel = st.selectbox("Select Channel", ["Blinkit", "Swiggy", "Amazon Seller", "Amazon Vendor", "Big Basket"])
    uploaded_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        c_names = ["None / Manual Selection"] + df.columns.tolist()
        
        res = c.execute("SELECT * FROM col_map WHERE channel = ?", (channel,)).fetchone()
        saved = {"item": 0, "qty": 0, "rev": 0, "date": 0}
        if res:
            saved = {k: (c_names.index(v) if v in c_names else 0) for k, v in zip(["item", "qty", "rev", "date"], res[1:])}

        st.markdown("### 🛠 Step 1: Confirm Column Mappings")
        col1, col2 = st.columns(2)
        with col1:
            col_item = st.selectbox("Product Name Column", c_names, index=saved["item"])
            col_qty = st.selectbox("Quantity Column", c_names, index=saved["qty"])
        with col2:
            col_rev = st.selectbox("Revenue Column", c_names, index=saved["rev"])
            col_date = st.selectbox("Order Date Column", c_names, index=saved["date"])
        
        manual_date = st.date_input("Manual Date (Only if Date Column is 'None')")

        if col_item != "None / Manual Selection":
            st.markdown("### 🛠 Step 2: SKU Mapping")
            existing_masters = sorted([r[0] for r in c.execute("SELECT DISTINCT master_name FROM item_map").fetchall()])
            unique_raw_items = df[col_item].unique()
            mapping_updates = {}
            
            for item in unique_raw_items:
                known = c.execute("SELECT master_name FROM item_map WHERE raw_name = ?", (item,)).fetchone()
                default_val = known[0] if known else item
                mapping_updates[item] = st.selectbox(f"Map: '{item}'", options=list(set([default_val] + existing_masters + ["+ ADD NEW SKU"])), key=f"map_{item}")
                if mapping_updates[item] == "+ ADD NEW SKU":
                    mapping_updates[item] = st.text_input(f"New Master Name for '{item}':", key=f"new_{item}")

            if st.button("🚀 Process & Save Data"):
                c.execute("INSERT OR REPLACE INTO col_map VALUES (?, ?, ?, ?, ?)", (channel, col_item, col_qty, col_rev, col_date))
                for raw, master in mapping_updates.items():
                    if master: c.execute("INSERT OR REPLACE INTO item_map VALUES (?, ?)", (raw, master))
                conn.commit()

                final_data = []
                for _, row in df.iterrows():
                    row_date = str(manual_date)
                    if col_date != "None / Manual Selection":
                        try: row_date = pd.to_datetime(row[col_date]).strftime("%Y-%m-%d")
                        except: pass
                    final_data.append({'date': row_date, 'channel': channel, 'item_name': mapping_updates.get(row[col_item], row[col_item]), 'qty_sold': clean_num(row[col_qty]), 'revenue': clean_num(row[col_rev])})
                
                upload_df = pd.DataFrame(final_data)
                for d in upload_df['date'].unique():
                    c.execute("DELETE FROM sales WHERE date = ? AND channel = ?", (d, channel))
                upload_df.to_sql('sales', conn, if_exists='append', index=False)
                conn.commit()
                st.success("Data Saved Successfully!"); st.rerun()

# --- 5. ANALYTICS (FIXED X-AXIS & SMART LABELS) ---
with tab2:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    if not history_df.empty:
        st.subheader("Filters")
        f1, f2 = st.columns(2)
        with f1: sel_chan = st.multiselect("Channels", sorted(history_df['channel'].unique()), default=history_df['channel'].unique())
        with f2: sel_item = st.multiselect("Products", sorted(history_df['item_name'].unique()))

        mask = history_df['channel'].isin(sel_chan)
        if sel_item: mask &= history_df['item_name'].isin(sel_item)
        
        # FIX: Ensure chronological order from left to right
        filtered = history_df[mask].copy()
        filtered['date_dt'] = pd.to_datetime(filtered['date'])
        filtered = filtered.sort_values('date_dt')
        
        color_theme = "item_name" if sel_item else "channel"
        
        # --- NEW GRAPH LOGIC: Integrated Totals ---
        # Group data by date and category for the stack
        chart_data = filtered.groupby(['date', color_theme])[target_col].sum().reset_index()
        
        fig = px.bar(
            chart_data, x="date", y=target_col, color=color_theme, 
            barmode="stack", title=f"Daily {metric_label} Performance",
            height=600, labels={target_col: metric_label, "date": "Date"}
        )
        
        # Format the numbers to be readable and attach them to the bars
        fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
        fig.update_xaxes(type='category', categoryorder='array', categoryarray=filtered['date'].unique())
        
        # Calculate daily totals for the top label
        totals = filtered.groupby('date')[target_col].sum().reset_index()
        
        # Add the total label layer properly synced with filters
        fig.add_scatter(
            x=totals['date'], y=totals[target_col], 
            text=totals[target_col].apply(lambda x: f'{x:,.0f}'),
            mode='text', textposition='top center', showlegend=False
        )

        st.plotly_chart(fig, use_container_width=True)
        
        st.divider()
        st.metric(f"Grand Total {metric_label}", f"{filtered[target_col].sum():,.2f}")
        st.dataframe(filtered.drop(columns=['date_dt']), use_container_width=True)
    else:
        st.info("No data yet. Upload a file in the first tab.")
