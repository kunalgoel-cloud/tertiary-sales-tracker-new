import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px

# --- 1. DATABASE & CONFIG ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS sales 
             (date TEXT, channel TEXT, item_name TEXT, qty_sold REAL, revenue REAL)''')
conn.commit()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. HEADER & METRIC TOGGLE ---
st.title("📊 Tertiary Sales Executive Dashboard")
view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
target_col = "revenue" if "Revenue" in view_metric else "qty_sold"

tab1, tab2 = st.tabs(["📤 Smart Upload & Mapping", "📈 Trend Analytics"])

# --- 3. UPLOAD & MAPPING LOGIC ---
with tab1:
    col1, col2 = st.columns(2)
    with col1:
        channel = st.selectbox("Channel", ["Blinkit", "Swiggy", "Amazon Seller", "Amazon Vendor", "Big Basket"])
    with col2:
        date = st.date_input("Data Date (For weekly, pick Start Date)")

    uploaded_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        
        st.markdown("### 🛠 Step 1: Match Columns")
        c_names = df.columns.tolist()
        col_item = st.selectbox("Which column has Product Names?", c_names)
        col_qty = st.selectbox("Which column has Quantity?", c_names)
        col_rev = st.selectbox("Which column has Sales/Revenue?", c_names)

        st.markdown("### 🛠 Step 2: Fix Item Names (Mapping)")
        st.caption("Correct the names below to match your Master List so the graphs group them correctly.")
        
        unique_file_items = df[col_item].unique()
        mapping_dict = {}
        # Create a clean interface for mapping
        map_col1, map_col2 = st.columns(2)
        for i, item in enumerate(unique_file_items):
            with (map_col1 if i % 2 == 0 else map_col2):
                mapping_dict[item] = st.text_input(f"File Name: {item}", value=item, key=f"map_{item}")

        # DUPLICATE CHECK
        history_df = pd.read_sql("SELECT * FROM sales", conn)
        is_duplicate = ((history_df['date'] == str(date)) & (history_df['channel'] == channel)).any()
        
        if is_duplicate:
            st.warning(f"⚠️ Data already exists for {channel} on {date}. Saving will add more rows.")

        if st.button("🚀 Process & Save to History"):
            final_df = pd.DataFrame()
            final_df['item_name'] = df[col_item].map(mapping_dict)
            final_df['qty_sold'] = df[col_qty].apply(clean_num)
            final_df['revenue'] = df[col_rev].apply(clean_num)
            
            # Consolidate and add metadata
            final_df = final_df.groupby('item_name').sum().reset_index()
            final_df['date'] = str(date)
            final_df['channel'] = channel
            
            final_df.to_sql('sales', conn, if_exists='append', index=False)
            st.success(f"Successfully saved {len(final_df)} products to history!")
            st.rerun()

# --- 4. ANALYTICS & BAR GRAPHS ---
with tab2:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    
    if not history_df.empty:
        st.subheader("Filters")
        f1, f2 = st.columns(2)
        with f1:
            sel_chan = st.multiselect("Channels", history_df['channel'].unique(), default=history_df['channel'].unique())
        with f2:
            sel_item = st.multiselect("Products (Leave blank for all)", history_df['item_name'].unique())

        # Filter Data
        mask = history_df['channel'].isin(sel_chan)
        if sel_item:
            mask &= history_df['item_name'].isin(sel_item)
        
        filtered = history_df[mask].sort_values('date')

        # DYNAMIC BAR CHART
        # If products are selected, show per-product colors. Otherwise, show per-channel colors.
        color_theme = "item_name" if sel_item else "channel"
        
        fig = px.bar(
            filtered, 
            x="date", 
            y=target_col, 
            color=color_theme,
            barmode="group",
            title=f"Total {view_metric} over Time",
            labels={target_col: view_metric, "date": "Date", "item_name": "Product", "channel": "Channel"}
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Total Summary")
        total_val = filtered[target_col].sum()
        st.metric(label=f"Total {view_metric}", value=f"{total_val:,.2f}")
        
        st.dataframe(filtered, use_container_width=True)
    else:
        st.info("Upload data in the first tab to see analytics.")
