import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px

# --- 1. DATABASE SETUP ---
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS sales 
             (date TEXT, channel TEXT, item_name TEXT, qty_sold REAL, revenue REAL)''')
conn.commit()

# --- 2. CLEANING FUNCTION ---
def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

st.set_page_config(page_title="Tertiary Sales Hub", layout="wide")
st.title("📈 Tertiary Sales Analytics")

# --- 3. DATA ENTRY TAB & ANALYTICS TAB ---
tab1, tab2 = st.tabs(["📤 Upload Data", "📊 View Analytics"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        channel = st.selectbox("Select Channel", ["Blinkit", "Swiggy", "Amazon Seller", "Amazon Vendor", "Big Basket"])
    with col2:
        date = st.date_input("Select Date")

    uploaded_file = st.file_uploader("Upload CSV or Excel data", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        st.subheader("Map Your Columns")
        col_names = df.columns.tolist()
        item_col = st.selectbox("Item Name / SKU Column", col_names)
        qty_col = st.selectbox("Quantity Sold Column", col_names)
        rev_col = st.selectbox("Revenue Column", col_names)

        if st.button("Save Data to History"):
            final_df = pd.DataFrame()
            final_df['item_name'] = df[item_col]
            final_df['qty_sold'] = df[qty_col].apply(clean_num)
            final_df['revenue'] = df[rev_col].apply(clean_num)
            final_df = final_df.groupby('item_name').sum().reset_index()
            final_df['date'] = date.strftime("%Y-%m-%d")
            final_df['channel'] = channel
            final_df.to_sql('sales', conn, if_exists='append', index=False)
            st.success(f"Saved {len(final_df)} items to history!")

with tab2:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    
    if not history_df.empty:
        # --- FILTERS ---
        st.subheader("Filters")
        f_col1, f_col2, f_col3 = st.columns(3)
        with f_col1:
            sel_channels = st.multiselect("Channels", history_df['channel'].unique(), default=history_df['channel'].unique())
        with f_col2:
            sel_items = st.multiselect("SKUs/Items", history_df['item_name'].unique())
        with f_col3:
            # Date range filter
            min_date = pd.to_datetime(history_df['date']).min()
            max_date = pd.to_datetime(history_df['date']).max()
            sel_date_range = st.date_input("Date Range", [min_date, max_date])

        # Apply Filters
        mask = history_df['channel'].isin(sel_channels)
        if sel_items:
            mask &= history_df['item_name'].isin(sel_items)
        if len(sel_date_range) == 2:
            mask &= (pd.to_datetime(history_df['date']) >= pd.to_datetime(sel_date_range[0])) & \
                    (pd.to_datetime(history_df['date']) <= pd.to_datetime(sel_date_range[1]))
        
        filtered_df = history_df[mask].sort_values(by="date")

        # --- GRAPH ---
        st.subheader("Sales Trend")
        # Aggregating for the graph
        chart_df = filtered_df.groupby(['date', 'channel'])['revenue'].sum().reset_index()
        fig = px.line(chart_df, x='date', y='revenue', color='channel', markers=True)
        st.plotly_chart(fig, use_container_width=True)

        # --- DOWNLOAD & DATA ---
        st.subheader("Raw Data & Download")
        st.dataframe(filtered_df, use_container_width=True)
        
        csv_data = filtered_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Filtered CSV",
            data=csv_data,
            file_name=f"sales_export_{date.today()}.csv",
            mime="text/csv"
        )
    else:
        st.info("No history found. Go to the 'Upload Data' tab to add some!")