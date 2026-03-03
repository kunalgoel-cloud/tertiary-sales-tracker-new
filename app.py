import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px

# --- 1. DATABASE & PERSISTENT MEMORY SETUP ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()

# Tables for History, Column Mappings, and Item Mappings
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
        if st.button("🔄 Clear All Mappings (Reset Memory)"):
            c.execute("DELETE FROM col_map"); c.execute("DELETE FROM item_map"); conn.commit()
            st.success("Memory Reset!"); st.rerun()

# --- 3. HEADER & METRIC TOGGLE ---
st.title("📊 Tertiary Sales Executive Dashboard")
view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
target_col = "revenue" if "Revenue" in view_metric else "qty_sold"

tab1, tab2 = st.tabs(["📤 Smart Upload & Mapping", "📈 Trend Analytics"])

# --- 4. SMART UPLOAD & MAPPING ---
with tab1:
    channel = st.selectbox("Select Channel", ["Blinkit", "Swiggy", "Amazon Seller", "Amazon Vendor", "Big Basket"])
    uploaded_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        c_names = ["None / Manual Selection"] + df.columns.tolist()
        
        # Load remembered column mappings for this channel
        res = c.execute("SELECT * FROM col_map WHERE channel = ?", (channel,)).fetchone()
        saved = {"item": 0, "qty": 0, "rev": 0, "date": 0}
        if res:
            saved = {
                "item": c_names.index(res[1]) if res[1] in c_names else 0,
                "qty": c_names.index(res[2]) if res[2] in c_names else 0,
                "rev": c_names.index(res[3]) if res[3] in c_names else 0,
                "date": c_names.index(res[4]) if res[4] in c_names else 0
            }

        st.markdown("### 🛠 Step 1: Confirm Column Mappings")
        col_item = st.selectbox("Product Name Column", c_names, index=saved["item"])
        col_qty = st.selectbox("Quantity Column", c_names, index=saved["qty"])
        col_rev = st.selectbox("Revenue Column", c_names, index=saved["rev"])
        col_date = st.selectbox("Order Date Column (Select 'None' to use manual date below)", c_names, index=saved["date"])
        
        manual_date = st.date_input("Manual Date (Only used if Date Column is 'None')")

        if col_item != "None / Manual Selection":
            st.markdown("### 🛠 Step 2: Item SKU Mapping")
            unique_raw_items = df[col_item].unique()
            mapping_updates = {}
            
            # Identify New vs Known items
            new_items = []
            for item in unique_raw_items:
                known = c.execute("SELECT master_name FROM item_map WHERE raw_name = ?", (item,)).fetchone()
                if not known:
                    new_items.append(item)
                else:
                    mapping_updates[item] = known[0]

            if new_items:
                st.info(f"System found {len(new_items)} new item names. Please map them to your Master SKUs:")
                for item in new_items:
                    mapping_updates[item] = st.text_input(f"Map '{item}' to:", value=item, key=f"new_{item}")
            else:
                st.success("✅ All items in this file are already mapped in memory!")

            if st.button("🚀 Process & Save Data"):
                # A. Update Persistent Memory
                c.execute("INSERT OR REPLACE INTO col_map VALUES (?, ?, ?, ?, ?)", 
                          (channel, col_item, col_qty, col_rev, col_date))
                for raw, master in mapping_updates.items():
                    c.execute("INSERT OR REPLACE INTO item_map VALUES (?, ?)", (raw, master))
                conn.commit()

                # B. Prepare Data Rows
                final_data = []
                for _, row in df.iterrows():
                    # Determine Date
                    row_date = str(manual_date)
                    if col_date != "None / Manual Selection":
                        row_date = pd.to_datetime(row[col_date]).strftime("%Y-%m-%d")
                    
                    final_data.append({
                        'date': row_date,
                        'channel': channel,
                        'item_name': mapping_updates.get(row[col_item], row[col_item]),
                        'qty_sold': clean_num(row[col_qty]),
                        'revenue': clean_num(row[col_rev])
                    })
                
                upload_df = pd.DataFrame(final_data)
                
                # C. Handle Overrides: Delete existing for specific dates found in this file
                dates_in_file = upload_df['date'].unique()
                for d in dates_in_file:
                    c.execute("DELETE FROM sales WHERE date = ? AND channel = ?", (d, channel))
                
                upload_df.to_sql('sales', conn, if_exists='append', index=False)
                conn.commit()
                st.success(f"Successfully processed {len(upload_df)} entries and updated memory!")
                st.rerun()

# --- 5. ANALYTICS ---
with tab2:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    if not history_df.empty:
        st.subheader("Filters")
        f1, f2 = st.columns(2)
        with f1:
            sel_chan = st.multiselect("Channels", sorted(history_df['channel'].unique()), default=history_df['channel'].unique())
        with f2:
            sel_item = st.multiselect("Products", sorted(history_df['item_name'].unique()))

        mask = history_df['channel'].isin(sel_chan)
        if sel_item: mask &= history_df['item_name'].isin(sel_item)
        
        filtered = history_df[mask].sort_values('date')
        
        # Summary Metric
        st.metric(f"Total {view_metric}", f"{filtered[target_col].sum():,.2f}")

        fig = px.bar(
            filtered, x="date", y=target_col, color="item_name" if sel_item else "channel", 
            barmode="group", text=target_col
        )
        fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_xaxes(type='category', title="Date")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(filtered, use_container_width=True)
    else:
        st.info("No history found. Go to the 'Smart Upload' tab to add data.")
