import streamlit as st
import pandas as pd
import sqlite3
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. DATABASE SETUP & MIGRATION ---
st.set_page_config(page_title="Executive Sales Tracker", layout="wide")
conn = sqlite3.connect('sales_history.db', check_same_thread=False)
c = conn.cursor()

c.execute('CREATE TABLE IF NOT EXISTS sales (date TEXT, channel TEXT, item_name TEXT, qty_sold REAL, revenue REAL)')
c.execute('''CREATE TABLE IF NOT EXISTS col_map 
             (channel TEXT PRIMARY KEY, item_col TEXT, qty_col TEXT, rev_col TEXT, date_col TEXT, var_col TEXT)''')
c.execute('CREATE TABLE IF NOT EXISTS item_map (raw_name TEXT PRIMARY KEY, master_name TEXT)')
c.execute('CREATE TABLE IF NOT EXISTS master_skus (name TEXT PRIMARY KEY)')
c.execute('CREATE TABLE IF NOT EXISTS master_channels (name TEXT PRIMARY KEY)')
conn.commit()

try:
    c.execute("ALTER TABLE col_map ADD COLUMN var_col TEXT")
    conn.commit()
except sqlite3.OperationalError:
    pass 

if not c.execute("SELECT name FROM master_channels").fetchall():
    for ch in ["Blinkit", "Swiggy", "Amazon", "Big Basket"]:
        c.execute("INSERT OR IGNORE INTO master_channels VALUES (?)", (ch,))
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

# --- 3. MAIN DASHBOARD ---
st.title("📈 Tertiary Sales Executive Dashboard")
view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
metric_label = "Revenue" if "Revenue" in view_metric else "Qty"
currency_prefix = "₹" if "Revenue" in view_metric else ""

tab1, tab2, tab3 = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])

# --- TAB 1: ANALYTICS ---
with tab1:
    history_df = pd.read_sql("SELECT * FROM sales", conn)
    if not history_df.empty:
        history_df['date_dt'] = pd.to_datetime(history_df['date'])
        min_db_date, max_db_date = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
        today = datetime.now().date()

        st.subheader("Filters")
        q_col1, q_col2 = st.columns([3, 1])
        with q_col1:
            time_preset = st.radio("Quick Select Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
        with q_col2:
            show_labels = st.checkbox("Show Data Labels", value=True)

        if time_preset == "Last 7 Days": start_date, end_date = today - timedelta(days=6), today
        elif time_preset == "Last 30 Days": start_date, end_date = today - timedelta(days=29), today
        elif time_preset == "Month to Date": start_date, end_date = today.replace(day=1), today
        elif time_preset == "All Time": start_date, end_date = min_db_date, max_db_date
        else:
            dr = st.date_input("Custom Range", value=(min_db_date, max_db_date))
            start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (min_db_date, max_db_date)

        # Calculate number of days in range for DRR
        num_days = (end_date - start_date).days + 1

        mask = (history_df['date_dt'].dt.date >= start_date) & (history_df['date_dt'].dt.date <= end_date)
        range_df = history_df[mask].copy()

        f1, f2 = st.columns(2)
        available_chans = sorted(range_df['channel'].unique()) if not range_df.empty else []
        with f1: sel_chan = st.multiselect("Filter Channels", available_chans, default=available_chans)
        
        chan_mask = range_df['channel'].isin(sel_chan)
        available_items = sorted(range_df[chan_mask]['item_name'].unique()) if not range_df[chan_mask].empty else []
        with f2: sel_item = st.multiselect("Filter Products", available_items)

        final_mask = chan_mask
        if sel_item: final_mask &= range_df['item_name'].isin(sel_item)
        filtered = range_df[final_mask].copy()

        # --- TOP METRICS (Total + DRR) ---
        total_val = filtered[target_col].sum()
        avg_drr = total_val / num_days if num_days > 0 else 0

        m1, m2 = st.columns(2)
        with m1:
            st.metric(label=f"Total {metric_label}", value=f"{currency_prefix}{total_val:,.2f}")
        with m2:
            st.metric(label=f"Average DRR (Daily Run Rate)", value=f"{currency_prefix}{avg_drr:,.2f}", help=f"Total divided by {num_days} days")
        
        st.divider()

        if not filtered.empty:
            color_theme = "item_name" if sel_item else "channel"
            plot_df = filtered.groupby(['date', color_theme])[target_col].sum().reset_index()
            all_dates = pd.date_range(start_date, end_date).strftime('%Y-%m-%d').tolist()
            all_groups = plot_df[color_theme].unique()
            
            if len(all_groups) > 0:
                template = pd.MultiIndex.from_product([all_dates, all_groups], names=['date', color_theme]).to_frame(index=False)
                plot_df = pd.merge(template, plot_df, on=['date', color_theme], how='left').fillna(0).sort_values('date')

                fig = px.bar(plot_df, x="date", y=target_col, color=color_theme, barmode="stack", height=550)
                fig.update_xaxes(type='category', tickangle=-45)
                
                # Add a horizontal line for DRR
                fig.add_hline(y=avg_drr, line_dash="dash", line_color="red", 
                              annotation_text=f"Avg DRR: {avg_drr:,.0f}", 
                              annotation_position="top left")

                if show_labels:
                    fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
                    totals = plot_df.groupby('date')[target_col].sum().reset_index()
                    fig.add_scatter(x=totals['date'], y=totals[target_col], text=totals[target_col].apply(lambda x: f'{x:,.0f}' if x > 0 else ''), mode='text', textposition='top center', showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(filtered.drop(columns=['date_dt']), use_container_width=True)
    else:
        st.info("No history found. Upload data to begin.")

# --- TAB 2: SMART UPLOAD ---
with tab2:
    chans = [r[0] for r in c.execute("SELECT name FROM master_channels ORDER BY name ASC").fetchall()]
    channel = st.selectbox("Select Channel", chans)
    uploaded_file = st.file_uploader("Upload Sales File", type=["csv", "xlsx"])

    if uploaded_file:
        df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
        c_names = ["None"] + df.columns.tolist()
        
        res = c.execute("SELECT * FROM col_map WHERE channel = ?", (channel,)).fetchone()
        saved = {"item": 0, "qty": 0, "rev": 0, "date": 0, "var": 0}
        if res:
            saved = {k: (c_names.index(v) if v in c_names else 0) for k, v in zip(["item", "qty", "rev", "date", "var"], res[1:])}

        st.markdown("### 🛠 Step 1: Confirm Columns")
        col1, col2, col3 = st.columns(3)
        with col1:
            col_item = st.selectbox("Product Name Column", c_names, index=saved["item"])
            col_qty = st.selectbox("Quantity Column", c_names, index=saved["qty"])
        with col2:
            col_rev = st.selectbox("Revenue Column", c_names, index=saved["rev"])
            col_date = st.selectbox("Order Date Column", c_names, index=saved["date"])
        with col3:
            col_var = st.selectbox("Variant Column (Optional)", c_names, index=saved["var"])
            manual_date = st.date_input("Manual Date (if Date Column is 'None')")

        if col_item != "None":
            st.markdown("### 🛠 Step 2: Mapping")
            masters = [r[0] for r in c.execute("SELECT name FROM master_skus ORDER BY name ASC").fetchall()]
            
            if col_var != "None":
                df_unique = df[[col_item, col_var]].drop_duplicates()
                df_unique['mapping_key'] = df_unique[col_item].astype(str) + " | " + df_unique[col_var].astype(str)
                df_unique['display_label'] = df_unique[col_item].astype(str) + " (" + df_unique[col_var].astype(str) + ")"
            else:
                df_unique = df[[col_item]].drop_duplicates()
                df_unique['mapping_key'] = df_unique[col_item].astype(str)
                df_unique['display_label'] = df_unique[col_item].astype(str)

            mapping_updates = {}
            for _, row in df_unique.iterrows():
                key, label = row['mapping_key'], row['display_label']
                known = c.execute("SELECT master_name FROM item_map WHERE raw_name = ?", (key,)).fetchone()
                mapping_updates[key] = st.selectbox(f"Map: '{label}'", masters, index=(masters.index(known[0]) if known and known[0] in masters else 0), key=f"up_{key}")

            if st.button("🚀 Process & Save Data"):
                c.execute("INSERT OR REPLACE INTO col_map VALUES (?, ?, ?, ?, ?, ?)", (channel, col_item, col_qty, col_rev, col_date, col_var))
                for raw, master in mapping_updates.items():
                    c.execute("INSERT OR REPLACE INTO item_map VALUES (?, ?)", (raw, master))
                conn.commit()

                final_data = []
                for _, row in df.iterrows():
                    row_date = str(manual_date)
                    if col_date != "None":
                        try: row_date = pd.to_datetime(row[col_date]).strftime("%Y-%m-%d")
                        except: pass
                    
                    row_key = str(row[col_item]) + (" | " + str(row[col_var]) if col_var != "None" else "")
                    final_data.append({
                        'date': row_date, 'channel': channel,
                        'item_name': mapping_updates.get(row_key, "Unknown"),
                        'qty_sold': clean_num(row[col_qty]), 'revenue': clean_num(row[col_rev])
                    })
                
                upload_df = pd.DataFrame(final_data)
                for d in upload_df['date'].unique():
                    c.execute("DELETE FROM sales WHERE date = ? AND channel = ?", (d, channel))
                upload_df.to_sql('sales', conn, if_exists='append', index=False)
                conn.commit(); st.success("History Updated!"); st.rerun()

# --- TAB 3: CONFIGURATION ---
with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📦 Master SKU List")
        new_sku = st.text_input("Add New SKU")
        if st.button("Add SKU") and new_sku: 
            try: c.execute("INSERT INTO master_skus VALUES (?)", (new_sku.strip(),)); conn.commit(); st.rerun()
            except: st.error("Exists!")
        m_list = [r[0] for r in c.execute("SELECT name FROM master_skus ORDER BY name ASC").fetchall()]
        st.dataframe(pd.DataFrame(m_list, columns=["Product Name"]), height=300, use_container_width=True)
    with c2:
        st.subheader("🏢 Sales Channels")
        new_chan = st.text_input("Add New Channel")
        if st.button("Add Channel") and new_chan:
            try: c.execute("INSERT INTO master_channels VALUES (?)", (new_chan.strip(),)); conn.commit(); st.rerun()
            except: st.error("Exists!")
        c_list = [r[0] for r in c.execute("SELECT name FROM master_channels ORDER BY name ASC").fetchall()]
        st.dataframe(pd.DataFrame(c_list, columns=["Channel Name"]), height=300, use_container_width=True)
