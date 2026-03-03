import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime

# --- 1. CONNECTION ---
try:
    supabase: Client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
except Exception as e:
    st.error(f"Secret Error: {e}"); st.stop()

# --- 2. THE DATE PARSER (CRITICAL FIX) ---
def parse_date_smart(val):
    if pd.isna(val) or val == "None": return None
    # Split "20260219 - 20260219"
    s = str(val).split(" - ")[0].strip()
    # Handle YYYYMMDD
    if len(s) == 8 and s.isdigit():
        try: return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except: pass
    # Handle standard formats
    try: return pd.to_datetime(s).strftime("%Y-%m-%d")
    except: return None

def clean_num(val):
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return float(res) if res else 0.0
    except: return 0.0

# --- 3. DATA FETCHING ---
def get_fresh_data():
    res = supabase.table("sales").select("*").execute()
    return pd.DataFrame(res.data)

# --- 4. TABS ---
t1, t2 = st.tabs(["📊 Analytics", "📤 Smart Upload"])

with t1:
    df = get_fresh_data()
    if df.empty:
        st.info("Database is empty.")
    else:
        df['date'] = pd.to_datetime(df['date'])
        st.subheader("Filters")
        c1, c2 = st.columns(2)
        with c1:
            sel_chan = st.multiselect("Channels", df['channel'].unique(), default=df['channel'].unique())
        with c2:
            # FIXED: Ensure the graph shows the last 30 days by default to capture Feb 20th
            min_date = df['date'].min().date()
            max_date = df['date'].max().date()
            date_range = st.date_input("Date Range", [min_date, max_date])

        # Filter Logic
        mask = (df['channel'].isin(sel_chan))
        if len(date_range) == 2:
            mask = mask & (df['date'].dt.date >= date_range[0]) & (df['date'].dt.date <= date_range[1])
        
        f_df = df[mask]
        
        if not f_df.empty:
            st.metric("Total Revenue", f"₹{f_df['revenue'].sum():,.2f}")
            # Grouping by date for the graph
            chart_data = f_df.groupby(['date', 'channel'])['revenue'].sum().reset_index()
            st.plotly_chart(px.bar(chart_data, x='date', y='revenue', color='channel', barmode='stack'), use_container_width=True)
            st.write("Last 10 Rows in Database:", f_df.tail(10))
        else:
            st.warning("No data found for selected filters.")

with t2:
    st.subheader("Upload Big Basket / Blinkit Report")
    sel_ch = st.selectbox("Channel", ["Blinkit", "Big Basket", "Zepto"])
    up = st.file_uploader("Upload CSV", type=["csv"])
    
    if up:
        raw_df = pd.read_csv(up)
        cols = raw_df.columns.tolist()
        
        # AUTO-DETECTION of your specific file headers
        def_p = "sku_description" if "sku_description" in cols else cols[0]
        def_q = "total_quantity" if "total_quantity" in cols else cols[0]
        def_r = "total_sales" if "total_sales" in cols else cols[0]
        def_d = "date_range" if "date_range" in cols else "None"

        c1, c2 = st.columns(2)
        p_col = c1.selectbox("Product Description Col", cols, index=cols.index(def_p))
        d_col = c1.selectbox("Date Range Col", ["None"] + cols, index=(cols.index(def_d)+1 if def_d != "None" else 0))
        q_col = c2.selectbox("Quantity Col", cols, index=cols.index(def_q))
        r_col = c2.selectbox("Revenue/Sales Col", cols, index=cols.index(def_r))
        
        if st.button("🚀 Push to Supabase"):
            batch = []
            valid_dates = 0
            for _, row in raw_df.iterrows():
                # Extract date
                final_date = parse_date_smart(row[d_col]) if d_col != "None" else datetime.now().strftime("%Y-%m-%d")
                
                if final_date:
                    valid_dates += 1
                    batch.append({
                        "date": final_date,
                        "channel": sel_ch,
                        "item_name": str(row[p_col]),
                        "qty_sold": clean_num(row[q_col]),
                        "revenue": clean_num(row[r_col])
                    })
            
            if batch:
                st.write(f"Parsed {valid_dates} valid rows. Sending to cloud...")
                res = supabase.table("sales").insert(batch).execute()
                st.success(f"Uploaded {len(res.data)} rows successfully!")
                st.rerun()
            else:
                st.error("No valid data found to upload. Check date formats.")
