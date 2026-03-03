import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# --- 1. CONFIG & PERMANENT DB CONNECTION ---
st.set_page_config(page_title="Mamanourish Executive Tracker", layout="wide")

try:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(url, key)
except Exception:
    st.error("Missing Supabase Secrets! Please add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
    st.stop()

def clean_num(val):
    if pd.isna(val): return 0.0
    res = re.sub(r'[^\d.]', '', str(val))
    return float(res) if res else 0.0

# --- 2. AUTHENTICATION SYSTEM ---
def check_auth():
    if "authenticated" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        role_choice = st.selectbox("I am a...", ["Select Role", "Admin (Full Access)", "Viewer (View Only)"])
        pw = st.text_input("Enter Password", type="password")
        if st.button("Login"):
            if role_choice == "Admin (Full Access)" and pw == "mamaadmin2026":
                st.session_state["authenticated"] = True
                st.session_state["role"] = "admin"
                st.rerun()
            elif role_choice == "Viewer (View Only)" and pw == "mamaview2026":
                st.session_state["authenticated"] = True
                st.session_state["role"] = "viewer"
                st.rerun()
            else: st.error("Incorrect password.")
        return False
    return True

if check_auth():
    role = st.session_state["role"]
    
    def get_data_safe(table, default_cols):
        try:
            res = supabase.table(table).select("*").execute()
            df = pd.DataFrame(res.data)
            return df if not df.empty else pd.DataFrame(columns=default_cols)
        except: return pd.DataFrame(columns=default_cols)

    # Load Data
    history_df = get_data_safe("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])
    master_skus = get_data_safe("master_skus", ["name"])
    master_chans = get_data_safe("master_channels", ["name"])
    item_map_df = get_data_safe("item_map", ["raw_name", "master_name"])

    # --- 3. SIDEBAR ---
    with st.sidebar:
        st.header(f"👤 {role.upper()}")
        if role == "admin":
            st.divider()
            if st.checkbox("Unlock Danger Zone"):
                if st.button("🗑️ Flush Sales History"):
                    supabase.table("sales").delete().neq("id", -1).execute()
                    st.rerun()
                if st.button("🔄 Reset Mappings"):
                    supabase.table("item_map").delete().neq("raw_name", "dummy").execute()
                    st.rerun()
        st.divider()
        if st.button("Logout"):
            del st.session_state["authenticated"]
            st.rerun()

    # --- 4. TABS ---
    tabs = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"]) if role == "admin" else st.tabs(["📊 Analytics"])
    
    # --- TAB 1: ANALYTICS ---
    with tabs[0]:
        if history_df.empty:
            st.info("No data found. Admin must upload sales data first.")
        else:
            history_df['date_dt'] = pd.to_datetime(history_df['date'])
            
            # View Controls
            v1, v2 = st.columns([2, 1])
            with v1:
                view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
                target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
                metric_label = "Revenue" if "Revenue" in view_metric else "Qty"
                currency_prefix = "₹" if "Revenue" in view_metric else ""
            with v2:
                show_labels = st.checkbox("Show Data Labels", value=True)

            # Time Filters
            st.subheader("Time Filters")
            today = datetime.now().date()
            t_col1, t_col2 = st.columns([3, 1])
            with t_col1:
                time_preset = st.radio("Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
            
            if time_preset == "Last 7 Days": start_date, end_date = today - timedelta(days=6), today
            elif time_preset == "Last 30 Days": start_date, end_date = today - timedelta(days=29), today
            elif time_preset == "Month to Date": start_date, end_date = today.replace(day=1), today
            elif time_preset == "All Time": start_date, end_date = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
            else:
                dr = st.date_input("Range", value=(history_df['date_dt'].min().date(), today))
                start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

            num_days = (end_date - start_date).days + 1
            mask = (history_df['date_dt'].dt.date >= start_date) & (history_df['date_dt'].dt.date <= end_date)
            range_df = history_df[mask].copy()

            # Channel & Item Filters
            f1, f2 = st.columns(2)
            avail_chans = sorted(range_df['channel'].unique())
            with f1: sel_chan = st.multiselect("Filter Channels", avail_chans, default=avail_chans)
            
            chan_mask = range_df['channel'].isin(sel_chan)
            avail_items = sorted(range_df[chan_mask]['item_name'].unique())
            with f2: sel_item = st.multiselect("Filter Products", avail_items)

            final_mask = chan_mask
            if sel_item: final_mask &= range_df['item_name'].isin(sel_item)
            filtered = range_df[final_mask].copy()

            # Metrics
            total_val = filtered[target_col].sum()
            avg_drr = total_val / num_days if num_days > 0 else 0

            m1, m2 = st.columns(2)
            m1.metric(f"Total {metric_label}", f"{currency_prefix}{total_val:,.2f}")
            m2.metric("Daily Run Rate (DRR)", f"{currency_prefix}{avg_drr:,.2f}", help=f"Total over {num_days} days")

            # Plot
            if not filtered.empty:
                color_theme = "item_name" if sel_item else "channel"
                plot_df = filtered.groupby(['date', color_theme])[target_col].sum().reset_index().sort_values('date')
                
                fig = px.bar(plot_df, x="date", y=target_col, color=color_theme, barmode="stack", height=500)
                fig.add_hline(y=avg_drr, line_dash="dash", line_color="red", annotation_text=f"Avg DRR: {avg_drr:,.0f}")
                
                if show_labels:
                    fig.update_traces(texttemplate='%{y:.2s}', textposition='inside')
                    totals = plot_df.groupby('date')[target_col].sum().reset_index()
                    fig.add_scatter(x=totals['date'], y=totals[target_col], text=totals[target_col].apply(lambda x: f'{x:,.0f}'), mode='text', textposition='top center', showlegend=False)
                
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(filtered.drop(columns=['date_dt', 'id']), hide_index=True)

    # --- TAB 2 & 3: UPLOAD & CONFIG ---
    if role == "admin":
        with tabs[1]:
            st.subheader("Upload Sales Report")
            channels = master_chans['name'].tolist() if not master_chans.empty else []
            selected_channel = st.selectbox("Select Channel", channels)
            up_file = st.file_uploader("Upload File", type=["csv", "xlsx"])

            if up_file and selected_channel:
                df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                cols = ["None"] + df.columns.tolist()
                c1, c2, c3 = st.columns(3)
                p_col = c1.selectbox("Product Col", cols); v_col = c1.selectbox("Var Col (Opt)", cols)
                q_col = c2.selectbox("Qty Col", cols); r_col = c2.selectbox("Rev Col", cols)
                d_col = c3.selectbox("Date Col", cols); fixed_date = c3.date_input("Manual Date")

                if p_col != "None":
                    df['m_key'] = df[p_col].astype(str) + ((" | " + df[v_col].astype(str)) if v_col != "None" else "")
                    u_keys = df['m_key'].unique()
                    sku_map = {}
                    masters = master_skus['name'].tolist() if not master_skus.empty else []
                    for k in u_keys:
                        ex = item_map_df[item_map_df['raw_name'] == k]
                        idx = masters.index(ex['master_name'].iloc[0]) if not ex.empty and ex['master_name'].iloc[0] in masters else 0
                        sku_map[k] = st.selectbox(f"Map: {k}", masters, index=idx)

                    if st.button("🚀 Sync to Cloud"):
                        for k, v in sku_map.items():
                            supabase.table("item_map").upsert({"raw_name": k, "master_name": v}).execute()
                        f_rows = []
                        for _, r in df.iterrows():
                            dt = pd.to_datetime(r[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                            f_rows.append({"date": dt, "channel": selected_channel, "item_name": sku_map[r['m_key']], "qty_sold": clean_num(r[q_col]), "revenue": clean_num(r[r_col])})
                        supabase.table("sales").insert(f_rows).execute()
                        st.success("Uploaded!"); st.rerun()

        with tabs[2]:
            st.subheader("⚙️ System Configuration")
            sc1, sc2 = st.columns(2)
            with sc1:
                st.markdown("#### 📦 Master SKUs")
                n_sku = st.text_input("New SKU")
                if st.button("Add SKU") and n_sku:
                    supabase.table("master_skus").insert({"name": n_sku.strip()}).execute()
                    st.rerun()
                st.dataframe(master_skus, hide_index=True)
            with sc2:
                st.markdown("#### 🏢 Sales Channels")
                n_ch = st.text_input("New Channel")
                if st.button("Add Channel") and n_ch:
                    supabase.table("master_channels").insert({"name": n_ch.strip()}).execute()
                    st.rerun()
                st.dataframe(master_chans, hide_index=True)
