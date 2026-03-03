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
        "admin": "mamaadmin2026",    # Can View, Upload & Configure
        "viewer": "mamaview2026"     # Can only View Analytics
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

    # Data Loading Helpers
    def load_sheet(name, columns):
        try:
            df = conn.read(spreadsheet=SHEET_URL, worksheet=name, ttl=0) 
            return df if df is not None and not df.empty else pd.DataFrame(columns=columns)
        except: return pd.DataFrame(columns=columns)

    def clean_num(val):
        if pd.isna(val): return 0.0
        res = re.sub(r'[^\d.]', '', str(val))
        return float(res) if res else 0.0

    # Load Data Components
    history_df = load_sheet("sales", ["date", "channel", "item_name", "qty_sold", "revenue"])
    col_map_df = load_sheet("col_map", ["channel", "item_col", "qty_col", "rev_col", "date_col", "var_col"])
    item_map_df = load_sheet("item_map", ["raw_name", "master_name"])
    master_skus = load_sheet("master_skus", ["name"])
    master_chans = load_sheet("master_channels", ["name"])

    st.title("📈 Sales Intelligence Dashboard")

    # --- 3. DYNAMIC TABS BASED ON ROLE ---
    if st.session_state["user_role"] == "admin":
        tabs = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])
    else:
        tabs = st.tabs(["📊 Trend Analytics"])
        st.info("💡 Reviewer Mode: Upload and Config are hidden.")

    # --- TAB 1: ANALYTICS (VIEWERS & ADMINS) ---
    with tabs[0]:
        view_metric = st.radio("Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
        target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
        currency = "₹" if "Revenue" in view_metric else ""

        if not history_df.empty:
            history_df['date_dt'] = pd.to_datetime(history_df['date'])
            today = datetime.now().date()
            
            q1, q2 = st.columns([3, 1])
            with q1:
                time_preset = st.radio("Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
            with q2:
                show_labels = st.checkbox("Show Labels", value=True)

            if time_preset == "Last 7 Days": start_d, end_d = today - timedelta(days=6), today
            elif time_preset == "Last 30 Days": start_d, end_d = today - timedelta(days=29), today
            elif time_preset == "Month to Date": start_d, end_d = today.replace(day=1), today
            elif time_preset == "All Time": start_d, end_d = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
            else:
                dr = st.date_input("Range", value=(today - timedelta(days=7), today))
                start_d, end_d = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

            num_days = (end_d - start_d).days + 1
            mask = (history_df['date_dt'].dt.date >= start_d) & (history_df['date_dt'].dt.date <= end_d)
            filtered = history_df[mask].copy()

            if not filtered.empty:
                c1, c2 = st.columns(2)
                sel_chan = c1.multiselect("Channels", sorted(filtered['channel'].unique()), default=filtered['channel'].unique())
                filtered = filtered[filtered['channel'].isin(sel_chan)]
                sel_item = c2.multiselect("Products", sorted(filtered['item_name'].unique()))
                if sel_item: filtered = filtered[filtered['item_name'].isin(sel_item)]

                total = filtered[target_col].sum()
                drr = total / num_days if num_days > 0 else 0
                
                m1, m2 = st.columns(2)
                m1.metric("Total Sales", f"{currency}{total:,.2f}")
                m2.metric("Daily Run Rate (DRR)", f"{currency}{drr:,.2f}")

                color_by = "item_name" if sel_item else "channel"
                plot_df = filtered.groupby(['date', color_by])[target_col].sum().reset_index()
                fig = px.bar(plot_df, x="date", y=target_col, color=color_by, barmode="stack", height=500)
                fig.add_hline(y=drr, line_dash="dash", line_color="red", annotation_text="Avg DRR")
                st.plotly_chart(fig, use_container_width=True)

                # Snapshot Download
                towrite = io.BytesIO()
                filtered.drop(columns=['date_dt']).to_excel(towrite, index=False, engine='xlsxwriter')
                towrite.seek(0)
                st.download_button("📥 Download Excel Snapshot", towrite, "mamanourish_snapshot.xlsx", "application/vnd.ms-excel")
        else:
            st.info("No sales data found in the cloud.")

    # --- ADMIN ONLY TABS ---
    if st.session_state["user_role"] == "admin":
        # TAB 2: SMART UPLOAD
        with tabs[1]:
            if master_chans.empty: st.warning("Add Channels in Config first.")
            else:
                channel = st.selectbox("Upload to Channel", master_chans['name'].tolist())
                up_file = st.file_uploader("Choose File", type=["csv", "xlsx"])
                if up_file:
                    df = pd.read_csv(up_file) if up_file.name.endswith('.csv') else pd.read_excel(up_file)
                    cols = ["None"] + df.columns.tolist()
                    mem = col_map_df[col_map_df['channel'] == channel]
                    saved = {k: (cols.index(mem[k].iloc[0]) if not mem.empty and mem[k].iloc[0] in cols else 0) 
                             for k in ["item_col", "qty_col", "rev_col", "date_col", "var_col"]}

                    c1, c2, c3 = st.columns(3)
                    ci = c1.selectbox("Product Name Col", cols, index=saved["item_col"])
                    cq = c1.selectbox("Qty Col", cols, index=saved["qty_col"])
                    cr = c2.selectbox("Revenue Col", cols, index=saved["rev_col"])
                    cd = c2.selectbox("Date Col", cols, index=saved["date_col"])
                    cv = c3.selectbox("Variant Col", cols, index=saved["var_col"])
                    md = c3.date_input("Manual Date")

                    if ci != "None" and not master_skus.empty:
                        df_u = df[[ci, cv]].drop_duplicates() if cv != "None" else df[[ci]].drop_duplicates()
                        maps = {}
                        m_list = master_skus['name'].tolist()
                        for _, r in df_u.iterrows():
                            key = f"{r[ci]} | {r[cv]}" if cv != "None" else str(r[ci])
                            known = item_map_df[item_map_df['raw_name'] == key]
                            maps[key] = st.selectbox(f"Map '{key}'", m_list, index=(m_list.index(known['master_name'].iloc[0]) if not known.empty else 0), key=key)

                        if st.button("🚀 Sync to Cloud"):
                            # Update Col Map
                            new_c = pd.DataFrame([[channel, ci, cq, cr, cd, cv]], columns=col_map_df.columns)
                            col_map_df = pd.concat([col_map_df[col_map_df['channel'] != channel], new_c])
                            conn.update(spreadsheet=SHEET_URL, worksheet="col_map", data=col_map_df)
                            # Update Item Map
                            new_m = pd.DataFrame([[k, v] for k, v in maps.items()], columns=["raw_name", "master_name"])
                            item_map_df = pd.concat([item_map_df, new_m]).drop_duplicates(subset=['raw_name'], keep='last')
                            conn.update(spreadsheet=SHEET_URL, worksheet="item_map", data=item_map_df)
                            # Update Sales
                            rows = []
                            for _, r in df.iterrows():
                                dt = pd.to_datetime(r[cd]).strftime("%Y-%m-%d") if cd != "None" else str(md)
                                k = f"{r[ci]} | {r[cv]}" if cv != "None" else str(r[ci])
                                rows.append([dt, channel, maps[k], clean_num(r[cq]), clean_num(r[cr])])
                            new_s = pd.DataFrame(rows, columns=["date", "channel", "item_name", "qty_sold", "revenue"])
                            history_df = pd.concat([history_df[~((history_df['date'].isin(new_s['date'])) & (history_df['channel'] == channel))], new_s])
                            conn.update(spreadsheet=SHEET_URL, worksheet="sales", data=history_df)
                            st.cache_data.clear(); st.success("Synced!"); st.rerun()

        # TAB 3: CONFIGURATION
        with tabs[2]:
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("📦 Master SKUs")
                new_s = st.text_input("New SKU")
                if st.button("Add SKU") and new_s:
                    master_skus = pd.concat([master_skus, pd.DataFrame([[new_s.strip()]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_skus", data=master_skus)
                    st.cache_data.clear(); st.rerun()
                st.dataframe(master_skus, use_container_width=True)
            with c2:
                st.subheader("🏢 Channels")
                new_c = st.text_input("New Channel")
                if st.button("Add Channel") and new_c:
                    master_chans = pd.concat([master_chans, pd.DataFrame([[new_c.strip()]], columns=["name"])]).drop_duplicates()
                    conn.update(spreadsheet=SHEET_URL, worksheet="master_channels", data=master_chans)
                    st.cache_data.clear(); st.rerun()
                st.dataframe(master_chans, use_container_width=True)
