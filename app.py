import streamlit as st
import pandas as pd
from numpy import nan
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta
from google import genai 
from google.genai import types

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
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'): s = '-' + s[1:-1]
    res = re.sub(r'[^-0-9.]', '', s)
    try: return round(float(res), 2) if res else 0.0
    except: return 0.0

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

    # Load Global Data
    history_df = get_data_safe("sales", ["id", "date", "channel", "item_name", "qty_sold", "revenue"])
    master_skus = get_data_safe("master_skus", ["name"])
    master_chans = get_data_safe("master_channels", ["name"])
    item_map_df = get_data_safe("item_map", ["raw_name", "master_name"])

    # --- 3. SIDEBAR ---
    with st.sidebar:
        st.header(f"👤 {role.upper()}")
        
        # New: AI Helper Tools
        st.subheader("🤖 AI Tools")
        if st.button("🧹 Clear AI Cache"):
            if "ai_response" in st.session_state:
                del st.session_state["ai_response"]
            st.success("AI Session Reset!")

        if role == "admin":
            st.divider()
            st.subheader("🛠 Data Correction")
            with st.expander("Delete Specific Entry"):
                del_date = st.date_input("Select Date", value=datetime.now().date())
                del_chan = st.selectbox("Select Channel", ["Select..."] + master_chans['name'].tolist())
                if st.button("🗑️ Delete Selection"):
                    if del_chan != "Select...":
                        supabase.table("sales").delete().eq("date", str(del_date)).eq("channel", del_chan).execute()
                        st.success(f"Deleted {del_chan} data for {del_date}"); st.rerun()
                    else: st.error("Please select a channel")

            st.divider()
            if st.checkbox("Unlock Global Danger Zone"):
                if st.button("💥 Flush Entire History"):
                    supabase.table("sales").delete().neq("id", -1).execute(); st.rerun()
                if st.button("🔄 Reset All Mappings"):
                    supabase.table("item_map").delete().neq("raw_name", "dummy").execute(); st.rerun()
        
        st.divider()
        if st.button("Logout"):
            del st.session_state["authenticated"]; st.rerun()
        
        st.caption("🚀 Version: 1.6.0 (Quota Fixed)")

    # --- 4. TABS ---
    if role == "admin":
        tabs = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration", "🤖 AI Insights"])
    else:
        tabs = st.tabs(["📊 Analytics", "🤖 AI Insights"])
    
    # --- TAB 1: ANALYTICS (Preserved) ---
    with tabs[0]:
        if history_df.empty:
            st.info("No data found. Admin must upload sales data first.")
        else:
            history_df['date_dt'] = pd.to_datetime(history_df['date'])
            v1, v2 = st.columns([2, 1])
            with v1:
                view_metric = st.radio("Display Dashboard By:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True)
                target_col = "revenue" if "Revenue" in view_metric else "qty_sold"
                metric_label = "Revenue" if "Revenue" in view_metric else "Qty"
                currency_prefix = "₹" if "Revenue" in view_metric else ""
            with v2: show_labels = st.checkbox("Show Data Labels", value=True)

            st.subheader("Time Filters")
            today = datetime.now().date()
            time_preset = st.radio("Period:", ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"], horizontal=True, index=3)
            
            if time_preset == "Last 7 Days": start_date, end_date = today - timedelta(days=6), today
            elif time_preset == "Last 30 Days": start_date, end_date = today - timedelta(days=29), today
            elif time_preset == "Month to Date": start_date, end_date = today.replace(day=1), today
            elif time_preset == "All Time": start_date, end_date = history_df['date_dt'].min().date(), history_df['date_dt'].max().date()
            else:
                dr = st.date_input("Range", value=(history_df['date_dt'].min().date(), today))
                start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

            mask = (history_df['date_dt'].dt.date >= start_date) & (history_df['date_dt'].dt.date <= end_date)
            filtered = history_df[mask].copy()

            m1, m2 = st.columns(2)
            m1.metric(f"Total {metric_label}", f"{currency_prefix}{filtered[target_col].sum():,.2f}")
            m2.metric("Filtered Records", len(filtered))

            if not filtered.empty:
                color_theme = "channel"
                plot_df = filtered.groupby(['date', color_theme])[target_col].sum().reset_index().sort_values('date')
                fig = px.bar(plot_df, x="date", y=target_col, color=color_theme, barmode="stack", height=500)
                st.plotly_chart(fig, use_container_width=True)

    # --- ADMIN ONLY TABS (Preserved) ---
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
                p_col = c1.selectbox("Product Col", cols); q_col = c2.selectbox("Qty Col", cols); r_col = c2.selectbox("Rev Col", cols)
                d_col = c3.selectbox("Date Col", cols); fixed_date = c3.date_input("Manual Date")

                if st.button("🚀 Sync to Cloud"):
                    raw_rows = []
                    for _, r in df.iterrows():
                        dt = pd.to_datetime(r[d_col]).strftime("%Y-%m-%d") if d_col != "None" else str(fixed_date)
                        raw_rows.append({"date": dt, "channel": selected_channel, "item_name": r[p_col], "qty_sold": clean_num(r[q_col]), "revenue": clean_num(r[r_col])})
                    if raw_rows:
                        supabase.table("sales").upsert(raw_rows, on_conflict="date,channel,item_name").execute()
                        st.success("Synced!"); st.rerun()

        with tabs[2]:
            st.subheader("⚙️ Configuration")
            sc1, sc2 = st.columns(2)
            with sc1:
                st.markdown("#### SKUs")
                st.dataframe(master_skus, hide_index=True)
            with sc2:
                st.markdown("#### Channels")
                st.dataframe(master_chans, hide_index=True)

    # --- FINAL TAB: AI INSIGHTS (QUOTA OPTIMIZED) ---
    with tabs[-1]:
        st.subheader("🤖 AI Sales Assistant")
        if "GEMINI_API_KEY" not in st.secrets:
            st.info("Please add `GEMINI_API_KEY` to Streamlit Secrets.")
        else:
            client = genai.Client(api_key=st.secrets["GEMINI_API_KEY"])
            
            # Optimized Model Order: Flash models have 10x higher free limits than Pro
            model_names = ['gemini-2.0-flash', 'gemini-1.5-flash', 'gemini-1.5-flash-8b']
            
            user_query = st.chat_input("Ask about your sales trends...")
            
            if user_query:
                # We show the last query to make it feel like a chat
                st.write(f"**You:** {user_query}")
                
                with st.spinner("Analyzing recent trends..."):
                    # We only send the last 40 rows to stay well within token limits (RESOURCE_EXHAUSTED fix)
                    context_data = history_df.tail(40).to_string(index=False)
                    prompt = f"""You are a Sales Analyst. Data below:
                    {context_data}
                    
                    Question: {user_query}
                    Instructions: Be extremely brief. Focus on numbers."""
                    
                    success = False
                    for m_name in model_names:
                        try:
                            response = client.models.generate_content(model=m_name, contents=prompt)
                            st.markdown(f"**AI ({m_name}):**\n\n{response.text}")
                            success = True
                            break
                        except Exception as e:
                            if "429" in str(e): continue
                            else: st.warning(f"Model {m_name} failed: {str(e)[:100]}")
                    
                    if not success:
                        st.error("⏳ All models are busy. This usually resets every 60 seconds on the free tier.")
