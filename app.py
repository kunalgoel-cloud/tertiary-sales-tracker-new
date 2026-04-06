import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from marketing_module import render_marketing_tab
from channel_performance_module import render_channel_performance_tab

def _fmt_err(e: Exception) -> str:
    """Short readable error — strips 502 HTML bodies."""
    msg = str(e)
    if len(msg) > 300 or "<html" in msg.lower() or "<!doctype" in msg.lower():
        code = "502" if "502" in msg else ("503" if "503" in msg else "server error")
        return f"Supabase {code} — temporarily unavailable. Retry in a moment."
    return msg[:300]

# ─────────────────────────────────────────────
# 1. CONFIG & DB CONNECTION
# ─────────────────────────────────────────────
st.set_page_config(page_title="Mamanourish Executive Tracker", layout="wide")

@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

try:
    supabase = get_supabase()
except Exception as e:
    st.error("Missing Supabase Secrets! Add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
    st.stop()

# ─────────────────────────────────────────────
# 2. HELPERS
# ─────────────────────────────────────────────
def clean_num(val) -> float:
    """Convert messy numeric strings (commas, parentheses) to float."""
    if pd.isna(val) or str(val).strip() == "":
        return 0.0
    s = str(val).strip().replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    res = re.sub(r"[^-0-9.]", "", s)
    try:
        return round(float(res), 2) if res else 0.0
    except ValueError:
        return 0.0

def sanitize(text: str) -> str:
    """Strip dangerous characters from user-supplied names."""
    return re.sub(r"[<>\"'%;()&+]", "", str(text)).strip()[:200]

@st.cache_data(ttl=30)
def get_table(table: str, default_cols: tuple) -> pd.DataFrame:
    """Fetch a Supabase table with pagination; return empty DataFrame on failure."""
    try:
        all_rows = []
        page = 0
        PAGE_SIZE = 1000
        while True:
            res = supabase.table(table).select("*").range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1).execute()
            if not res.data:
                break
            all_rows.extend(res.data)
            if len(res.data) < PAGE_SIZE:
                break
            page += 1
        if not all_rows:
            return pd.DataFrame(columns=list(default_cols))
        df = pd.DataFrame(all_rows)
        for col in ["qty_sold", "revenue"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df
    except Exception as e:
        st.warning(f"Could not load '{table}': {_fmt_err(e)}")
        return pd.DataFrame(columns=list(default_cols))

def invalidate_data_cache():
    get_table.clear()

# ─────────────────────────────────────────────
# 3. AUTHENTICATION
# ─────────────────────────────────────────────
def check_auth() -> bool:
    if "authenticated" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        role_choice = st.selectbox("I am a…", ["Select Role", "Admin (Full Access)", "Viewer (View Only)"])
        pw = st.text_input("Enter Password", type="password")

        if st.button("Login"):
            try:
                admin_pw  = st.secrets["ADMIN_PASSWORD"]
                viewer_pw = st.secrets["VIEWER_PASSWORD"]
            except KeyError:
                st.error("ADMIN_PASSWORD / VIEWER_PASSWORD not set in Streamlit Secrets.")
                return False

            if role_choice == "Admin (Full Access)" and pw == admin_pw:
                st.session_state["authenticated"] = True
                st.session_state["role"] = "admin"
                st.rerun()
            elif role_choice == "Viewer (View Only)" and pw == viewer_pw:
                st.session_state["authenticated"] = True
                st.session_state["role"] = "viewer"
                st.rerun()
            else:
                st.error("Incorrect password or role.")
        return False
    return True

# ─────────────────────────────────────────────
# 4. MAIN APP
# ─────────────────────────────────────────────
if not check_auth():
    st.stop()

role: str = st.session_state["role"]

history_df  = get_table("sales",           ("id", "date", "channel", "item_name", "qty_sold", "revenue"))
master_skus  = get_table("master_skus",    ("name",))
master_chans = get_table("master_channels",("name",))
item_map_df  = get_table("item_map",       ("raw_name", "master_name"))

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.header(f"👤 {role.upper()}")

    if role == "admin":
        st.divider()
        st.subheader("🛠 Data Correction")

        with st.expander("Delete Specific Entry"):
            del_date = st.date_input("Select Date to Clear", value=datetime.now().date())
            chan_options = ["Select…"] + master_chans["name"].tolist()
            del_chan = st.selectbox("Select Channel to Clear", chan_options)
            if st.button("🗑️ Delete Selection"):
                if del_chan != "Select…":
                    try:
                        supabase.table("sales").delete()\
                            .eq("date", str(del_date))\
                            .eq("channel", del_chan)\
                            .execute()
                        st.success(f"Deleted {del_chan} data for {del_date}")
                        invalidate_data_cache()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")
                else:
                    st.error("Please select a channel.")

        st.divider()
        if st.checkbox("Unlock Global Danger Zone"):
            if st.button("💥 Flush Entire History"):
                try:
                    supabase.table("sales").delete().neq("id", -1).execute()
                    st.success("All history flushed.")
                    invalidate_data_cache()
                    st.rerun()
                except Exception as e:
                    st.error(f"Flush failed: {e}")
            if st.button("🔄 Reset All Mappings"):
                try:
                    supabase.table("item_map").delete().neq("raw_name", "dummy").execute()
                    st.success("Mappings reset.")
                    invalidate_data_cache()
                    st.rerun()
                except Exception as e:
                    st.error(f"Reset failed: {e}")

    st.divider()
    if st.button("Logout"):
        del st.session_state["authenticated"]
        del st.session_state["role"]
        st.rerun()

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
if role == "admin":
    tabs = st.tabs(["📊 Trend Analytics", "🔬 Deep Dive", "📣 Performance Marketing", "📤 Smart Upload", "🛠 Configuration", "📦 Channel Performance"])
else:
    tabs = st.tabs(["📊 Trend Analytics", "🔬 Deep Dive", "📣 Performance Marketing", "📦 Channel Performance"])

# ══════════════════════════════════════════════
# TAB 1 – TREND ANALYTICS  (unchanged)
# ══════════════════════════════════════════════
with tabs[0]:
    if history_df.empty:
        st.info("No data found. Admin must upload sales data first.")
    else:
        history_df["date_dt"] = pd.to_datetime(history_df["date"], errors="coerce")
        history_df = history_df.dropna(subset=["date_dt"])

        v1, v2 = st.columns([2, 1])
        with v1:
            view_metric = st.radio(
                "Display Dashboard By:",
                ["Revenue (₹)", "Quantity (Units)"],
                horizontal=True,
            )
        with v2:
            show_labels = st.checkbox("Show Data Labels", value=True)

        target_col      = "revenue" if "Revenue" in view_metric else "qty_sold"
        metric_label    = "Revenue"  if "Revenue" in view_metric else "Qty"
        currency_prefix = "₹"        if "Revenue" in view_metric else ""

        st.subheader("Time Filters")
        today = datetime.now().date()

        time_preset = st.radio(
            "Period:",
            ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"],
            horizontal=True,
            index=3,
        )

        # Cap end_date at the latest date with actual data to avoid
        # diluting DRR with today (which has no sales data yet)
        last_data_date = history_df["date_dt"].max().date()
        effective_end  = min(today, last_data_date)

        if time_preset == "Last 7 Days":
            start_date, end_date = effective_end - timedelta(days=6), effective_end
        elif time_preset == "Last 30 Days":
            start_date, end_date = effective_end - timedelta(days=29), effective_end
        elif time_preset == "Month to Date":
            start_date, end_date = effective_end.replace(day=1), effective_end
        elif time_preset == "All Time":
            start_date = history_df["date_dt"].min().date()
            end_date   = last_data_date
        else:
            dr = st.date_input("Range", value=(history_df["date_dt"].min().date(), effective_end))
            start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (effective_end, effective_end)

        mask     = (history_df["date_dt"].dt.date >= start_date) & (history_df["date_dt"].dt.date <= end_date)
        range_df = history_df[mask].copy()

        f1, f2 = st.columns(2)
        avail_chans = sorted(range_df["channel"].unique())
        with f1:
            sel_chan = st.multiselect("Filter Channels", avail_chans, default=avail_chans)

        chan_mask   = range_df["channel"].isin(sel_chan)
        avail_items = sorted(range_df[chan_mask]["item_name"].unique())
        with f2:
            sel_item = st.multiselect("Filter Products", avail_items)

        final_mask = chan_mask
        if sel_item:
            final_mask &= range_df["item_name"].isin(sel_item)
        filtered = range_df[final_mask].copy()

        total_val     = filtered[target_col].sum()
        intended_days = max((end_date - start_date).days + 1, 1)
        avg_drr       = total_val / intended_days

        m1, m2 = st.columns(2)
        m1.metric(f"Total {metric_label}", f"{currency_prefix}{total_val:,.2f}")
        m2.metric(
            "Daily Run Rate (DRR)",
            f"{currency_prefix}{avg_drr:,.2f}",
            help=f"Total ÷ {intended_days} days in selected period.",
        )

        if not filtered.empty:
            color_theme = "item_name" if sel_item else "channel"
            plot_df = (
                filtered
                .groupby(["date", color_theme])[target_col]
                .sum()
                .reset_index()
                .sort_values("date")
            )
            fig = px.bar(
                plot_df, x="date", y=target_col,
                color=color_theme, barmode="stack", height=500,
            )
            fig.add_hline(
                y=avg_drr, line_dash="dash", line_color="red",
                annotation_text="Avg DRR",
            )
            if show_labels:
                fig.update_traces(texttemplate="%{y:.2s}", textposition="inside")
                totals = plot_df.groupby("date")[target_col].sum().reset_index()
                fig.add_scatter(
                    x=totals["date"],
                    y=totals[target_col],
                    text=totals[target_col].apply(lambda x: f"{x:,.0f}"),
                    mode="text",
                    textposition="top center",
                    showlegend=False,
                )
            st.plotly_chart(fig, use_container_width=True)
            display_cols = [c for c in filtered.columns if c not in ("date_dt", "id")]
            st.dataframe(filtered[display_cols], hide_index=True)

# ══════════════════════════════════════════════
# TAB 2 – DEEP DIVE  (new)
# ══════════════════════════════════════════════
with tabs[1]:
    if history_df.empty:
        st.info("No data found. Admin must upload sales data first.")
    else:
        # Ensure date column is parsed (re-do in case tab 1 was skipped)
        if "date_dt" not in history_df.columns:
            history_df["date_dt"] = pd.to_datetime(history_df["date"], errors="coerce")
            history_df = history_df.dropna(subset=["date_dt"])

        st.subheader("🔬 Deep Dive Analytics")

        # ── Shared filters for this tab ───────────────────────────────
        today_dd = datetime.now().date()
        dd_col1, dd_col2 = st.columns([3, 1])
        with dd_col1:
            dd_preset = st.radio(
                "Period:",
                ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"],
                horizontal=True,
                index=3,
                key="dd_period",
            )
        last_data_date_dd = history_df["date_dt"].max().date()
        effective_end_dd  = min(today_dd, last_data_date_dd)

        if dd_preset == "Last 7 Days":
            dd_start, dd_end = effective_end_dd - timedelta(days=6), effective_end_dd
        elif dd_preset == "Last 30 Days":
            dd_start, dd_end = effective_end_dd - timedelta(days=29), effective_end_dd
        elif dd_preset == "Month to Date":
            dd_start, dd_end = effective_end_dd.replace(day=1), effective_end_dd
        elif dd_preset == "All Time":
            dd_start = history_df["date_dt"].min().date()
            dd_end   = last_data_date_dd
        else:
            dd_dr = st.date_input("Custom Range", value=(history_df["date_dt"].min().date(), effective_end_dd), key="dd_range")
            dd_start, dd_end = (dd_dr[0], dd_dr[1]) if len(dd_dr) == 2 else (effective_end_dd, effective_end_dd)

        dd_mask = (history_df["date_dt"].dt.date >= dd_start) & (history_df["date_dt"].dt.date <= dd_end)
        dd_df   = history_df[dd_mask].copy()

        if dd_df.empty:
            st.warning("No data in the selected period.")
            st.stop()

        dd_days = max((dd_end - dd_start).days + 1, 1)
        dd_df["week_label"] = dd_df["date_dt"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
        dd_df["dow"]        = dd_df["date_dt"].dt.day_name()

        st.divider()

        # ════════════════════════════════
        # VIEW 1 — Channel Mix Donut
        # ════════════════════════════════
        st.markdown("### 🍩 Channel Revenue Mix")
        st.caption("What % of total revenue comes from each platform — spot platform concentration risk at a glance.")

        chan_rev = dd_df.groupby("channel")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)
        chan_qty = dd_df.groupby("channel")["qty_sold"].sum().reset_index().sort_values("qty_sold", ascending=False)

        donut_c1, donut_c2 = st.columns(2)

        with donut_c1:
            fig_donut_rev = px.pie(
                chan_rev, values="revenue", names="channel",
                hole=0.55, height=380,
                title="By Revenue (₹)",
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            fig_donut_rev.update_traces(textinfo="label+percent", textposition="outside")
            fig_donut_rev.update_layout(showlegend=False, margin=dict(t=50, b=10, l=10, r=10))
            # Centre annotation
            total_rev = chan_rev["revenue"].sum()
            fig_donut_rev.add_annotation(
                text=f"₹{total_rev/1000:.1f}K", x=0.5, y=0.5,
                font=dict(size=18, color="black"), showarrow=False,
            )
            st.plotly_chart(fig_donut_rev, use_container_width=True)

        with donut_c2:
            fig_donut_qty = px.pie(
                chan_qty, values="qty_sold", names="channel",
                hole=0.55, height=380,
                title="By Units Sold",
                color_discrete_sequence=px.colors.qualitative.Bold,
            )
            fig_donut_qty.update_traces(textinfo="label+percent", textposition="outside")
            fig_donut_qty.update_layout(showlegend=False, margin=dict(t=50, b=10, l=10, r=10))
            total_qty = chan_qty["qty_sold"].sum()
            fig_donut_qty.add_annotation(
                text=f"{total_qty:,.0f} units", x=0.5, y=0.5,
                font=dict(size=14, color="black"), showarrow=False,
            )
            st.plotly_chart(fig_donut_qty, use_container_width=True)

        # Channel mix summary table
        chan_summary = chan_rev.merge(chan_qty, on="channel")
        chan_summary["rev_%"]       = (chan_summary["revenue"] / chan_summary["revenue"].sum() * 100).round(1)
        chan_summary["avg_rev/day"] = (chan_summary["revenue"] / dd_days).round(0)
        chan_summary["avg_price"]   = pd.to_numeric(chan_summary["revenue"] / chan_summary["qty_sold"].where(chan_summary["qty_sold"] > 0), errors="coerce").round(1)
        chan_summary.columns        = ["Channel", "Revenue (₹)", "Units Sold", "Rev Share %", "DRR (₹)", "Avg Price (₹)"]
        st.dataframe(
            chan_summary.style.format({
                "Revenue (₹)": "₹{:,.0f}", "Units Sold": "{:,.0f}",
                "Rev Share %": "{:.1f}%",  "DRR (₹)": "₹{:,.0f}",
                "Avg Price (₹)": "₹{:.1f}",
            }),
            hide_index=True, use_container_width=True,
        )

        st.divider()

        # ════════════════════════════════
        # VIEW 2 — SKU Performance Table
        # ════════════════════════════════
        st.markdown("### 🏆 SKU Performance Ranking")
        st.caption("Which products are driving the business — and which need attention.")

        sku_perf = (
            dd_df.groupby("item_name")
            .agg(revenue=("revenue", "sum"), qty_sold=("qty_sold", "sum"))
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        sku_perf["rev_%"]       = (sku_perf["revenue"] / sku_perf["revenue"].sum() * 100).round(1)
        sku_perf["avg_price"]   = pd.to_numeric(sku_perf["revenue"] / sku_perf["qty_sold"].where(sku_perf["qty_sold"] > 0), errors="coerce").round(1)
        sku_perf["drr"]         = (sku_perf["revenue"] / dd_days).round(0)
        sku_perf["status"]      = sku_perf["revenue"].apply(
            lambda r: "🔴 Dead" if r == 0 else ("🟡 Slow" if r < sku_perf["revenue"].mean() * 0.3 else "🟢 Active")
        )

        # Horizontal bar chart
        fig_sku = px.bar(
            sku_perf.sort_values("revenue"),
            x="revenue", y="item_name",
            orientation="h", height=max(350, len(sku_perf) * 45),
            color="rev_%",
            color_continuous_scale="Blues",
            labels={"revenue": "Revenue (₹)", "item_name": ""},
            text=sku_perf.sort_values("revenue")["revenue"].apply(lambda x: f"₹{x:,.0f}"),
        )
        fig_sku.update_traces(textposition="outside")
        fig_sku.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=80, t=20, b=20))
        st.plotly_chart(fig_sku, use_container_width=True)

        sku_perf.columns = ["SKU", "Revenue (₹)", "Units", "Rev Share %", "Avg Price (₹)", "DRR (₹)", "Status"]
        st.dataframe(
            sku_perf.style.format({
                "Revenue (₹)": "₹{:,.0f}", "Units": "{:,.0f}",
                "Rev Share %": "{:.1f}%",   "Avg Price (₹)": "₹{:.1f}",
                "DRR (₹)":     "₹{:,.0f}",
            }),
            hide_index=True, use_container_width=True,
        )

        st.divider()

        # ════════════════════════════════
        # VIEW 3 — Week-over-Week Trend
        # ════════════════════════════════
        st.markdown("### 📈 Week-over-Week Performance")
        st.caption("Are we growing or declining? WoW change per channel reveals momentum shifts.")

        wow_metric = st.radio("WoW Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True, key="wow_metric")
        wow_col    = "revenue" if "Revenue" in wow_metric else "qty_sold"
        wow_prefix = "₹" if "Revenue" in wow_metric else ""

        weekly = (
            dd_df.groupby(["week_label", "channel"])[wow_col]
            .sum()
            .reset_index()
            .sort_values("week_label")
        )

        fig_wow = px.line(
            weekly, x="week_label", y=wow_col, color="channel",
            markers=True, height=420,
            labels={"week_label": "Week Starting", wow_col: wow_metric},
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig_wow.update_traces(line_width=2.5, marker_size=8)
        fig_wow.update_layout(xaxis_title="Week", hovermode="x unified")
        st.plotly_chart(fig_wow, use_container_width=True)

        # WoW change table — pivot weeks as columns
        wow_pivot = weekly.pivot_table(index="channel", columns="week_label", values=wow_col, aggfunc="sum").fillna(0)
        weeks_sorted = sorted(wow_pivot.columns)
        wow_pivot    = wow_pivot[weeks_sorted]

        if len(weeks_sorted) >= 2:
            last_w  = weeks_sorted[-1]
            prev_w  = weeks_sorted[-2]
            wow_pivot["WoW Change"] = wow_pivot[last_w] - wow_pivot[prev_w]
            wow_pivot["WoW %"]      = (
                (wow_pivot["WoW Change"] / wow_pivot[prev_w].where(wow_pivot[prev_w] != 0)) * 100
            ).round(1)

        st.dataframe(
            wow_pivot.style.format(
                {c: f"{wow_prefix}{{:,.0f}}" for c in weeks_sorted}
                | ({"WoW Change": f"{wow_prefix}{{:+,.0f}}", "WoW %": "{:+.1f}%"} if len(weeks_sorted) >= 2 else {})
            ).map(
                lambda v: "color: green" if isinstance(v, (int, float)) and v > 0
                else ("color: red" if isinstance(v, (int, float)) and v < 0 else ""),
                subset=["WoW %", "WoW Change"] if len(weeks_sorted) >= 2 else [],
            ),
            use_container_width=True,
        )

        st.divider()

        # ════════════════════════════════
        # VIEW 4 — Day-of-Week Heatmap
        # ════════════════════════════════
        st.markdown("### 🗓️ Day-of-Week Revenue Heatmap")
        st.caption("When do customers buy? Use this to time promotions and ensure stock is ready on peak days.")

        dow_metric = st.radio("Heatmap Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True, key="dow_metric")
        dow_col    = "revenue" if "Revenue" in dow_metric else "qty_sold"

        dow_order  = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        avail_chans_dd = sorted(dd_df["channel"].unique())
        dow_chan_sel   = st.multiselect(
            "Channels to include:", avail_chans_dd, default=avail_chans_dd, key="dow_chan"
        )

        dow_filtered = dd_df[dd_df["channel"].isin(dow_chan_sel)]
        dow_pivot    = (
            dow_filtered.groupby(["channel", "dow"])[dow_col]
            .sum()
            .reset_index()
            .pivot(index="channel", columns="dow", values=dow_col)
            .reindex(columns=[d for d in dow_order if d in dd_df["dow"].unique()])
            .fillna(0)
        )

        fig_heat = go.Figure(
            data=go.Heatmap(
                z=dow_pivot.values,
                x=dow_pivot.columns.tolist(),
                y=dow_pivot.index.tolist(),
                colorscale="Blues",
                text=[[f"{'₹' if dow_col=='revenue' else ''}{v:,.0f}" for v in row] for row in dow_pivot.values],
                texttemplate="%{text}",
                showscale=True,
            )
        )
        fig_heat.update_layout(
            height=max(250, len(dow_pivot) * 60 + 100),
            xaxis_title="Day of Week",
            yaxis_title="Channel",
            margin=dict(l=10, r=10, t=20, b=20),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

        # Best/worst day callout
        if not dow_pivot.empty:
            day_totals  = dow_pivot.sum()
            best_day    = day_totals.idxmax()
            worst_day   = day_totals.idxmin()
            hw1, hw2    = st.columns(2)
            hw1.success(f"📈 **Best day:** {best_day} — avg {'₹' if dow_col=='revenue' else ''}{day_totals[best_day]/max(len(dd_df['date_dt'].dt.isocalendar().week.unique()),1):,.0f}/week")
            hw2.warning(f"📉 **Weakest day:** {worst_day} — avg {'₹' if dow_col=='revenue' else ''}{day_totals[worst_day]/max(len(dd_df['date_dt'].dt.isocalendar().week.unique()),1):,.0f}/week")

        st.divider()

        # ════════════════════════════════
        # VIEW 5 — SKU × Channel Matrix
        # ════════════════════════════════
        st.markdown("### 📦 SKU × Channel Revenue Matrix")
        st.caption("Where is each product actually selling? Blank cells = not listed or zero sales on that platform.")

        matrix_metric = st.radio("Matrix Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True, key="matrix_metric")
        mat_col       = "revenue" if "Revenue" in matrix_metric else "qty_sold"

        sku_chan = (
            dd_df.groupby(["item_name", "channel"])[mat_col]
            .sum()
            .reset_index()
            .pivot(index="item_name", columns="channel", values=mat_col)
            .fillna(0)
        )
        sku_chan["TOTAL"] = sku_chan.sum(axis=1)
        sku_chan = sku_chan.sort_values("TOTAL", ascending=False)

        fig_matrix = go.Figure(
            data=go.Heatmap(
                z=sku_chan.drop(columns="TOTAL").values,
                x=sku_chan.drop(columns="TOTAL").columns.tolist(),
                y=sku_chan.index.tolist(),
                colorscale="Greens",
                text=[[f"{'₹' if mat_col=='revenue' else ''}{v:,.0f}" if v > 0 else "—" for v in row]
                      for row in sku_chan.drop(columns="TOTAL").values],
                texttemplate="%{text}",
                showscale=True,
            )
        )
        fig_matrix.update_layout(
            height=max(300, len(sku_chan) * 50 + 100),
            xaxis_title="Channel",
            yaxis_title="",
            margin=dict(l=10, r=10, t=20, b=20),
        )
        st.plotly_chart(fig_matrix, use_container_width=True)

        # Distribution gap alert
        zero_combos = []
        for sku in sku_chan.index:
            for chan in sku_chan.drop(columns="TOTAL").columns:
                if sku_chan.loc[sku, chan] == 0:
                    zero_combos.append(f"**{sku}** on {chan}")
        if zero_combos:
            with st.expander(f"⚠️ {len(zero_combos)} distribution gaps (zero sales combos)"):
                for z in zero_combos:
                    st.write(f"• {z}")

        st.divider()

        # ════════════════════════════════
        # VIEW 6 — Zero-Sales Alert Panel
        # ════════════════════════════════
        st.markdown("### ⚠️ Ops Health Check")
        st.caption("SKUs with no sales in the last 7 days, and channels with no recent data uploads.")

        alert_cutoff   = today_dd - timedelta(days=7)
        recent_df      = history_df[history_df["date_dt"].dt.date >= alert_cutoff]

        alert_col1, alert_col2 = st.columns(2)

        with alert_col1:
            st.markdown("**🔴 SKUs with zero sales (last 7 days)**")
            all_skus       = set(history_df["item_name"].unique())
            active_skus    = set(recent_df[recent_df["revenue"] > 0]["item_name"].unique())
            dead_skus      = sorted(all_skus - active_skus)
            if dead_skus:
                for sku in dead_skus:
                    st.error(f"• {sku}")
            else:
                st.success("All SKUs had sales in the last 7 days ✅")

        with alert_col2:
            st.markdown("**🟡 Channels with no data in last 3 days**")
            stale_cutoff   = today_dd - timedelta(days=3)
            all_channels   = set(history_df["channel"].unique())
            fresh_channels = set(history_df[history_df["date_dt"].dt.date >= stale_cutoff]["channel"].unique())
            stale_channels = sorted(all_channels - fresh_channels)
            if stale_channels:
                for ch in stale_channels:
                    last_seen = history_df[history_df["channel"] == ch]["date_dt"].max().date()
                    st.warning(f"• {ch} — last data: {last_seen}")
            else:
                st.success("All channels have recent data ✅")

# ══════════════════════════════════════════════
# TAB 3 – PERFORMANCE MARKETING (all roles, index 2)
# ══════════════════════════════════════════════
with tabs[2]:
    render_marketing_tab(role)

# ══════════════════════════════════════════════
# TAB 4 – SMART UPLOAD  (admin only)
# ══════════════════════════════════════════════
if role == "admin":
    with tabs[3]:
        st.subheader("Upload Sales Report")

        channels = master_chans["name"].tolist() if not master_chans.empty else []
        if not channels:
            st.warning("No channels configured. Add channels in the Configuration tab first.")
            st.stop()

        selected_channel = st.selectbox("Select Channel", channels)
        up_file = st.file_uploader("Upload File", type=["csv", "xlsx"])

        if up_file and selected_channel:
            try:
                raw_df = (
                    pd.read_csv(up_file)
                    if up_file.name.lower().endswith(".csv")
                    else pd.read_excel(up_file)
                )
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.stop()

            st.write(f"**Preview** — {len(raw_df)} rows × {len(raw_df.columns)} cols")
            st.dataframe(raw_df.head(5), hide_index=True)

            cols = ["None"] + raw_df.columns.tolist()

            # Channels that carry city-level sales data
            CITY_CHANNELS = {"Blinkit", "Swiggy", "Big Basket"}
            needs_city = selected_channel in CITY_CHANNELS

            c1, c2, c3 = st.columns(3)

            with c1:
                p_col = st.selectbox("Product Column *", cols, key="p_col")
                v_col = st.selectbox("Variant Column (optional)", cols, key="v_col")
            with c2:
                q_col = st.selectbox("Qty Column *",     cols, key="q_col")
                r_col = st.selectbox("Revenue Column *", cols, key="r_col")
            with c3:
                d_col      = st.selectbox("Date Column (or use manual date below)", cols, key="d_col")
                fixed_date = st.date_input("Manual Date (used if no date column)", key="fixed_date")
                if needs_city:
                    city_col = st.selectbox(
                        "City Column *", cols, key="city_col",
                        help="Required for city-level inventory tracking "
                             "(Blinkit=Supply City, Swiggy=CITY, BigBasket=DC)",
                    )
                else:
                    city_col = "None"  # Amazon and others — city not applicable

            # Validate mandatory column picks
            mandatory = [("Product", p_col), ("Qty", q_col), ("Revenue", r_col)]
            if needs_city:
                mandatory.append(("City", city_col))
            missing = [name for name, col in mandatory if col == "None"]
            if missing:
                st.info(f"Please select columns for: {', '.join(missing)}")
                st.stop()

            # ── Build composite key safely ──────────────────────────────────
            work_df = raw_df.copy()
            work_df["__prod__"] = work_df[p_col].astype(str).str.strip()

            if v_col != "None":
                work_df["__var__"] = work_df[v_col].astype(str).str.strip()
                work_df["m_key"]   = work_df["__prod__"] + " | " + work_df["__var__"]
            else:
                work_df["m_key"] = work_df["__prod__"]

            # ── Filter out totals rows ──────────────────────────────────────
            SKIP_LABELS = {"total", "grand total", "subtotal", "nan", ""}
            valid_mask  = ~work_df["__prod__"].str.lower().isin(SKIP_LABELS)
            work_df     = work_df[valid_mask].copy()

            if work_df.empty:
                st.error("No valid data rows found after filtering. Check column mapping.")
                st.stop()

            # ── SKU Mapping UI ──────────────────────────────────────────────
            masters = master_skus["name"].tolist() if not master_skus.empty else []
            if not masters:
                st.warning("No master SKUs configured. Add SKUs in the Configuration tab first.")
                st.stop()

            st.markdown("#### 🗺 Map Raw Product Names → Master SKUs")
            unique_keys = sorted(work_df["m_key"].unique())
            sku_map: dict[str, str] = {}

            saved_map: dict[str, str] = {}
            if not item_map_df.empty:
                saved_map = dict(zip(item_map_df["raw_name"], item_map_df["master_name"]))

            for k in unique_keys:
                saved       = saved_map.get(k, "")
                default_idx = masters.index(saved) if saved in masters else 0
                sku_map[k]  = st.selectbox(
                    f"Map: `{k}`",
                    masters,
                    index=default_idx,
                    key=f"sku_{k}",
                )

            # ── Date Preview ────────────────────────────────────────────────
            if d_col != "None":
                def preview_date(val):
                    s = str(val).strip()
                    if " - " in s:
                        s = s.split(" - ")[0].strip()
                    try:
                        return pd.to_datetime(s).strftime("%Y-%m-%d")
                    except Exception:
                        return f"⚠️ unparseable: {val}"

                sample_dates   = work_df[d_col].dropna().unique()[:5]
                parsed_preview = [preview_date(d) for d in sample_dates]
                st.info(f"📅 **Date column preview** — raw: `{sample_dates[0]}` → parsed as: `{parsed_preview[0]}`")
                if any("unparseable" in str(p) for p in parsed_preview):
                    st.warning("Some dates couldn't be parsed — those rows will use the Manual Date instead.")

            # ── Sync Button ─────────────────────────────────────────────────
            if st.button("🚀 Sync to Cloud"):
                errors: list[str] = []

                with st.spinner("Saving mappings…"):
                    for raw_name, master_name in sku_map.items():
                        try:
                            supabase.table("item_map").upsert(
                                {"raw_name": raw_name, "master_name": master_name},
                                on_conflict="raw_name",
                            ).execute()
                        except Exception as e:
                            errors.append(f"Mapping save failed for '{raw_name}': {e}")

                with st.spinner("Processing rows…"):
                    raw_rows = []
                    for _, r in work_df.iterrows():
                        if d_col != "None":
                            raw_date_val = str(r[d_col]).strip()
                            if " - " in raw_date_val:
                                raw_date_val = raw_date_val.split(" - ")[0].strip()
                            try:
                                dt_str = pd.to_datetime(raw_date_val).strftime("%Y-%m-%d")
                            except Exception:
                                dt_str = str(fixed_date)
                        else:
                            dt_str = str(fixed_date)

                        row_city = (
                            str(r[city_col]).strip()
                            if city_col != "None" and city_col in r.index
                            else None
                        )
                        raw_rows.append({
                            "date":      dt_str,
                            "channel":   selected_channel,
                            "item_name": sku_map[r["m_key"]],
                            "qty_sold":  clean_num(r[q_col]),
                            "revenue":   clean_num(r[r_col]),
                            "city":      row_city,
                        })

                    if not raw_rows:
                        st.error("No rows to upload after processing.")
                        st.stop()

                    group_cols = ["date", "channel", "item_name", "city"]
                    final_df = (
                        pd.DataFrame(raw_rows)
                        .groupby(group_cols, dropna=False)
                        .agg({"qty_sold": "sum", "revenue": "sum"})
                        .reset_index()
                    )

                with st.spinner(f"Uploading {len(final_df)} records to Supabase…"):
                    try:
                        # Sanitise before JSON serialisation:
                        # - city: NaN → None (null in JSON, allowed by Supabase)
                        # - qty_sold / revenue: NaN → 0.0
                        final_df["qty_sold"] = final_df["qty_sold"].fillna(0.0)
                        final_df["revenue"]  = final_df["revenue"].fillna(0.0)
                        # Replace NaN city with None so JSON encodes as null
                        final_df["city"] = final_df["city"].where(
                            final_df["city"].notna() & (final_df["city"].astype(str) != "nan"),
                            other=None,
                        )
                        CHUNK   = 500
                        records = final_df.to_dict(orient="records")
                        # Ensure city None stays None (not "None" string)
                        for rec in records:
                            if rec.get("city") in (float("nan"), "nan", "None"):
                                rec["city"] = None
                        for i in range(0, len(records), CHUNK):
                            chunk = records[i : i + CHUNK]
                            res   = supabase.table("sales").upsert(
                                chunk,
                                on_conflict="date,channel,item_name,city",
                            ).execute()
                            if hasattr(res, "error") and res.error:
                                errors.append(f"Upsert chunk {i//CHUNK+1} error: {res.error}")
                    except Exception as e:
                        errors.append(f"Upload failed: {e}")

                if errors:
                    for err in errors:
                        st.error(err)
                    st.warning(
                        "⚠️ Some records may not have synced. "
                        "Check that your `sales` table has a UNIQUE constraint on "
                        "(date, channel, item_name, city) in Supabase."
                    )
                else:
                    st.success(f"✅ Synced {len(final_df)} unique records for '{selected_channel}'!")
                    invalidate_data_cache()
                    st.rerun()

    # ══════════════════════════════════════════
    # TAB 4 – CONFIGURATION  (admin only)
    # ══════════════════════════════════════════
    with tabs[4]:
        st.subheader("⚙️ System Configuration")
        sc1, sc2 = st.columns(2)

        with sc1:
            st.markdown("#### 📦 Master SKUs")
            n_sku = st.text_input("New SKU Name")
            if st.button("Add SKU") and n_sku.strip():
                safe_sku = sanitize(n_sku)
                if safe_sku:
                    try:
                        supabase.table("master_skus").insert({"name": safe_sku}).execute()
                        st.success(f"Added SKU: {safe_sku}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add SKU: {e}")
            if not master_skus.empty:
                st.dataframe(master_skus, hide_index=True)

        with sc2:
            st.markdown("#### 🏢 Sales Channels")
            n_ch = st.text_input("New Channel Name")
            if st.button("Add Channel") and n_ch.strip():
                safe_ch = sanitize(n_ch)
                if safe_ch:
                    try:
                        supabase.table("master_channels").insert({"name": safe_ch}).execute()
                        st.success(f"Added channel: {safe_ch}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add channel: {e}")
            if not master_chans.empty:
                st.dataframe(master_chans, hide_index=True)

        st.divider()
        st.markdown("#### 🗺 Current Item Mappings")
        if not item_map_df.empty:
            st.dataframe(item_map_df, hide_index=True)
        else:
            st.info("No mappings saved yet.")

# ══════════════════════════════════════════════
# TAB – CHANNEL PERFORMANCE (admin + viewer, last tab)
# ══════════════════════════════════════════════
# Admin: tabs[5]  |  Viewer: tabs[3]
_cp_tab_index = 5 if role == "admin" else 3
with tabs[_cp_tab_index]:
    render_channel_performance_tab(supabase, master_skus, role)
