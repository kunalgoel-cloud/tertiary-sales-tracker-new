import streamlit as st
import pandas as pd
import calendar
from supabase import create_client, Client
import re
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from marketing_module import render_marketing_tab
from channel_performance_module import render_channel_performance_tab
from vending_module import render_vending_tab
from sop_module import render_sop_tab

# ── Global Filter System ──────────────────────────────────────────────────────
# Centralized filter state management. All tabs share these filter values
# via st.session_state. See global_filters.py for full architecture docs.
from global_filters import (
    init_global_filters,
    render_global_filter_bar,
    apply_global_filters,
    get_global_filters,
    get_date_range,
)

# ── UI Design System ──────────────────────────────────────────────────────────
# All visual styling, Plotly theming, and reusable components live here.
# Call inject_css() once after set_page_config() to activate the design system.
from ui_theme import (
    inject_css,
    apply_chart_theme,
    brand_color_sequence,
    section_header,
    badge,
    kpi_row,
    page_header,
    empty_state,
    active_filter_pill,
)

# ── Performance Optimisation Layer ────────────────────────────────────────────
# Smart filter caching, upload progress bars, skeleton loaders, lazy sections.
# See performance.py for full architecture docs.
from performance import (
    inject_perf_css,
    should_recompute,
    get_filtered_df,
    upload_progress_bar,
    skeleton_metrics,
    skeleton_chart,
    lazy_section,
    is_lazy_skip,
    cached_agg,
)

# ── User Management & Access Control ─────────────────────────────────────────
# Handles custom users, hashed passwords, and per-tab permissions.
# See user_management.py for full architecture docs and DB schema.
from user_management import (
    render_user_management_tab,
    render_user_login_option,
    load_user_session,
    is_tab_allowed,
    has_tab_access,
    tab_denied_message,
    ALL_TABS,
    ADMIN_ONLY_TABS,
)

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
st.set_page_config(
    page_title="Mamanourish Executive Tracker",
    layout="wide",
    initial_sidebar_state="expanded",
)
# ── Activate the Mamanourish Design System ────────────────────────────────────
# Injects fonts, CSS custom properties, and all component overrides.
inject_css()
# ── Activate Performance Layer ────────────────────────────────────────────────
# Top loading bar, skeleton shimmers, upload progress CSS, fade-in animations.
inject_perf_css()

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

@st.cache_data(ttl=120, show_spinner=False)
def get_table(table: str, default_cols: tuple) -> pd.DataFrame:
    """Fetch a Supabase table with pagination (cached 120 s)."""
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
        # ── Styled login page ─────────────────────────────────────────────────
        _, center_col, _ = st.columns([1, 1.2, 1])
        with center_col:
            st.markdown(
                """
                <div style="text-align:center; margin-top:2.5rem; margin-bottom:2rem;">
                  <div style="font-family:'DM Serif Display',Georgia,serif;
                              font-size:2rem; color:#C47A2B; margin-bottom:0.3rem;">
                    Mamanourish
                  </div>
                  <div style="font-size:0.75rem; color:#A89E95;
                              letter-spacing:0.12em; text-transform:uppercase;">
                    Executive Sales Portal
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Role selector now includes "Custom User"
            role_choice = st.selectbox(
                "Role",
                ["Select Role", "Admin (Full Access)", "Viewer (View Only)", "Custom User"],
                label_visibility="collapsed",
                placeholder="Select your role…",
            )

            # Show username field only for Custom User
            username_input = ""
            if role_choice == "Custom User":
                username_input = st.text_input(
                    "Username", placeholder="Enter your username…",
                    label_visibility="collapsed",
                )

            pw = st.text_input(
                "Password", type="password",
                placeholder="Enter password…",
                label_visibility="collapsed",
            )

            if st.button("Sign In →", use_container_width=True):
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
                elif role_choice == "Custom User":
                    # Authenticate against app_users table via user_management module
                    if load_user_session(supabase, username_input.strip(), pw):
                        st.rerun()
                    else:
                        st.error("Invalid username or password.")
                else:
                    st.error("Incorrect password or role — please try again.")

            st.markdown(
                '<div style="text-align:center;margin-top:1.5rem;font-size:0.7rem;color:#A89E95;">Mamanourish © 2025</div>',
                unsafe_allow_html=True,
            )
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
master_chans = get_table("master_channels",("name", "is_monthly", "requires_city"))
item_map_df  = get_table("item_map",       ("raw_name", "master_name"))

# Pre-parse dates once at top level so both Analytics and Deep Dive tabs
# share the same parsed DataFrame without repeating the expensive conversion.
if not history_df.empty:
    if "date_dt" not in history_df.columns:
        history_df = history_df.copy()
        history_df["date_dt"] = pd.to_datetime(history_df["date"], errors="coerce")
        history_df = history_df.dropna(subset=["date_dt"])

# ── Initialize global filter state (idempotent — safe to call on every rerun) ─
# Seeds st.session_state["gf_*"] with default values on first load.
# On subsequent reruns, existing user selections are preserved intact.
if not history_df.empty:
    init_global_filters(history_df)


# ── Channel attribute helpers ─────────────────────────────────────────────
def _chan_flag(channel_name: str, flag: str, default: bool) -> bool:
    """Return a boolean flag for a channel from master_chans; default if missing."""
    if master_chans.empty or flag not in master_chans.columns:
        return default
    row = master_chans[master_chans["name"] == channel_name]
    if row.empty:
        return default
    val = row.iloc[0][flag]
    if pd.isna(val):
        return default
    return bool(val)

def is_monthly_channel(ch: str) -> bool:
    return _chan_flag(ch, "is_monthly", False)

def requires_city_channel(ch: str) -> bool:
    return _chan_flag(ch, "requires_city", True)

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    # ── Branded sidebar header ────────────────────────────────────────────────
    st.markdown(
        f"""
        <div style="margin-bottom:1rem;">
          <div style="font-family:'DM Serif Display',Georgia,serif;
                      font-size:1.15rem; color:#F5A623; margin-bottom:0.15rem;">
            Mamanourish
          </div>
          <div class="role-badge">
            {"🔑 Admin" if role == "admin" else "👁 Viewer"}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if role == "admin":
        st.divider()
        st.markdown("## 🛠 Data Correction")

        with st.expander("Delete Specific Entry"):
            del_mode = st.radio(
                "Delete by:",
                ["Single Date", "Entire Month (for monthly channels)"],
                key="del_mode",
                horizontal=True,
            )
            chan_options = ["Select…"] + master_chans["name"].tolist()
            del_chan = st.selectbox("Select Channel to Clear", chan_options)

            if del_mode == "Single Date":
                del_date = st.date_input("Select Date to Clear", value=datetime.now().date())
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
            else:
                del_year  = st.number_input("Year",  min_value=2020, max_value=2100,
                                             value=datetime.now().year,  step=1, key="del_year")
                del_month = st.number_input("Month", min_value=1,    max_value=12,
                                             value=datetime.now().month, step=1, key="del_month")
                if st.button("🗑️ Delete Entire Month"):
                    if del_chan != "Select…":
                        try:
                            days_m = calendar.monthrange(int(del_year), int(del_month))[1]
                            start_str = f"{int(del_year):04d}-{int(del_month):02d}-01"
                            end_str   = f"{int(del_year):04d}-{int(del_month):02d}-{days_m:02d}"
                            supabase.table("sales").delete()\
                                .eq("channel", del_chan)\
                                .gte("date", start_str)\
                                .lte("date", end_str)\
                                .execute()
                            st.success(
                                f"Deleted {del_chan} data for "
                                f"{calendar.month_name[int(del_month)]} {int(del_year)}"
                            )
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

    # ── Global Filters — placed in sidebar for persistent visibility ──────────
    # In Streamlit, the sidebar is the ONLY area that stays visible while the
    # user scrolls through tab content. The main area has no reliable sticky
    # mechanism. So we render the global filter bar here in the sidebar.
    if not history_df.empty:
        render_global_filter_bar(history_df)

    st.divider()
    if st.button("Logout"):
        del st.session_state["authenticated"]
        del st.session_state["role"]
        st.rerun()

# ─────────────────────────────────────────────
# TABS — built dynamically based on role + user permissions
# ─────────────────────────────────────────────
# ALL_TABS defines every possible tab as (key, label) — see user_management.py.
# Each user's allowed_tabs list (from app_users table) gates visibility.
# Admin sees all tabs. Viewer sees non-admin tabs. Custom users see only
# what was granted. The User Management tab is admin-only always.

# Build the ordered list of (label, key) this user can see.
# ALL_TABS tuples are (key, label) — unpack accordingly.
_visible_tabs: list[tuple[str, str]] = []
for _key, _label in ALL_TABS:
    if has_tab_access(_key):
        _visible_tabs.append((_label, _key))

# Admin also sees User Management (always last)
if role == "admin":
    _visible_tabs.append(("👥 User Management", "user_management"))

# Build st.tabs from the visible list
tabs = st.tabs([label for label, _ in _visible_tabs])

# Build a lookup: tab_key → index in tabs list
_tab_index: dict[str, int] = {key: i for i, (_, key) in enumerate(_visible_tabs)}

# Helper: get tab index safely (-1 if not visible)
def _tidx(key: str) -> int:
    return _tab_index.get(key, -1)

# Convenience aliases for the original fixed indices — now dynamic
_TAB_ANALYTICS      = _tidx("trend_analytics")
_TAB_DEEPDIVE       = _tidx("deep_dive")
_TAB_MARKETING      = _tidx("performance_marketing")
_TAB_UPLOAD         = _tidx("smart_upload")
_TAB_MONTHLY_UPLOAD = _tidx("monthly_upload")
_TAB_CONFIG         = _tidx("configuration")
_TAB_CHANPERF       = _tidx("channel_performance")
_TAB_VENDING        = _tidx("vending")
_TAB_SOP            = _tidx("sop")
_TAB_USERMGMT       = _tidx("user_management")

# ══════════════════════════════════════════════
# TAB 1 – TREND ANALYTICS  (unchanged)
# ══════════════════════════════════════════════
if _TAB_ANALYTICS >= 0:
  with tabs[_TAB_ANALYTICS]:
    if history_df.empty:
        empty_state("📊", "No data yet", "Admin must upload sales data via the Smart Upload tab first.")
    else:
        # date_dt already parsed at startup — no re-parse needed

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

        # ── GLOBAL FILTER: Date + Channel + Product ───────────────────────────
        # Replaces the local Time Filters section. Values come from the
        # Global Filter Bar (rendered above the tabs). Users adjust the
        # period/channel/product once and all tabs update together.
        _gf         = get_global_filters()
        start_date  = _gf["start"]
        end_date    = _gf["end"]
        _gf_chans   = _gf["channels"]   # None = all channels selected
        _gf_prods   = _gf["products"]   # None = no product filter

        # Styled active-filter pill row
        active_filter_pill(start_date, end_date, _gf_chans, _gf_prods)

        # ── SMART FILTER CACHE ────────────────────────────────────────────────
        # get_filtered_df() checks a hash of (start, end, channels, products).
        # If the hash matches the previous render, it returns the cached slice
        # without re-scanning history_df — making filter changes instant.
        filtered = get_filtered_df(
            "analytics", history_df,
            start_date, end_date, _gf_chans, _gf_prods,
        )

        # range_df is the channel-filtered (pre-product) slice — used for
        # sel_chan/sel_item backward compat with chart color-theme logic.
        range_df = get_filtered_df(
            "analytics_range", history_df,
            start_date, end_date, _gf_chans, None,  # no product filter
        )

        # Keep sel_item/sel_chan variables for backward-compatible chart logic below
        sel_chan = _gf_chans or sorted(range_df["channel"].unique()) if not range_df.empty else []
        sel_item = _gf_prods or []

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
                color=color_theme, barmode="stack", height=480,
                color_discrete_sequence=brand_color_sequence(),
            )
            fig.add_hline(
                y=avg_drr, line_dash="dash", line_color="#E74C3C",
                annotation_text="Avg DRR",
                annotation_font_color="#E74C3C",
            )
            if show_labels:
                fig.update_traces(texttemplate="%{y:.2s}", textposition="inside",
                                  textfont_size=10)
                totals = plot_df.groupby("date")[target_col].sum().reset_index()
                fig.add_scatter(
                    x=totals["date"],
                    y=totals[target_col],
                    text=totals[target_col].apply(lambda x: f"{x:,.0f}"),
                    mode="text",
                    textposition="top center",
                    showlegend=False,
                    textfont=dict(color="#1C1917", size=10, family="JetBrains Mono"),
                )
            fig = apply_chart_theme(fig)
            # Increase top margin so total labels above bars aren't clipped
            fig.update_layout(margin=dict(l=12, r=12, t=48, b=12))
            st.plotly_chart(fig, use_container_width=True)
            display_cols = [c for c in filtered.columns if c not in ("date_dt", "id")]
            st.dataframe(filtered[display_cols], hide_index=True)

# ══════════════════════════════════════════════
# TAB 2 – DEEP DIVE  (new)
# ══════════════════════════════════════════════
if _TAB_DEEPDIVE >= 0:
  with tabs[_TAB_DEEPDIVE]:
    if history_df.empty:
        empty_state("📊", "No data yet", "Admin must upload sales data via the Smart Upload tab first.")
    else:
        # Ensure date column is parsed (re-do in case tab 1 was skipped)
        # date_dt already parsed at startup — no re-parse needed

        st.subheader("🔬 Deep Dive Analytics")

        # ── GLOBAL FILTER: Date + Channel + Product ───────────────────────────
        # The local Period radio and date pickers have been removed.
        # This tab now reads the shared filter state set in the Global Filter Bar.
        # To change the date range, channels, or products, adjust the controls
        # above the tab strip — changes reflect here immediately.
        _gf_dd   = get_global_filters()
        dd_start = _gf_dd["start"]
        dd_end   = _gf_dd["end"]

        # Styled active-filter pill row
        active_filter_pill(dd_start, dd_end, _gf_dd["channels"], _gf_dd["products"])

        # ── SMART FILTER CACHE (Deep Dive) ───────────────────────────────────
        # Same hash-check pattern: only re-scans history_df when the
        # filter state actually changed. Display-toggle reruns (WoW metric,
        # heatmap metric) skip this expensive Boolean mask entirely.
        dd_df = get_filtered_df(
            "deep_dive", history_df,
            dd_start, dd_end, _gf_dd["channels"], _gf_dd["products"],
        )

        if dd_df.empty:
            st.warning("No data in the selected period.")
        else:

            dd_days  = max((dd_end - dd_start).days + 1, 1)
            today_dd = datetime.now().date()   # needed by Ops Health Check below
            dd_df["week_label"] = dd_df["date_dt"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
            dd_df["dow"]        = dd_df["date_dt"].dt.day_name()
    
            st.divider()
    
            # ════════════════════════════════
            # VIEW 1 — Channel Mix Donut
            # ════════════════════════════════
            section_header("🍩", "Channel Revenue Mix", "revenue & unit share")
    
            chan_rev = dd_df.groupby("channel")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)
            chan_qty = dd_df.groupby("channel")["qty_sold"].sum().reset_index().sort_values("qty_sold", ascending=False)
    
            donut_c1, donut_c2 = st.columns(2)
    
            with donut_c1:
                fig_donut_rev = px.pie(
                    chan_rev, values="revenue", names="channel",
                    hole=0.55, height=380,
                    title="By Revenue (₹)",
                    color_discrete_sequence=brand_color_sequence(),
                )
                fig_donut_rev.update_traces(textinfo="label+percent", textposition="outside")
                fig_donut_rev.update_layout(showlegend=False, margin=dict(t=50, b=10, l=10, r=10))
                # Centre annotation
                total_rev = chan_rev["revenue"].sum()
                fig_donut_rev.add_annotation(
                    text=f"₹{total_rev/1000:.1f}K", x=0.5, y=0.5,
                    font=dict(size=18, color="black"), showarrow=False,
                )
                fig_donut_rev = apply_chart_theme(fig_donut_rev)
                st.plotly_chart(fig_donut_rev, use_container_width=True)
    
            with donut_c2:
                fig_donut_qty = px.pie(
                    chan_qty, values="qty_sold", names="channel",
                    hole=0.55, height=380,
                    title="By Units Sold",
                    color_discrete_sequence=brand_color_sequence(),
                )
                fig_donut_qty.update_traces(textinfo="label+percent", textposition="outside")
                fig_donut_qty.update_layout(showlegend=False, margin=dict(t=50, b=10, l=10, r=10))
                total_qty = chan_qty["qty_sold"].sum()
                fig_donut_qty.add_annotation(
                    text=f"{total_qty:,.0f} units", x=0.5, y=0.5,
                    font=dict(size=14, color="black"), showarrow=False,
                )
                fig_donut_qty = apply_chart_theme(fig_donut_qty)
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
            section_header("🏆", "SKU Performance Ranking", "by revenue")
    
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
            fig_sku = apply_chart_theme(fig_sku)
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
            section_header("📈", "Week-over-Week Performance")
    
            wow_metric = st.radio("WoW Metric:", ["Revenue (₹)", "Quantity (Units)"], horizontal=True, key="wow_metric")
            wow_col    = "revenue" if "Revenue" in wow_metric else "qty_sold"
            wow_prefix = "₹" if "Revenue" in wow_metric else ""

            # lazy_section: skip heavy groupby if only display metric changed
            # but data (dd_df) and metric (wow_col) are the same
            _wow_dep = (id(dd_df), wow_col, dd_start, dd_end)
            with lazy_section("wow_chart", depends_on=_wow_dep):
              if not is_lazy_skip("wow_chart"):
                weekly = (
                  dd_df.groupby(["week_label", "channel"])[wow_col]
                  .sum()
                  .reset_index()
                  .sort_values("week_label")
                )
                st.session_state["_wow_weekly"] = weekly
              weekly = st.session_state.get("_wow_weekly", pd.DataFrame())
    
            fig_wow = px.line(
                weekly, x="week_label", y=wow_col, color="channel",
                markers=True, height=420,
                labels={"week_label": "Week Starting", wow_col: wow_metric},
                color_discrete_sequence=brand_color_sequence(),
            )
            fig_wow.update_traces(line_width=2.5, marker_size=8)
            fig_wow.update_layout(xaxis_title="Week", hovermode="x unified")
            fig_wow = apply_chart_theme(fig_wow)
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
            section_header("🗓️", "Day-of-Week Heatmap", "when do customers buy?")
    
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
            fig_heat = apply_chart_theme(fig_heat)
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
            section_header("📦", "SKU × Channel Matrix", "distribution gaps")
    
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
            fig_matrix = apply_chart_theme(fig_matrix)
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
            section_header("⚠️", "Ops Health Check", "last 7 days")
    
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
# TAB – PERFORMANCE MARKETING (all roles, index 2)
# ══════════════════════════════════════════════
if _TAB_MARKETING >= 0:
  with tabs[_TAB_MARKETING]:
    # Check if any channels with city data exist in the sales history
    _has_city_data = (
        not history_df.empty
        and "city" in history_df.columns
        and history_df["city"].notna().any()
    )
    if not _has_city_data:
        empty_state(
            "📣", "City-Level Data Required",
            "Performance Marketing uses city-level sales. Upload data from a channel "
            "with 'Has city-level data' enabled to unlock this module."
        )
    else:
        render_marketing_tab(role)

# ══════════════════════════════════════════════
# TAB 4 – SMART UPLOAD  (admin only)
# ══════════════════════════════════════════════
if role == "admin":
    with tabs[_TAB_UPLOAD]:
        page_header("Smart Upload", "Map & sync sales reports to cloud", role="")

        channels = master_chans["name"].tolist() if not master_chans.empty else []
        if not channels:
            st.warning("No channels configured. Add channels in the Configuration tab first.")
        else:
          selected_channel = st.selectbox("Select Channel", channels)
          up_file = st.file_uploader("Upload File", type=["csv", "xlsx"])

          if up_file and selected_channel:
            _upload_ok = True
            try:
                raw_df = (
                    pd.read_csv(up_file)
                    if up_file.name.lower().endswith(".csv")
                    else pd.read_excel(up_file)
                )
            except Exception as e:
                st.error(f"Could not read file: {e}")
                _upload_ok = False

            if _upload_ok:
              st.write(f"**Preview** — {len(raw_df)} rows × {len(raw_df.columns)} cols")
              st.dataframe(raw_df.head(5), hide_index=True)

              cols = ["None"] + raw_df.columns.tolist()

              # Channels that carry city-level sales data — driven by master_channels config
              needs_city = requires_city_channel(selected_channel)

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
                          "City Column (optional)",
                          cols,
                          key="city_col",
                          help="Select a city/region column if available, or leave as None to set a fixed city below.",
                      )
                      if city_col == "None":
                          city_fallback = st.selectbox(
                              "City / Region (no column available)",
                              ["National", "Pan India", "Online", "Unknown"],
                              key="city_fallback",
                              help="All rows will be tagged with this value since no city column is available.",
                          )
                      else:
                          city_fallback = None
                  else:
                      city_col      = "None"
                      city_fallback = None

              # Validate mandatory column picks — city is no longer blocking
              mandatory = [("Product", p_col), ("Qty", q_col), ("Revenue", r_col)]
              missing = [name for name, col in mandatory if col == "None"]
              if missing:
                  st.info(f"Please select columns for: {', '.join(missing)}")
              else:
                # ── Build composite key safely ──────────────────────────────────
                work_df = raw_df.copy()
                work_df["__prod__"] = work_df[p_col].astype(object).fillna("").astype(str).str.strip()

                if v_col != "None":
                    work_df["__var__"] = work_df[v_col].astype(str).str.strip()
                    work_df["m_key"]   = work_df["__prod__"].astype(str) + " | " + work_df["__var__"].astype(str)
                else:
                    work_df["m_key"] = work_df["__prod__"].astype(str)

                # ── Filter out totals rows ──────────────────────────────────────
                SKIP_LABELS = {"total", "grand total", "subtotal", "nan", ""}
                valid_mask  = ~work_df["__prod__"].str.lower().isin(SKIP_LABELS)
                work_df     = work_df[valid_mask].copy()

                masters = master_skus["name"].tolist() if not master_skus.empty else []

                if work_df.empty:
                    st.error("No valid data rows found after filtering. Check column mapping.")
                elif not masters:
                    st.warning("No master SKUs configured. Add SKUs in the Configuration tab first.")
                else:
                  st.markdown("#### 🗺 Map Raw Product Names → Master SKUs")
                  unique_keys = sorted(work_df["m_key"].dropna().astype(str).unique().tolist())
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
                                  else city_fallback  # "National" / "Pan India" / etc, or None
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
                          else:
                              group_cols = ["date", "channel", "item_name", "city"]
                              final_df = (
                                  pd.DataFrame(raw_rows)
                                  .groupby(group_cols, dropna=False)
                                  .agg({"qty_sold": "sum", "revenue": "sum"})
                                  .reset_index()
                              )

                              try:
                                  final_df["qty_sold"] = final_df["qty_sold"].fillna(0.0)
                                  final_df["revenue"]  = final_df["revenue"].fillna(0.0)
                                  final_df["city"]     = (
                                      final_df["city"]
                                      .astype(object)
                                      .where(final_df["city"].notna(), other=None)
                                  )
                                  CHUNK   = 500
                                  records = final_df.to_dict(orient="records")
                                  # ── Progress bar upload ───────────────────
                                  with upload_progress_bar(
                                      len(records), chunk_size=CHUNK,
                                      label=f"Uploading to Supabase ({selected_channel})"
                                  ) as tick:
                                      for i in range(0, len(records), CHUNK):
                                          chunk = records[i : i + CHUNK]
                                          res   = supabase.table("sales").upsert(
                                              chunk,
                                              on_conflict="date,channel,item_name,city",
                                          ).execute()
                                          if hasattr(res, "error") and res.error:
                                              errors.append(f"Upsert chunk {i//CHUNK+1} error: {res.error}")
                                          tick()
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
    # TAB – MONTHLY CHANNEL UPLOAD  (admin only)
    # ══════════════════════════════════════════
    with tabs[_TAB_MONTHLY_UPLOAD]:
        page_header("Monthly Channel Upload", "Spread channel totals across days", role="")
        st.caption(
            "For channels where daily data is unavailable. "
            "Sales can be entered for a whole month (auto-split across days) "
            "or for specific days. City-level data is optional."
        )

        # Only show channels marked as monthly
        monthly_chan_list = []
        if not master_chans.empty:
            mc = master_chans.copy()
            if "is_monthly" in mc.columns:
                monthly_chan_list = mc[mc["is_monthly"] == True]["name"].tolist()

        if not monthly_chan_list:
            st.info(
                "No monthly channels configured yet. "
                "Go to **🛠 Configuration** → Add Channel → check 'Monthly reporting channel'."
            )
        else:
          mc_channel = st.selectbox("Select Monthly Channel", monthly_chan_list, key="mc_chan")
          mc_needs_city = requires_city_channel(mc_channel)

          st.divider()
          st.markdown("#### 📆 Reporting Period")
          mc_col1, mc_col2 = st.columns(2)
          with mc_col1:
              mc_year  = st.number_input("Year",  min_value=2020, max_value=2100,
                                          value=datetime.now().year,  step=1, key="mc_year")
          with mc_col2:
              mc_month = st.number_input("Month", min_value=1,    max_value=12,
                                          value=datetime.now().month, step=1, key="mc_month")

          days_in_month = calendar.monthrange(int(mc_year), int(mc_month))[1]
          all_days_of_month = list(range(1, days_in_month + 1))

          st.markdown("#### 🗓️ Day Coverage")
          mc_day_mode = st.radio(
              "Which days does this data cover?",
              ["Entire month (split equally across all days)", "Specific days of the month"],
              key="mc_day_mode",
          )

          if mc_day_mode.startswith("Specific"):
              selected_days = st.multiselect(
                  f"Select days in {calendar.month_name[int(mc_month)]} {int(mc_year)}",
                  all_days_of_month, default=all_days_of_month, key="mc_sel_days",
              )
              if not selected_days:
                  st.warning("Select at least one day.")
                  spread_days = []
              else:
                  spread_days = sorted(selected_days)
          else:
              spread_days = all_days_of_month

          if spread_days:
            st.divider()
            st.markdown("#### 📂 Data Entry Method")
            mc_entry_mode = st.radio(
                "How would you like to enter data?",
                ["Upload Excel / CSV file", "Manual product entry (no file)"],
                key="mc_entry_mode",
            )

            mc_masters = master_skus["name"].tolist() if not master_skus.empty else []
            if not mc_masters:
                st.warning("No master SKUs configured. Add SKUs in the Configuration tab first.")
            else:
              mc_city_val = None
              if mc_needs_city:
                  mc_city_val = st.text_input(
                      "City / Region (optional — leave blank if not available)",
                      key="mc_city",
                  ).strip() or None

              # ─── PATH A — FILE UPLOAD ───────────────────────────────────────
              if mc_entry_mode.startswith("Upload"):
                  mc_file = st.file_uploader(
                      "Upload monthly sales file", type=["csv", "xlsx"], key="mc_file_uploader",
                  )
                  if mc_file:
                      _mc_ok = True
                      try:
                          mc_raw = (
                              pd.read_csv(mc_file)
                              if mc_file.name.lower().endswith(".csv")
                              else pd.read_excel(mc_file)
                          )
                      except Exception as e:
                          st.error(f"Could not read file: {e}")
                          _mc_ok = False

                      if _mc_ok:
                          st.write(f"**Preview** — {len(mc_raw)} rows × {len(mc_raw.columns)} cols")
                          st.dataframe(mc_raw.head(5), hide_index=True)

                          mc_cols = ["None"] + mc_raw.columns.tolist()
                          mcc1, mcc2, mcc3 = st.columns(3)
                          with mcc1:
                              mc_p_col = st.selectbox("Product Column *", mc_cols, key="mc_p_col")
                              mc_v_col = st.selectbox("Variant Column (optional)", mc_cols, key="mc_v_col")
                          with mcc2:
                              mc_q_col = st.selectbox("Qty Column *", mc_cols, key="mc_q_col")
                              mc_r_col = st.selectbox("Revenue Column *", mc_cols, key="mc_r_col")
                          with mcc3:
                              mc_d_col = st.selectbox(
                                  "Date/Day Column (optional — leave None to use period above)",
                                  mc_cols, key="mc_d_col",
                                  help="If your file has a date or day-of-month column, select it here.",
                              )

                          mc_mandatory = [("Product", mc_p_col), ("Qty", mc_q_col), ("Revenue", mc_r_col)]
                          mc_missing = [n for n, c in mc_mandatory if c == "None"]
                          if mc_missing:
                              st.info(f"Please select columns for: {', '.join(mc_missing)}")
                          else:
                              mc_work = mc_raw.copy()
                              mc_work["__prod__"] = mc_work[mc_p_col].astype(str).str.strip()
                              if mc_v_col != "None":
                                  mc_work["__var__"] = mc_work[mc_v_col].astype(str).str.strip()
                                  mc_work["m_key"] = mc_work["__prod__"] + " | " + mc_work["__var__"]
                              else:
                                  mc_work["m_key"] = mc_work["__prod__"]
                              SKIP_LABELS_MC = {"total", "grand total", "subtotal", "nan", ""}
                              mc_work = mc_work[~mc_work["__prod__"].str.lower().isin(SKIP_LABELS_MC)].copy()

                              if mc_work.empty:
                                  st.error("No valid product rows found after filtering.")
                              else:
                                  st.markdown("#### 🗺 Map Raw Product Names → Master SKUs")
                                  saved_map_mc: dict = {}
                                  if not item_map_df.empty:
                                      saved_map_mc = dict(zip(item_map_df["raw_name"], item_map_df["master_name"]))
                                  mc_sku_map: dict = {}
                                  for k in sorted(mc_work["m_key"].dropna().astype(str).unique().tolist()):
                                      saved = saved_map_mc.get(k, "")
                                      default_idx = mc_masters.index(saved) if saved in mc_masters else 0
                                      mc_sku_map[k] = st.selectbox(
                                          f"Map: `{k}`", mc_masters, index=default_idx, key=f"mc_sku_{k}"
                                      )

                                  if st.button("🚀 Sync Monthly Data to Cloud", key="mc_file_sync"):
                                      mc_errors: list = []
                                      with st.spinner("Saving mappings…"):
                                          for raw_n, master_n in mc_sku_map.items():
                                              try:
                                                  supabase.table("item_map").upsert(
                                                      {"raw_name": raw_n, "master_name": master_n},
                                                      on_conflict="raw_name",
                                                  ).execute()
                                              except Exception as e:
                                                  mc_errors.append(f"Mapping failed for '{raw_n}': {e}")

                                      with st.spinner("Spreading sales across days…"):
                                          mc_rows_to_insert = []
                                          for _, r in mc_work.iterrows():
                                              row_days = spread_days
                                              if mc_d_col != "None":
                                                  raw_dval = str(r[mc_d_col]).strip()
                                                  try:
                                                      day_int = int(float(raw_dval))
                                                      row_days = [day_int] if 1 <= day_int <= days_in_month else spread_days
                                                  except (ValueError, TypeError):
                                                      try:
                                                          parsed = pd.to_datetime(raw_dval)
                                                          row_days = [parsed.day] if (parsed.year == int(mc_year) and parsed.month == int(mc_month)) else spread_days
                                                      except Exception:
                                                          row_days = spread_days
                                              n_days = len(row_days)
                                              qty_per = round(clean_num(r[mc_q_col]) / n_days, 4) if n_days else 0
                                              rev_per = round(clean_num(r[mc_r_col]) / n_days, 4) if n_days else 0
                                              for day in row_days:
                                                  mc_rows_to_insert.append({
                                                      "date":      f"{int(mc_year):04d}-{int(mc_month):02d}-{day:02d}",
                                                      "channel":   mc_channel,
                                                      "item_name": mc_sku_map[r["m_key"]],
                                                      "qty_sold":  qty_per,
                                                      "revenue":   rev_per,
                                                      "city":      mc_city_val,
                                                  })

                                      if not mc_rows_to_insert:
                                          st.error("No rows generated.")
                                      else:
                                          gc = ["date", "channel", "item_name", "city"]
                                          mc_final = (
                                              pd.DataFrame(mc_rows_to_insert)
                                              .groupby(gc, dropna=False)
                                              .agg({"qty_sold": "sum", "revenue": "sum"})
                                              .reset_index()
                                          )
                                          mc_final["qty_sold"] = mc_final["qty_sold"].fillna(0.0)
                                          mc_final["revenue"]  = mc_final["revenue"].fillna(0.0)
                                          mc_final["city"]     = mc_final["city"].astype(object).where(mc_final["city"].notna(), other=None)
                                          try:
                                              CHUNK = 500
                                              recs = mc_final.to_dict(orient="records")
                                              with upload_progress_bar(
                                                  len(recs), chunk_size=CHUNK,
                                                  label=f"Monthly upload ({mc_channel})"
                                              ) as tick:
                                                  for i in range(0, len(recs), CHUNK):
                                                      res = supabase.table("sales").upsert(recs[i:i+CHUNK], on_conflict="date,channel,item_name,city").execute()
                                                      if hasattr(res, "error") and res.error:
                                                          mc_errors.append(f"Chunk {i//CHUNK+1}: {res.error}")
                                                      tick()
                                          except Exception as e:
                                              mc_errors.append(f"Upload failed: {e}")
                                          if mc_errors:
                                              for err in mc_errors: st.error(err)
                                          else:
                                              st.success(f"✅ Synced {len(mc_final)} records for '{mc_channel}' — {calendar.month_name[int(mc_month)]} {int(mc_year)} across {len(spread_days)} day(s).")
                                              invalidate_data_cache()
                                              st.rerun()

              # ─── PATH B — MANUAL ENTRY ──────────────────────────────────────
              else:
                  st.markdown("#### ✏️ Manual Product Entry")
                  st.caption("Add each product's quantity and revenue for the selected period.")
                  if "mc_manual_rows" not in st.session_state:
                      st.session_state["mc_manual_rows"] = []

                  with st.form("mc_add_row_form", clear_on_submit=True):
                      fm1, fm2, fm3, fm4 = st.columns([3, 2, 2, 1])
                      with fm1:
                          fm_sku = st.selectbox("Product (Master SKU)", mc_masters, key="fm_sku")
                      with fm2:
                          fm_qty = st.number_input("Quantity", min_value=0.0, step=1.0, key="fm_qty")
                      with fm3:
                          fm_rev = st.number_input("Revenue (₹)", min_value=0.0, step=0.01, key="fm_rev")
                      with fm4:
                          fm_day = st.text_input("Day(s) (opt.)", placeholder="e.g. 5 or 5,10,15", key="fm_day",
                                                  help="Leave blank to spread across the whole period.")
                      if st.form_submit_button("➕ Add Row"):
                          st.session_state["mc_manual_rows"].append({
                              "sku": fm_sku, "qty": fm_qty, "revenue": fm_rev, "day_spec": fm_day.strip(),
                          })

                  if st.session_state["mc_manual_rows"]:
                      st.dataframe(
                          pd.DataFrame(st.session_state["mc_manual_rows"]).rename(columns={
                              "sku": "Product", "qty": "Qty", "revenue": "Revenue (₹)", "day_spec": "Day(s)"
                          }), hide_index=True, use_container_width=True
                      )
                      rm1, rm2 = st.columns(2)
                      with rm1:
                          if st.button("🗑️ Clear All Rows", key="mc_clear"):
                              st.session_state["mc_manual_rows"] = []
                              st.rerun()
                      with rm2:
                          if st.button("🚀 Sync Manual Data to Cloud", key="mc_manual_sync"):
                              mc_m_errors: list = []
                              mc_manual_inserts = []
                              for entry in st.session_state["mc_manual_rows"]:
                                  day_spec = entry.get("day_spec", "").strip()
                                  if day_spec:
                                      try:
                                          parsed_days = [int(d.strip()) for d in day_spec.split(",") if d.strip().isdigit()]
                                          row_days = [d for d in parsed_days if 1 <= d <= days_in_month] or spread_days
                                      except Exception:
                                          row_days = spread_days
                                  else:
                                      row_days = spread_days
                                  n_days = len(row_days)
                                  qty_per = round(entry["qty"] / n_days, 4) if n_days else 0
                                  rev_per = round(entry["revenue"] / n_days, 4) if n_days else 0
                                  for day in row_days:
                                      mc_manual_inserts.append({
                                          "date":      f"{int(mc_year):04d}-{int(mc_month):02d}-{day:02d}",
                                          "channel":   mc_channel,
                                          "item_name": entry["sku"],
                                          "qty_sold":  qty_per,
                                          "revenue":   rev_per,
                                          "city":      mc_city_val,
                                      })
                              if not mc_manual_inserts:
                                  st.error("No records to insert.")
                              else:
                                  gc = ["date", "channel", "item_name", "city"]
                                  mc_m_final = (
                                      pd.DataFrame(mc_manual_inserts)
                                      .groupby(gc, dropna=False)
                                      .agg({"qty_sold": "sum", "revenue": "sum"})
                                      .reset_index()
                                  )
                                  mc_m_final["qty_sold"] = mc_m_final["qty_sold"].fillna(0.0)
                                  mc_m_final["revenue"]  = mc_m_final["revenue"].fillna(0.0)
                                  mc_m_final["city"]     = mc_m_final["city"].astype(object).where(mc_m_final["city"].notna(), other=None)
                                  try:
                                      CHUNK = 500
                                      recs = mc_m_final.to_dict(orient="records")
                                      with upload_progress_bar(
                                          len(recs), chunk_size=CHUNK,
                                          label=f"Manual upload ({mc_channel})"
                                      ) as tick:
                                          for i in range(0, len(recs), CHUNK):
                                              res = supabase.table("sales").upsert(recs[i:i+CHUNK], on_conflict="date,channel,item_name,city").execute()
                                              if hasattr(res, "error") and res.error:
                                                  mc_m_errors.append(f"Chunk {i//CHUNK+1}: {res.error}")
                                              tick()
                                  except Exception as e:
                                      mc_m_errors.append(f"Upload failed: {e}")
                                  if mc_m_errors:
                                      for err in mc_m_errors: st.error(err)
                                  else:
                                      st.success(f"✅ Synced {len(mc_m_final)} records for '{mc_channel}' — {calendar.month_name[int(mc_month)]} {int(mc_year)} across {len(spread_days)} day(s).")
                                      st.session_state["mc_manual_rows"] = []
                                      invalidate_data_cache()
                                      st.rerun()
                  else:
                      st.info("No rows added yet. Use the form above to add products.")

              st.divider()
              st.info(
                  "💡 **To delete monthly data**: use the **Delete Specific Entry** panel "
                  "in the sidebar — select the channel and any date within the uploaded month. "
                  "All rows for that channel & date will be removed."
              )


    # ══════════════════════════════════════════
    # ══════════════════════════════════════════
    # TAB – CONFIGURATION  (admin only)
    # ══════════════════════════════════════════
    with tabs[_TAB_CONFIG]:
        page_header("System Configuration", "Manage SKUs, channels, and mappings", role="")
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
            ch_is_monthly    = st.checkbox("📅 Monthly reporting channel", value=False,
                                           help="Enable if daily data is unavailable — sales are reported monthly.")
            ch_requires_city = st.checkbox("🏙️ Has city-level data", value=True,
                                           help="Uncheck if city breakdown is unavailable (disables Channel Performance & Marketing for this channel).")
            if st.button("Add Channel") and n_ch.strip():
                safe_ch = sanitize(n_ch)
                if safe_ch:
                    try:
                        supabase.table("master_channels").insert({
                            "name":          safe_ch,
                            "is_monthly":    ch_is_monthly,
                            "requires_city": ch_requires_city,
                        }).execute()
                        st.success(f"Added channel: {safe_ch}")
                        invalidate_data_cache()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add channel: {e}")
            if not master_chans.empty:
                display_chans = master_chans.copy()
                # Friendly column names
                rename_map = {"name": "Channel", "is_monthly": "Monthly?", "requires_city": "City Data?"}
                display_chans = display_chans.rename(columns={k: v for k, v in rename_map.items() if k in display_chans.columns})
                st.dataframe(display_chans, hide_index=True)

        st.divider()
        st.markdown("#### 🗺 Current Item Mappings")
        if not item_map_df.empty:
            st.dataframe(item_map_df, hide_index=True)
        else:
            st.info("No mappings saved yet.")

# ══════════════════════════════════════════════
# TAB – CHANNEL PERFORMANCE (admin + viewer, last tab)
# ══════════════════════════════════════════════
if _TAB_CHANPERF >= 0:
  with tabs[_TAB_CHANPERF]:
    _has_city_data_cp = (
        not history_df.empty
        and "city" in history_df.columns
        and history_df["city"].notna().any()
    )
    if not _has_city_data_cp:
        empty_state(
            "📦", "City-Level Data Required",
            "Channel Performance requires city-tagged sales records. "
            "Configure channels with 'Has city-level data' in Settings and upload data."
        )
    else:
        render_channel_performance_tab(supabase, master_skus, role)

# ══════════════════════════════════════════════
# TAB – VENDING (admin + viewer)
# ══════════════════════════════════════════════
if _TAB_VENDING >= 0:
  with tabs[_TAB_VENDING]:
    render_vending_tab(role, supabase_client=supabase)

# ══════════════════════════════════════════════
# TAB – S&OP (admin + viewer)
# ══════════════════════════════════════════════
if _TAB_SOP >= 0:
  with tabs[_TAB_SOP]:
    render_sop_tab(supabase, history_df, master_skus, master_chans, role)

# ══════════════════════════════════════════════
# TAB – USER MANAGEMENT (admin only)
# ══════════════════════════════════════════════
if _TAB_USERMGMT >= 0:
  with tabs[_TAB_USERMGMT]:
    if role != "admin":
        empty_state("🔒", "Access Denied", "User Management is only available to administrators.")
    else:
        render_user_management_tab(supabase)
