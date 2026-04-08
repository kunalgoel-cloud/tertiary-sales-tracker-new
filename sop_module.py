"""
sop_module.py
─────────────────────────────────────────────────────────────────────────────
Sales & Operations Planning (S&OP) Tab for Mamanourish Executive Tracker.

Entry point:  render_sop_tab(supabase_client, history_df, master_skus_df,
                             master_chans_df, role)

DESIGN PHILOSOPHY
─────────────────
1.  BASE FORECAST  — weighted average DRR from historical sales.
    Recent weeks are weighted more (exponential decay).  As more history
    accumulates the model automatically improves.

2.  MARKETING UPLIFT  — user enters planned ₹ spend per channel.
    Uplift is derived from the observed revenue-per-spend ratio (ROAS)
    from the marketing history already in the system.  When no marketing
    data exists a conservative default ROAS is applied.

3.  ATTRIBUTION  — every rupee of delta between the "organic" forecast
    and the "adjusted" forecast is tagged as "Marketing Driven" so the
    system always knows WHY a prediction changed.

4.  ACTUAL VS PREDICTED  — as the month progresses, actual sales are
    fetched from the sales table and overlaid on the forecast.
    Variance is decomposed into:
        • Organic variance   (market / demand shift)
        • Marketing variance (spend came in above / below plan)

5.  PERSISTENCE  — plans are saved to a Supabase table
    `sop_plans` so history accumulates and comparisons survive reruns.

TABLE SCHEMA (run once in Supabase SQL Editor)
───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sop_plans (
    id              SERIAL PRIMARY KEY,
    plan_month      TEXT NOT NULL,          -- 'YYYY-MM'
    channel         TEXT NOT NULL,
    item_name       TEXT NOT NULL,
    base_qty_30d    NUMERIC,               -- organic forecast
    mkt_uplift_qty  NUMERIC,               -- marketing-driven units
    total_qty_30d   NUMERIC,               -- base + uplift
    base_rev_30d    NUMERIC,
    mkt_uplift_rev  NUMERIC,
    total_rev_30d   NUMERIC,
    planned_mkt_spend NUMERIC DEFAULT 0,   -- ₹ budget entered by user
    assumed_roas    NUMERIC DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(plan_month, channel, item_name)
);
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_ROAS          = 3.0    # assumed ROAS when no marketing data available
_DECAY_HALFLIFE_WEEKS  = 4      # recent weeks weighted 2× more than 4-week-old data
_MIN_HISTORY_DAYS      = 7      # need at least this many days to build a forecast
_PLAN_TABLE            = "sop_plans"


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_plan(sb, plan_month: str) -> pd.DataFrame:
    try:
        res = sb.table(_PLAN_TABLE).select("*").eq("plan_month", plan_month).execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _save_plan(sb, rows: list[dict]) -> tuple[bool, str]:
    try:
        sb.table(_PLAN_TABLE).upsert(
            rows, on_conflict="plan_month,channel,item_name"
        ).execute()
        return True, ""
    except Exception as e:
        return False, str(e)


def _load_all_plans(sb) -> pd.DataFrame:
    try:
        res = sb.table(_PLAN_TABLE).select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# FORECASTING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _weighted_drr(daily: pd.Series, halflife_weeks: int = _DECAY_HALFLIFE_WEEKS) -> float:
    """
    Compute a decay-weighted average of a daily time series.
    More recent days contribute more to the estimate.
    """
    if daily.empty:
        return 0.0
    n = len(daily)
    # Exponential weights: newest day = weight 1, oldest = weight e^(-lambda*n)
    lam = math.log(2) / (halflife_weeks * 7)
    weights = np.array([math.exp(-lam * (n - 1 - i)) for i in range(n)])
    weights /= weights.sum()
    return float(np.dot(weights, daily.fillna(0).values))


def build_base_forecast(
    history_df:   pd.DataFrame,
    forecast_days: int = 30,
    lookback_days: int = 90,
    mkt_spend_by_date: dict | None = None,   # {date_str: total_spend} from marketing DB
) -> pd.DataFrame:
    """
    Compute a decay-weighted ORGANIC baseline forecast per (channel, item_name).

    ORGANIC DRR LOGIC
    ─────────────────
    Total sales on any day = organic + marketing-driven sales.
    We want the pure organic component so the marketing uplift is not
    double-counted when we later add it back.

    Strategy (in order of preference):
      A) Use only zero-spend days (days with no marketing budget) to
         compute DRR — these are uncontaminated organic observations.
      B) If A gives < _MIN_HISTORY_DAYS, use all days but deflate DRR
         by the channel's historical organic_share:
             organic_share = revenue on zero-spend days / total revenue
         This preserves the correct SKU × Channel split ratios.
      C) If zero-spend days don't exist at all, use full DRR as proxy
         (conservative — will be refined as data accumulates).

    SKU REVENUE SHARE is computed within each channel so the relative
    mix is preserved regardless of the absolute DRR level.

    Returns DataFrame with columns:
        channel, item_name,
        base_qty_30d, base_rev_30d,        ← organic forecast
        total_qty_30d_hist, total_rev_30d_hist,  ← total historical DRR × 30
        rev_share_in_channel,              ← this SKU's % of channel revenue
        organic_share,                     ← % of revenue from zero-spend days
        avg_price, drr_qty, history_days_used
    """
    if history_df.empty:
        return pd.DataFrame()

    df = history_df.copy()

    # Ensure key columns are plain Python str (not StringDtype with pd.NA)
    for col in ["channel", "item_name"]:
        if col in df.columns:
            df[col] = df[col].astype(object).fillna("").astype(str).str.strip()
    df = df[df["channel"] != ""]
    df = df[df["item_name"] != ""]

    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    # Use whatever data is available within the lookback window.
    # If lookback exceeds available history, use all history (no truncation error).
    latest = df["date_dt"].max().date()
    cutoff = latest - timedelta(days=lookback_days)
    df = df[df["date_dt"].dt.date >= cutoff]

    if df.empty:
        return pd.DataFrame()

    # Build set of zero-spend dates (dates with no marketing spend)
    zero_spend_dates: set = set()
    if mkt_spend_by_date:
        all_dates_in_window = set(df["date_dt"].dt.strftime("%Y-%m-%d").unique())
        zero_spend_dates    = all_dates_in_window - set(mkt_spend_by_date.keys())
    else:
        # No marketing data at all → treat all dates as organic
        zero_spend_dates = set(df["date_dt"].dt.strftime("%Y-%m-%d").unique())

    rows = []
    for (channel, item), grp in df.groupby(["channel", "item_name"]):
        date_range = pd.date_range(grp["date_dt"].min(), grp["date_dt"].max(), freq="D")
        daily_qty  = grp.groupby("date_dt")["qty_sold"].sum().reindex(date_range, fill_value=0)
        daily_rev  = grp.groupby("date_dt")["revenue"].sum().reindex(date_range, fill_value=0)

        history_days = len(date_range)
        if history_days < _MIN_HISTORY_DAYS:
            continue

        # ── Total DRR (includes all days) ──────────────────────────────────
        drr_qty_total = _weighted_drr(daily_qty)
        drr_rev_total = _weighted_drr(daily_rev)
        avg_price     = round(drr_rev_total / drr_qty_total, 2) if drr_qty_total > 0 else 0.0

        # ── Organic DRR ─────────────────────────────────────────────────────
        zero_mask = pd.Series(
            [d.strftime("%Y-%m-%d") in zero_spend_dates for d in date_range],
            index=date_range,
        )
        organic_qty_days = daily_qty[zero_mask]
        organic_rev_days = daily_rev[zero_mask]

        if len(organic_qty_days) >= _MIN_HISTORY_DAYS:
            # Strategy A: enough clean days → use zero-spend DRR directly
            drr_qty_org = _weighted_drr(organic_qty_days)
            drr_rev_org = _weighted_drr(organic_rev_days)
            organic_share = (
                organic_rev_days.sum() / daily_rev.sum()
                if daily_rev.sum() > 0 else 1.0
            )
        elif len(organic_qty_days) > 0:
            # Strategy B: some clean days but < MIN_HISTORY_DAYS
            # Use organic_share ratio to deflate full-period DRR
            organic_share = (
                organic_rev_days.sum() / daily_rev.sum()
                if daily_rev.sum() > 0 else 1.0
            )
            drr_qty_org = drr_qty_total * organic_share
            drr_rev_org = drr_rev_total * organic_share
        else:
            # Strategy C: no zero-spend days — use total as proxy
            organic_share = 1.0
            drr_qty_org   = drr_qty_total
            drr_rev_org   = drr_rev_total

        rows.append({
            "channel":               channel,
            "item_name":             item,
            "base_qty_30d":          round(drr_qty_org   * forecast_days, 2),
            "base_rev_30d":          round(drr_rev_org   * forecast_days, 2),
            "total_qty_30d_hist":    round(drr_qty_total * forecast_days, 2),
            "total_rev_30d_hist":    round(drr_rev_total * forecast_days, 2),
            "drr_qty":               round(drr_qty_org,   3),
            "drr_rev":               round(drr_rev_org,   2),
            "avg_price":             avg_price,
            "organic_share":         round(organic_share, 3),
            "history_days_used":     history_days,
            "organic_days_used":     int(zero_mask.sum()),
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)

    # ── Revenue share within each channel (used for budget allocation) ───────
    chan_total = result.groupby("channel")["base_rev_30d"].transform("sum")
    result["rev_share_in_channel"] = np.where(
        chan_total > 0,
        (result["base_rev_30d"] / chan_total).round(4),
        1.0 / result.groupby("channel")["base_rev_30d"].transform("count"),
    )

    return result


def apply_marketing_uplift(
    base_df:        pd.DataFrame,
    channel_budgets: dict[str, float],   # channel → total planned ₹ spend
    channel_roas:    dict[str, float],   # channel → ROAS
) -> pd.DataFrame:
    """
    Allocate the per-channel marketing budget across SKUs proportionally
    to their organic revenue share, then compute incremental uplift.

    KEY FIX vs original:
    ────────────────────
    The budget is entered at CHANNEL level (e.g. ₹1,00,000 for Swiggy).
    It must be split across SKUs in proportion to each SKU's share of
    that channel's organic revenue — NOT given in full to every SKU.

    Formula per SKU:
        sku_budget     = channel_budget × rev_share_in_channel
        uplift_rev     = sku_budget × ROAS
        uplift_qty     = uplift_rev / avg_price

    Attribution:
        Every rupee of uplift is tagged "marketing-driven" so the split
        between organic and marketing is always traceable.
    """
    if base_df.empty:
        return base_df

    df = base_df.copy()

    # Ensure rev_share_in_channel exists (fallback: equal split)
    if "rev_share_in_channel" not in df.columns:
        df["rev_share_in_channel"] = (
            1.0 / df.groupby("channel")["channel"].transform("count")
        )

    # Allocate budget proportionally across SKUs within each channel
    df["channel_total_budget"] = df["channel"].map(channel_budgets).fillna(0.0)
    df["planned_mkt_spend"]    = (df["channel_total_budget"] * df["rev_share_in_channel"]).round(2)
    df["assumed_roas"]         = df["channel"].map(channel_roas).fillna(_DEFAULT_ROAS)

    df["mkt_uplift_rev"] = (df["planned_mkt_spend"] * df["assumed_roas"]).round(2)
    df["mkt_uplift_qty"] = np.where(
        df["avg_price"] > 0,
        df["mkt_uplift_rev"] / df["avg_price"],
        0.0,
    ).round(2)

    df["total_qty_30d"] = (df["base_qty_30d"] + df["mkt_uplift_qty"]).round(2)
    df["total_rev_30d"] = (df["base_rev_30d"] + df["mkt_uplift_rev"]).round(2)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# MARKETING ROAS HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _estimate_roas_from_marketing_db(
    sb,
    channels: list[str],
) -> tuple[dict[str, float], dict[str, str]]:
    """
    Compute ROAS estimates with a strict priority hierarchy:
      1. Channel-level ROAS from marketing DB (ALL available history)
      2. Company-level ROAS from marketing DB (aggregate across all channels)
      3. Hardcoded _DEFAULT_ROAS  ← only if absolutely no data exists anywhere

    Returns:
        roas_map    — {channel: roas_value}
        roas_source — {channel: human-readable source label}
    """
    roas_map:    dict[str, float] = {}
    roas_source: dict[str, str]   = {}
    channel_roas: dict[str, float] = {}
    company_roas: float | None     = None

    try:
        from supabase import create_client
        mkt_url = st.secrets.get("MARKETING_SUPABASE_URL")
        mkt_key = st.secrets.get("MARKETING_SUPABASE_KEY")
        if not mkt_url or not mkt_key:
            raise ValueError("no marketing secrets")
        mkt_sb = create_client(mkt_url, mkt_key)

        # Fetch ALL campaign records — no date filter so partial history is used
        res = mkt_sb.table("campaigns").select(
            "channel, total_spend, attributed_revenue"
        ).execute()

        if res.data:
            mdf = pd.DataFrame(res.data)
            mdf["total_spend"]        = pd.to_numeric(mdf["total_spend"],        errors="coerce").fillna(0)
            mdf["attributed_revenue"] = pd.to_numeric(mdf["attributed_revenue"], errors="coerce").fillna(0)

            # Channel-level ROAS
            grp = mdf.groupby("channel")[["total_spend", "attributed_revenue"]].sum()
            for ch, row in grp.iterrows():
                if row["total_spend"] > 0:
                    channel_roas[ch] = round(row["attributed_revenue"] / row["total_spend"], 2)

            # Company-level ROAS (aggregate)
            tot_spend = mdf["total_spend"].sum()
            tot_rev   = mdf["attributed_revenue"].sum()
            if tot_spend > 0:
                company_roas = round(tot_rev / tot_spend, 2)
    except Exception:
        pass

    # Assign ROAS using priority hierarchy
    for ch in channels:
        if ch in channel_roas:
            roas_map[ch]    = channel_roas[ch]
            roas_source[ch] = f"Channel history ({channel_roas[ch]:.1f}×)"
        elif company_roas is not None:
            roas_map[ch]    = company_roas
            roas_source[ch] = f"Company average ({company_roas:.1f}×)"
        else:
            roas_map[ch]    = _DEFAULT_ROAS
            roas_source[ch] = f"Default — no marketing data ({_DEFAULT_ROAS:.1f}×)"

    return roas_map, roas_source


def _get_marketing_spend_by_date(sb) -> dict[str, float]:
    """
    Return {date_str: total_spend} for all dates that had marketing spend.
    Used to identify zero-spend (pure organic) days.
    Empty dict if marketing DB unavailable.
    """
    result: dict[str, float] = {}
    try:
        from supabase import create_client
        mkt_url = st.secrets.get("MARKETING_SUPABASE_URL")
        mkt_key = st.secrets.get("MARKETING_SUPABASE_KEY")
        if not mkt_url or not mkt_key:
            return result
        mkt_sb = create_client(mkt_url, mkt_key)
        res = mkt_sb.table("campaigns").select("date, total_spend").execute()
        if res.data:
            mdf = pd.DataFrame(res.data)
            mdf["total_spend"] = pd.to_numeric(mdf["total_spend"], errors="coerce").fillna(0)
            mdf = mdf[mdf["total_spend"] > 0]
            if "date" in mdf.columns:
                mdf["date"] = pd.to_datetime(mdf["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                result = mdf.groupby("date")["total_spend"].sum().to_dict()
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ACTUAL vs PREDICTED COMPARISON
# ─────────────────────────────────────────────────────────────────────────────

def build_actuals_vs_plan(
    history_df:  pd.DataFrame,
    plan_df:     pd.DataFrame,
    plan_month:  str,                    # 'YYYY-MM'
    channel_budgets: dict[str, float],   # planned budget per channel
) -> pd.DataFrame:
    """
    Compare plan vs actuals for the current plan month (or any past month).

    Variance decomposition:
        organic_var  = actual - base_qty_30d          ← demand shift
        mkt_var      = (actual mkt spend × ROAS) - mkt_uplift_qty  ← spend variance
    (These two should approximately sum to total variance.)
    """
    if history_df.empty or plan_df.empty:
        return pd.DataFrame()

    df = history_df.copy()
    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    year, month = int(plan_month[:4]), int(plan_month[5:7])
    month_mask = (df["date_dt"].dt.year == year) & (df["date_dt"].dt.month == month)
    actuals = (
        df[month_mask]
        .groupby(["channel", "item_name"])
        .agg(actual_qty=("qty_sold", "sum"), actual_rev=("revenue", "sum"))
        .reset_index()
    )

    merged = plan_df.merge(actuals, on=["channel", "item_name"], how="outer")
    for col in ["base_qty_30d", "mkt_uplift_qty", "total_qty_30d",
                "base_rev_30d", "mkt_uplift_rev", "total_rev_30d",
                "planned_mkt_spend", "assumed_roas",
                "actual_qty", "actual_rev"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0.0)

    # Days elapsed in plan month (for progress context)
    today = date.today()
    if today.year == year and today.month == month:
        days_elapsed = today.day
    else:
        import calendar
        days_elapsed = calendar.monthrange(year, month)[1]

    merged["days_elapsed"]    = days_elapsed
    merged["total_variance"]  = (merged["actual_qty"] - merged["total_qty_30d"]).round(2)
    merged["organic_var"]     = (merged["actual_qty"] - merged["base_qty_30d"]).round(2)
    merged["mkt_var"]         = (merged["mkt_uplift_qty"]).round(2)  # proxy: planned uplift
    merged["attainment_pct"]  = np.where(
        merged["total_qty_30d"] > 0,
        (merged["actual_qty"] / merged["total_qty_30d"] * 100).round(1),
        0.0,
    )

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _month_options(history_df: pd.DataFrame, future_months: int = 2) -> list[str]:
    """Return months from earliest history to N months ahead, newest first."""
    today = date.today()
    end   = date(today.year + (today.month + future_months - 1) // 12,
                 (today.month + future_months - 1) % 12 + 1, 1)
    if not history_df.empty and "date_dt" in history_df.columns:
        start = history_df["date_dt"].min().date().replace(day=1)
    else:
        start = date(today.year, today.month, 1)

    months = []
    cur = start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        # Advance one month
        m = cur.month + 1
        y = cur.year + (m - 1) // 12
        m = (m - 1) % 12 + 1
        cur = date(y, m, 1)

    return sorted(months, reverse=True)


def _fmt_num(v, prefix="₹", decimals=0) -> str:
    if v >= 1_00_000:
        return f"{prefix}{v/1_00_000:.1f}L"
    if v >= 1_000:
        return f"{prefix}{v/1_000:.1f}K"
    return f"{prefix}{v:,.{decimals}f}"


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def render_sop_tab(
    supabase_client,
    history_df:      pd.DataFrame,
    master_skus_df:  pd.DataFrame,
    master_chans_df: pd.DataFrame,
    role:            str,
) -> None:
    st.subheader("📋 S&OP — Sales & Operations Planning")
    st.caption(
        "30-day product-level demand forecast, marketing budget planning, "
        "and actual vs predicted tracking. As history grows, predictions improve automatically."
    )

    if history_df.empty:
        st.info("No sales data available yet. Upload data via Smart Upload first.")
        return

    # Ensure date_dt parsed
    df = history_df.copy()
    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date_dt"])

    channels     = sorted(df["channel"].unique().tolist())
    all_items    = sorted(df["item_name"].unique().tolist())
    month_opts   = _month_options(df, future_months=2)
    today        = date.today()
    current_month = today.strftime("%Y-%m")

    # ── Section selector ──────────────────────────────────────────────────────
    section = st.radio(
        "View:",
        ["📊 Forecast & Plan", "📈 Actual vs Predicted", "📚 Plan History"],
        horizontal=True,
        key="sop_section",
    )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — FORECAST & PLAN
    # ══════════════════════════════════════════════════════════════════════════
    if section == "📊 Forecast & Plan":

        # ── Plan month picker ─────────────────────────────────────────────────
        plan_month = st.selectbox(
            "Plan month", month_opts,
            index=month_opts.index(current_month) if current_month in month_opts else 0,
            key="sop_plan_month",
        )

        # ── Lookback window for forecast ──────────────────────────────────────
        lookback = st.select_slider(
            "Historical window for forecast",
            options=[30, 45, 60, 90, 120],
            value=60,
            key="sop_lookback",
            help="How many past days to use when computing the baseline DRR. "
                 "Longer windows are more stable; shorter ones react faster to trends.",
        )

        # ── Build base forecast ───────────────────────────────────────────────
        # Build marketing spend calendar (which dates had spend) for organic separation
        with st.spinner("Fetching marketing spend calendar…"):
            mkt_spend_by_date = _get_marketing_spend_by_date(supabase_client)

        with st.spinner("Building baseline forecast…"):
            try:
                base_df = build_base_forecast(
                    df, forecast_days=30, lookback_days=lookback,
                    mkt_spend_by_date=mkt_spend_by_date,
                )
            except Exception as _e:
                st.error(f"Forecast error: {_e}")
                base_df = pd.DataFrame()

        if base_df.empty:
            st.warning(f"Not enough history (need ≥ {_MIN_HISTORY_DAYS} days per SKU-channel). Upload more data.")
            return

        # ── Channel filters ───────────────────────────────────────────────────
        sel_channels = st.multiselect(
            "Channels to include", channels, default=channels, key="sop_channels"
        )
        base_df = base_df[base_df["channel"].isin(sel_channels)]

        if base_df.empty:
            st.warning("No forecast data for selected channels.")
            return

        # ── Marketing budget inputs ───────────────────────────────────────────
        st.markdown("#### 💰 Marketing Budget Plan (₹)")
        st.caption(
            "Enter planned marketing spend per channel for the forecast month. "
            "Leave 0 for organic-only forecast. "
            "The system uses historical ROAS to convert spend → incremental units."
        )

        # Try to load ROAS from marketing DB; fall back to defaults
        with st.spinner("Fetching historical ROAS…"):
            roas_map, roas_source = _estimate_roas_from_marketing_db(supabase_client, sel_channels)

        budget_cols = st.columns(min(len(sel_channels), 4))
        channel_budgets: dict[str, float] = {}
        channel_roas_overrides: dict[str, float] = {}

        for i, ch in enumerate(sel_channels):
            with budget_cols[i % len(budget_cols)]:
                st.markdown(f"**{ch}**")
                budget = st.number_input(
                    f"Spend (₹)", min_value=0.0, step=1000.0,
                    value=0.0, key=f"sop_budget_{ch}",
                )
                roas_default = roas_map.get(ch, _DEFAULT_ROAS)
                roas_label   = roas_source.get(ch, "Default")
                st.caption(f"📊 {roas_label}")
                roas_override = st.number_input(
                    f"ROAS", min_value=0.1, step=0.1,
                    value=float(roas_default),
                    key=f"sop_roas_{ch}",
                    help=f"Source: {roas_label}. Edit to model a different scenario.",
                )
                channel_budgets[ch]       = budget
                channel_roas_overrides[ch] = roas_override

        # ── Apply marketing uplift ────────────────────────────────────────────
        forecast_df = apply_marketing_uplift(base_df, channel_budgets, channel_roas_overrides)

        total_budget = sum(channel_budgets.values())
        has_marketing = total_budget > 0

        # ── Top-line metrics ──────────────────────────────────────────────────
        st.divider()
        st.markdown(f"#### 📊 30-Day Forecast — {plan_month}")

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric(
            "Organic Units",
            f"{forecast_df['base_qty_30d'].sum():,.0f}",
            help="Units expected from organic demand alone",
        )
        m2.metric(
            "Mkt-Driven Units",
            f"{forecast_df['mkt_uplift_qty'].sum():,.0f}",
            delta=f"+{forecast_df['mkt_uplift_qty'].sum():,.0f}" if has_marketing else None,
            help="Incremental units from planned marketing spend",
        )
        m3.metric(
            "Total Units",
            f"{forecast_df['total_qty_30d'].sum():,.0f}",
        )
        m4.metric(
            "Total Revenue",
            _fmt_num(forecast_df["total_rev_30d"].sum()),
        )
        m5.metric(
            "Marketing Budget",
            _fmt_num(total_budget),
            help="Total planned spend across all channels",
        )

        # ── Forecast table ────────────────────────────────────────────────────
        st.markdown("##### Forecast by SKU × Channel")

        display_cols = [
            "channel", "item_name",
            "base_qty_30d", "mkt_uplift_qty", "total_qty_30d",
            "base_rev_30d", "mkt_uplift_rev", "total_rev_30d",
            "rev_share_in_channel", "planned_mkt_spend",
            "drr_qty", "avg_price", "organic_share",
            "organic_days_used", "history_days_used",
        ]
        show_cols = [c for c in display_cols if c in forecast_df.columns]

        col_rename = {
            "channel": "Channel", "item_name": "SKU",
            "base_qty_30d": "Organic Units", "mkt_uplift_qty": "Mkt Units",
            "total_qty_30d": "Total Units",
            "base_rev_30d": "Organic Rev (₹)", "mkt_uplift_rev": "Mkt Rev (₹)",
            "total_rev_30d": "Total Rev (₹)",
            "rev_share_in_channel": "Rev Share in Ch",
            "planned_mkt_spend": "Allocated Budget (₹)",
            "drr_qty": "Organic DRR", "avg_price": "Avg Price (₹)",
            "organic_share": "Organic %",
            "organic_days_used": "Organic Days", "history_days_used": "Total Days",
        }

        disp = forecast_df[show_cols].rename(columns=col_rename).sort_values(
            ["Channel", "Total Units"], ascending=[True, False]
        )

        st.dataframe(
            disp.style.format({
                "Organic Units": "{:,.1f}", "Mkt Units": "{:,.1f}", "Total Units": "{:,.1f}",
                "Organic Rev (₹)": "₹{:,.0f}", "Mkt Rev (₹)": "₹{:,.0f}", "Total Rev (₹)": "₹{:,.0f}",
                "Rev Share in Ch": "{:.1%}", "Allocated Budget (₹)": "₹{:,.0f}",
                "Organic DRR": "{:.2f}", "Avg Price (₹)": "₹{:.1f}",
                "Organic %": "{:.1%}",
            }).bar(subset=["Total Units"], color="#4C8BF5"),
            use_container_width=True, hide_index=True,
        )

        # ── Forecast charts ───────────────────────────────────────────────────
        st.markdown("##### Organic vs Marketing Split")
        chart_grp = forecast_df.groupby("channel").agg(
            organic=("base_qty_30d", "sum"),
            marketing=("mkt_uplift_qty", "sum"),
        ).reset_index()
        chart_melt = chart_grp.melt(id_vars="channel", var_name="Type", value_name="Units")
        fig_split = px.bar(
            chart_melt, x="channel", y="Units", color="Type",
            barmode="stack", height=350,
            color_discrete_map={"organic": "#4C8BF5", "marketing": "#F4A261"},
            labels={"channel": "Channel", "Units": "Forecast Units (30d)"},
        )
        fig_split.update_layout(legend_title="", margin=dict(t=20))
        st.plotly_chart(fig_split, use_container_width=True)

        # SKU breakdown chart
        sku_grp = forecast_df.groupby("item_name").agg(
            total=("total_qty_30d", "sum"),
            organic=("base_qty_30d", "sum"),
        ).sort_values("total", ascending=False).head(15).reset_index()
        fig_sku = px.bar(
            sku_grp.sort_values("total"),
            x="total", y="item_name", orientation="h",
            height=max(300, len(sku_grp) * 35),
            color="total", color_continuous_scale="Blues",
            labels={"total": "Total Units (30d)", "item_name": ""},
            text=sku_grp.sort_values("total")["total"].apply(lambda v: f"{v:,.0f}"),
        )
        fig_sku.update_traces(textposition="outside")
        fig_sku.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=60, t=20))
        st.plotly_chart(fig_sku, use_container_width=True)

        # ── Save plan ─────────────────────────────────────────────────────────
        if role == "admin":
            st.divider()
            st.markdown("#### 💾 Save Plan")
            st.caption(
                "Saving locks in this forecast as the plan for the selected month. "
                "It will be used as the baseline for Actual vs Predicted tracking."
            )

            if st.button("💾 Save Plan to Cloud", key="sop_save_plan"):
                rows = []
                for _, r in forecast_df.iterrows():
                    rows.append({
                        "plan_month":       plan_month,
                        "channel":          r["channel"],
                        "item_name":        r["item_name"],
                        "base_qty_30d":     float(r["base_qty_30d"]),
                        "mkt_uplift_qty":   float(r.get("mkt_uplift_qty", 0)),
                        "total_qty_30d":    float(r.get("total_qty_30d", r["base_qty_30d"])),
                        "base_rev_30d":     float(r["base_rev_30d"]),
                        "mkt_uplift_rev":   float(r.get("mkt_uplift_rev", 0)),
                        "total_rev_30d":    float(r.get("total_rev_30d", r["base_rev_30d"])),
                        "planned_mkt_spend": float(channel_budgets.get(r["channel"], 0)),
                        "assumed_roas":      float(channel_roas_overrides.get(r["channel"], _DEFAULT_ROAS)),
                    })

                ok, err = _save_plan(supabase_client, rows)
                if ok:
                    st.success(f"✅ Plan for {plan_month} saved — {len(rows)} SKU-channel records.")
                else:
                    st.error(f"Save failed: {err}")
                    st.info("If the `sop_plans` table doesn't exist yet, run the SQL in the module docstring in Supabase.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — ACTUAL vs PREDICTED
    # ══════════════════════════════════════════════════════════════════════════
    elif section == "📈 Actual vs Predicted":

        plan_month_avp = st.selectbox(
            "Select plan month to review",
            month_opts, key="sop_avp_month",
            index=month_opts.index(current_month) if current_month in month_opts else 0,
        )

        with st.spinner("Loading saved plan…"):
            plan_df = _load_plan(supabase_client, plan_month_avp)

        if plan_df.empty:
            st.info(
                f"No saved plan found for {plan_month_avp}. "
                "Go to **Forecast & Plan**, build a forecast and save it first."
            )
            return

        year, month = int(plan_month_avp[:4]), int(plan_month_avp[5:7])
        import calendar as _cal
        days_in_month = _cal.monthrange(year, month)[1]
        is_current = (today.year == year and today.month == month)
        days_elapsed = today.day if is_current else days_in_month
        pct_elapsed  = days_elapsed / days_in_month * 100

        st.info(
            f"📅 {_cal.month_name[month]} {year} — "
            f"**{days_elapsed} of {days_in_month} days elapsed** ({pct_elapsed:.0f}%)"
            + (" ← month in progress" if is_current else " ← month complete")
        )

        # Load budgets from plan
        channel_budgets_avp = (
            plan_df.groupby("channel")["planned_mkt_spend"].first().to_dict()
        )
        avp_df = build_actuals_vs_plan(df, plan_df, plan_month_avp, channel_budgets_avp)

        if avp_df.empty:
            st.warning("Could not build comparison. Check data.")
            return

        # Pro-rate plan to days elapsed for fair comparison
        avp_df["prorated_plan"] = (avp_df["total_qty_30d"] * days_elapsed / days_in_month).round(1)
        avp_df["vs_prorated"]   = (avp_df["actual_qty"] - avp_df["prorated_plan"]).round(1)

        # ── Top metrics ───────────────────────────────────────────────────────
        tot_plan   = avp_df["prorated_plan"].sum()
        tot_actual = avp_df["actual_qty"].sum()
        tot_var    = tot_actual - tot_plan
        attain_pct = (tot_actual / tot_plan * 100) if tot_plan > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Pro-rated Plan", f"{tot_plan:,.0f} units",
                  help=f"Full-month plan × ({days_elapsed}/{days_in_month} days)")
        m2.metric("Actual to Date", f"{tot_actual:,.0f} units")
        m3.metric("Variance", f"{tot_var:+,.0f} units",
                  delta=f"{tot_var:+,.0f}", delta_color="normal")
        m4.metric("Attainment", f"{attain_pct:.1f}%",
                  delta=f"{attain_pct-100:+.1f}pp vs plan")

        st.divider()

        # ── Waterfall chart — variance decomposition ──────────────────────────
        st.markdown("#### 🔍 Variance Decomposition")
        st.caption(
            "Green = marketing-driven uplift was planned. "
            "Blue = organic demand variance. "
            "Total bar = actual vs full-month plan."
        )

        ch_avp = avp_df.groupby("channel").agg(
            prorated_plan=("prorated_plan", "sum"),
            actual_qty=("actual_qty", "sum"),
            mkt_uplift_qty=("mkt_uplift_qty", "sum"),
        ).reset_index()
        ch_avp["organic_base"]  = ch_avp["prorated_plan"] - ch_avp["mkt_uplift_qty"] * days_elapsed / days_in_month
        ch_avp["organic_var"]   = ch_avp["actual_qty"] - ch_avp["organic_base"]
        ch_avp["mkt_var"]       = ch_avp["mkt_uplift_qty"] * days_elapsed / days_in_month

        fig_waterfall = go.Figure()
        fig_waterfall.add_bar(
            name="Organic Base",
            x=ch_avp["channel"], y=ch_avp["organic_base"].round(0),
            marker_color="#4C8BF5",
        )
        fig_waterfall.add_bar(
            name="Mkt Uplift (planned)",
            x=ch_avp["channel"], y=ch_avp["mkt_var"].round(0),
            marker_color="#F4A261",
        )
        fig_waterfall.add_bar(
            name="Organic Variance",
            x=ch_avp["channel"], y=ch_avp["organic_var"].round(0),
            marker_color=["#2E9E4F" if v >= 0 else "#E63946" for v in ch_avp["organic_var"]],
        )
        fig_waterfall.update_layout(
            barmode="stack", height=380,
            xaxis_title="Channel", yaxis_title="Units",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=40),
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)

        # ── Attainment table ──────────────────────────────────────────────────
        st.markdown("#### 📋 SKU × Channel Attainment")

        avp_display = avp_df[[
            "channel", "item_name",
            "prorated_plan", "actual_qty", "vs_prorated", "attainment_pct",
            "base_qty_30d", "mkt_uplift_qty", "total_qty_30d",
        ]].copy().rename(columns={
            "channel": "Channel", "item_name": "SKU",
            "prorated_plan": "Plan (pro-rated)", "actual_qty": "Actual",
            "vs_prorated": "Variance", "attainment_pct": "Attainment %",
            "base_qty_30d": "Full Organic Plan", "mkt_uplift_qty": "Mkt Uplift Plan",
            "total_qty_30d": "Full Month Plan",
        }).sort_values(["Channel", "Actual"], ascending=[True, False])

        def _color_var(v):
            if isinstance(v, (int, float)):
                return "color: green" if v >= 0 else "color: red"
            return ""

        st.dataframe(
            avp_display.style.format({
                "Plan (pro-rated)": "{:,.1f}", "Actual": "{:,.1f}",
                "Variance": "{:+,.1f}", "Attainment %": "{:.1f}%",
                "Full Organic Plan": "{:,.1f}", "Mkt Uplift Plan": "{:,.1f}",
                "Full Month Plan": "{:,.1f}",
            }).map(_color_var, subset=["Variance"]),
            use_container_width=True, hide_index=True,
        )

        # ── Trend chart — daily actuals vs implied daily plan ─────────────────
        st.markdown("#### 📅 Daily Actuals vs Plan Trajectory")

        month_mask = (df["date_dt"].dt.year == year) & (df["date_dt"].dt.month == month)
        daily_actuals = (
            df[month_mask]
            .groupby("date_dt")[["qty_sold", "revenue"]]
            .sum()
            .reset_index()
            .sort_values("date_dt")
        )

        if not daily_actuals.empty:
            daily_plan_units = avp_df["total_qty_30d"].sum() / days_in_month

            fig_trend = go.Figure()
            fig_trend.add_bar(
                x=daily_actuals["date_dt"], y=daily_actuals["qty_sold"],
                name="Actual Units", marker_color="#4C8BF5", opacity=0.7,
            )
            fig_trend.add_hline(
                y=daily_plan_units, line_dash="dash", line_color="#E63946",
                annotation_text=f"Daily Plan ({daily_plan_units:.1f} units)",
                annotation_position="top right",
            )
            # Cumulative actual vs plan line
            daily_actuals["cum_actual"] = daily_actuals["qty_sold"].cumsum()
            cum_plan = [(i + 1) * daily_plan_units for i in range(len(daily_actuals))]
            fig_trend.add_scatter(
                x=daily_actuals["date_dt"], y=daily_actuals["cum_actual"],
                name="Cumulative Actual", mode="lines+markers",
                line=dict(color="#2E9E4F", width=2), yaxis="y2",
            )
            fig_trend.add_scatter(
                x=daily_actuals["date_dt"], y=cum_plan,
                name="Cumulative Plan", mode="lines",
                line=dict(color="#E63946", width=2, dash="dot"), yaxis="y2",
            )
            fig_trend.update_layout(
                height=420, barmode="overlay",
                yaxis=dict(title="Daily Units"),
                yaxis2=dict(title="Cumulative Units", overlaying="y", side="right"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                hovermode="x unified",
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.info(f"No sales data recorded yet for {_cal.month_name[month]} {year}.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — PLAN HISTORY
    # ══════════════════════════════════════════════════════════════════════════
    else:
        st.markdown("#### 📚 All Saved S&OP Plans")

        with st.spinner("Loading plan history…"):
            all_plans = _load_all_plans(supabase_client)

        if all_plans.empty:
            st.info("No plans saved yet. Go to Forecast & Plan and save a plan first.")
            return

        # Summary by month
        summary = (
            all_plans.groupby("plan_month").agg(
                sku_channels=("item_name", "count"),
                total_organic=("base_qty_30d", "sum"),
                total_mkt_uplift=("mkt_uplift_qty", "sum"),
                total_plan=("total_qty_30d", "sum"),
                total_rev=("total_rev_30d", "sum"),
                total_budget=("planned_mkt_spend", "sum"),
            ).reset_index().sort_values("plan_month", ascending=False)
        )

        st.dataframe(
            summary.rename(columns={
                "plan_month": "Month", "sku_channels": "SKU-Channels",
                "total_organic": "Organic Units", "total_mkt_uplift": "Mkt Units",
                "total_plan": "Total Units", "total_rev": "Total Rev (₹)",
                "total_budget": "Mkt Budget (₹)",
            }).style.format({
                "Organic Units": "{:,.0f}", "Mkt Units": "{:,.0f}",
                "Total Units": "{:,.0f}", "Total Rev (₹)": "₹{:,.0f}",
                "Mkt Budget (₹)": "₹{:,.0f}",
            }),
            use_container_width=True, hide_index=True,
        )

        # Trend chart across saved plans
        if len(summary) >= 2:
            fig_hist = px.bar(
                summary.sort_values("plan_month"),
                x="plan_month", y=["total_organic", "total_mkt_uplift"],
                barmode="stack", height=350,
                color_discrete_map={
                    "total_organic":     "#4C8BF5",
                    "total_mkt_uplift":  "#F4A261",
                },
                labels={"plan_month": "Month", "value": "Units", "variable": "Type"},
            )
            fig_hist.update_layout(
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(t=40),
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        # Drill into a specific plan month
        st.markdown("#### 🔎 Drill into a Plan Month")
        drill_month = st.selectbox(
            "Select month", all_plans["plan_month"].unique().tolist(),
            key="sop_drill_month",
        )
        drill_df = all_plans[all_plans["plan_month"] == drill_month].copy()
        st.dataframe(
            drill_df[[
                "channel", "item_name", "base_qty_30d", "mkt_uplift_qty",
                "total_qty_30d", "total_rev_30d", "planned_mkt_spend", "assumed_roas",
            ]].rename(columns={
                "channel": "Channel", "item_name": "SKU",
                "base_qty_30d": "Organic", "mkt_uplift_qty": "Mkt Uplift",
                "total_qty_30d": "Total Units", "total_rev_30d": "Total Rev (₹)",
                "planned_mkt_spend": "Budget (₹)", "assumed_roas": "ROAS",
            }).style.format({
                "Organic": "{:,.1f}", "Mkt Uplift": "{:,.1f}",
                "Total Units": "{:,.1f}", "Total Rev (₹)": "₹{:,.0f}",
                "Budget (₹)": "₹{:,.0f}", "ROAS": "{:.1f}",
            }),
            use_container_width=True, hide_index=True,
        )

        if role == "admin":
            if st.button("🗑️ Delete this plan", key="sop_delete_plan"):
                try:
                    supabase_client.table(_PLAN_TABLE).delete().eq("plan_month", drill_month).execute()
                    st.success(f"Deleted plan for {drill_month}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")
