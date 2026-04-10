"""
sop_module.py  —  S&OP Tab for Mamanourish Executive Tracker
─────────────────────────────────────────────────────────────
Entry point: render_sop_tab(supabase_client, history_df,
                            master_skus_df, master_chans_df, role)

Supabase table required (run once):
────────────────────────────────────
CREATE TABLE IF NOT EXISTS sop_plans (
    id SERIAL PRIMARY KEY,
    plan_month TEXT NOT NULL,
    channel TEXT NOT NULL,
    item_name TEXT NOT NULL,
    base_qty_30d NUMERIC, mkt_uplift_qty NUMERIC, total_qty_30d NUMERIC,
    base_rev_30d NUMERIC, mkt_uplift_rev NUMERIC, total_rev_30d NUMERIC,
    planned_mkt_spend NUMERIC DEFAULT 0,
    assumed_roas NUMERIC DEFAULT 0,
    organic_assumption TEXT,
    growth_factor NUMERIC DEFAULT 1,
    growth_factor_model NUMERIC DEFAULT 1,
    growth_overridden BOOLEAN DEFAULT FALSE,
    roas_overridden BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(plan_month, channel, item_name)
);

-- If the table already exists, add the new columns with:
-- ALTER TABLE sop_plans ADD COLUMN IF NOT EXISTS growth_factor NUMERIC DEFAULT 1;
-- ALTER TABLE sop_plans ADD COLUMN IF NOT EXISTS growth_factor_model NUMERIC DEFAULT 1;
-- ALTER TABLE sop_plans ADD COLUMN IF NOT EXISTS growth_overridden BOOLEAN DEFAULT FALSE;
-- ALTER TABLE sop_plans ADD COLUMN IF NOT EXISTS roas_overridden BOOLEAN DEFAULT FALSE;
"""
from __future__ import annotations
import calendar as _cal
import math
from datetime import date, timedelta

import numpy as np

# ── Global Filter Integration ─────────────────────────────────────────────────
# The SOP module reads channel selection from global filter state so the
# "Channels" multiselect is pre-seeded with the globally selected channels.
# The plan_month selector remains LOCAL — it is a planning-horizon control,
# not a historical data filter, so it should not be globalized.
try:
    from global_filters import get_global_filters as _get_global_filters
    _GLOBAL_FILTERS_AVAILABLE = True
except ImportError:
    _GLOBAL_FILTERS_AVAILABLE = False
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Matplotlib-free colour gradient for st.dataframe Styler
# ─────────────────────────────────────────────────────────────────────────────

def _css_gradient(df_sub: pd.DataFrame, low_col: str, mid_col: str | None,
                  high_col: str) -> pd.DataFrame:
    """
    Return a same-shape DataFrame of CSS background-color strings.
    Works on a numeric subset.  No matplotlib required.

    Colour stops:
      Blues  : white → #1565C0
      RdYlGn : #d73027 → #ffffbf → #1a9850
    """
    arr = df_sub.to_numpy(dtype=float, na_value=0.0)
    vmin, vmax = arr.min(), arr.max()
    span = vmax - vmin if vmax != vmin else 1.0

    def _lerp(a: tuple, b: tuple, t: float) -> str:
        r = int(a[0] + (b[0] - a[0]) * t)
        g = int(a[1] + (b[1] - a[1]) * t)
        b_ = int(a[2] + (b[2] - a[2]) * t)
        return f"background-color: rgb({r},{g},{b_})"

    low_rgb  = tuple(int(low_col.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
    high_rgb = tuple(int(high_col.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
    mid_rgb  = (tuple(int(mid_col.lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
                if mid_col else None)

    out = []
    for row in arr:
        row_css = []
        for v in row:
            t = (v - vmin) / span           # 0 → 1
            if mid_rgb is None:
                css = _lerp(low_rgb, high_rgb, t)
            else:
                if t < 0.5:
                    css = _lerp(low_rgb, mid_rgb, t * 2)
                else:
                    css = _lerp(mid_rgb, high_rgb, (t - 0.5) * 2)
            row_css.append(css)
        out.append(row_css)
    return pd.DataFrame(out, index=df_sub.index, columns=df_sub.columns)


def _apply_gradient(styler: "pd.io.formats.style.Styler",
                    subset,
                    cmap: str = "Blues") -> "pd.io.formats.style.Styler":
    """
    Drop-in replacement for .background_gradient(cmap=..., axis=None, subset=...).
    Supported cmaps: 'Blues', 'RdYlGn'.
    """
    if cmap == "RdYlGn":
        return styler.apply(
            _css_gradient,
            low_col="d73027", mid_col="ffffbf", high_col="1a9850",
            subset=subset, axis=None,
        )
    # Default → Blues (white to deep blue)
    return styler.apply(
        _css_gradient,
        low_col="ffffff", mid_col=None, high_col="1565C0",
        subset=subset, axis=None,
    )


# ─────────────────────────────────────────────────────────────────────────────
_DEFAULT_ROAS         = 3.0
_DECAY_HALFLIFE_WEEKS = 4
_MIN_HISTORY_DAYS     = 7
_PLAN_TABLE           = "sop_plans"


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _paginate(sb, table: str, select: str) -> list[dict]:
    rows, page, SZ = [], 0, 1000
    while True:
        res = sb.table(table).select(select).range(page * SZ, (page+1)*SZ - 1).execute()
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < SZ:
            break
        page += 1
    return rows


def _load_plan(sb, plan_month: str) -> pd.DataFrame:
    try:
        res = sb.table(_PLAN_TABLE).select("*").eq("plan_month", plan_month).execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _save_plan(sb, rows: list[dict]) -> tuple[bool, str]:
    try:
        sb.table(_PLAN_TABLE).upsert(rows, on_conflict="plan_month,channel,item_name").execute()
        return True, ""
    except Exception as e:
        return False, str(e)


def _load_all_plans(sb) -> pd.DataFrame:
    try:
        rows = _paginate(sb, _PLAN_TABLE, "*")
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# MARKETING DB — fetch ROAS and spend calendar
# ─────────────────────────────────────────────────────────────────────────────

def _get_mkt_supabase():
    """Return marketing supabase client or None."""
    try:
        from supabase import create_client
        url = st.secrets.get("MARKETING_SUPABASE_URL")
        key = st.secrets.get("MARKETING_SUPABASE_KEY")
        if url and key:
            return create_client(url, key)
    except Exception:
        pass
    return None


def _fetch_performance_df(mkt_sb) -> pd.DataFrame:
    """
    Fetch ALL rows from marketing `performance` table.
    Columns used: date, channel, product, spend, sales
    """
    try:
        rows = _paginate(mkt_sb, "performance", "date, channel, product, spend, sales")
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
        df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)
        df["date"]  = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        return df
    except Exception:
        return pd.DataFrame()


def _fetch_channel_map(mkt_sb) -> dict[str, str]:
    """Returns {sales_channel: mkt_channel} from channel_map table."""
    result = {}
    try:
        res = mkt_sb.table("channel_map").select("mkt_channel, sales_channel").execute()
        if res.data:
            for row in res.data:
                result[str(row["sales_channel"])] = str(row["mkt_channel"])
    except Exception:
        pass
    return result


def _fetch_product_map(mkt_sb) -> dict[str, str]:
    """Returns {sales_item: mkt_product} from product_map table."""
    result = {}
    try:
        res = mkt_sb.table("product_map").select("mkt_product, sales_item").execute()
        if res.data:
            for row in res.data:
                result[str(row["sales_item"])] = str(row["mkt_product"])
    except Exception:
        pass
    return result


def get_marketing_data(sales_channels: list[str], sales_items: list[str]) -> dict:
    """
    Single entry point that fetches everything from the marketing DB.

    Returns dict with keys:
        perf_df          — full performance DataFrame (date, channel, product, spend, sales)
        channel_roas     — {sales_channel: roas}
        product_roas     — {(sales_channel, sales_item): roas}
        company_roas     — float (aggregate ROAS across all channels/products)
        roas_source      — {sales_channel: human-readable source string}
        spend_by_date    — {date_str: total_spend}  (for organic day detection)
    """
    empty = dict(
        perf_df=pd.DataFrame(),
        channel_roas={},
        product_roas={},
        company_roas=None,
        roas_source={ch: f"No marketing data — default ({_DEFAULT_ROAS:.1f}×)"
                     for ch in sales_channels},
        spend_by_date={},
    )

    mkt_sb = _get_mkt_supabase()
    if mkt_sb is None:
        return empty

    perf_df = _fetch_performance_df(mkt_sb)
    if perf_df.empty:
        return empty

    ch_map   = _fetch_channel_map(mkt_sb)    # sales_ch → mkt_ch
    prod_map = _fetch_product_map(mkt_sb)    # sales_item → mkt_product

    # ── Spend calendar ────────────────────────────────────────────────────────
    spend_by_date = (
        perf_df[perf_df["spend"] > 0]
        .groupby("date")["spend"].sum()
        .to_dict()
    )

    # ── Company-level ROAS ────────────────────────────────────────────────────
    tot_spend = perf_df["spend"].sum()
    tot_sales = perf_df["sales"].sum()
    company_roas = round(tot_sales / tot_spend, 2) if tot_spend > 0 else None

    # ── Channel-level ROAS (aggregate across all products) ───────────────────
    channel_roas: dict[str, float] = {}
    ch_grp = perf_df.groupby("channel")[["spend", "sales"]].sum()
    for mkt_ch, row in ch_grp.iterrows():
        if row["spend"] > 0:
            channel_roas[str(mkt_ch)] = round(row["sales"] / row["spend"], 2)

    # ── Product-level ROAS per channel ────────────────────────────────────────
    # Key: (sales_channel, sales_item) → ROAS
    product_roas: dict[tuple, float] = {}
    if "product" in perf_df.columns:
        prod_grp = perf_df.groupby(["channel", "product"])[["spend", "sales"]].sum()
        for (mkt_ch, mkt_prod), row in prod_grp.iterrows():
            if row["spend"] > 0:
                product_roas[(str(mkt_ch), str(mkt_prod))] = round(
                    row["sales"] / row["spend"], 2
                )

    # ── Map to sales names ────────────────────────────────────────────────────
    # Build channel_roas and product_roas keyed by SALES channel/item names
    ch_roas_by_sales: dict[str, float] = {}
    for sc in sales_channels:
        mc = ch_map.get(sc, sc)          # translate to mkt channel name
        if mc in channel_roas:
            ch_roas_by_sales[sc] = channel_roas[mc]
        elif sc in channel_roas:
            ch_roas_by_sales[sc] = channel_roas[sc]

    prod_roas_by_sales: dict[tuple, float] = {}
    for sc in sales_channels:
        mc = ch_map.get(sc, sc)
        for si in sales_items:
            mp = prod_map.get(si, si)    # translate to mkt product name
            key_mkt   = (mc, mp)
            key_sales = (sc, si)
            if key_mkt in product_roas:
                prod_roas_by_sales[key_sales] = product_roas[key_mkt]
            elif (sc, si) in product_roas:
                prod_roas_by_sales[key_sales] = product_roas[(sc, si)]
            elif (mc, si) in product_roas:
                prod_roas_by_sales[key_sales] = product_roas[(mc, si)]

    # ── Build roas_source strings ─────────────────────────────────────────────
    roas_source = {}
    for sc in sales_channels:
        if sc in ch_roas_by_sales:
            roas_source[sc] = f"Channel history ({ch_roas_by_sales[sc]:.1f}×)"
        elif company_roas is not None:
            roas_source[sc] = f"Company average ({company_roas:.1f}×)"
        else:
            roas_source[sc] = f"No marketing data — default ({_DEFAULT_ROAS:.1f}×)"

    return dict(
        perf_df=perf_df,
        channel_roas=ch_roas_by_sales,
        product_roas=prod_roas_by_sales,
        company_roas=company_roas,
        roas_source=roas_source,
        spend_by_date=spend_by_date,
    )


# ─────────────────────────────────────────────────────────────────────────────
# FORECASTING ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _weighted_drr(series: pd.Series) -> float:
    """Exponential decay weighted average — recent days weigh more."""
    if series.empty:
        return 0.0
    n   = len(series)
    lam = math.log(2) / (_DECAY_HALFLIFE_WEEKS * 7)
    w   = np.array([math.exp(-lam * (n - 1 - i)) for i in range(n)])
    w  /= w.sum()
    return float(np.dot(w, series.fillna(0).values))


def build_base_forecast(
    history_df: pd.DataFrame,
    forecast_days: int = 30,
    spend_by_date: dict | None = None,
    growth_overrides: dict | None = None,
) -> pd.DataFrame:
    """
    Organic baseline forecast per (channel, item_name).

    KEY DESIGN DECISIONS (all shown transparently to user):

    1. NO LOOKBACK WINDOW — uses every available day of history so
       even sparse data contributes.  More history = more accurate.

    2. ORGANIC SEPARATION:
       The performance table tells us which dates had marketing spend.
       Days with zero spend are "clean" organic observations.
       Strategy A: ≥7 clean days → use clean-day DRR directly.
       Strategy B: 1-6 clean days → scale total DRR by organic_share %.
       Strategy C: 0 clean days (always running ads) → use total DRR
                   (conservative; labelled clearly).

    3. ORGANIC ASSUMPTION TEXT — every row gets a plain-English string
       explaining exactly how its forecast was derived.

    4. GROWTH ADJUSTMENT — compares last 30d DRR vs full-history DRR.
       If last 30d > history avg, applies a weighted growth factor.
       This makes the number evidence-based, not arbitrary.

    Returns columns:
        channel, item_name,
        base_qty_30d, base_rev_30d,        ← organic forecast
        hist_monthly_avg_qty,              ← simple monthly average (reference)
        growth_factor,                     ← applied growth multiplier
        organic_share, organic_days_used,
        drr_qty, avg_price,
        history_days_used, organic_assumption,
        rev_share_in_channel               ← for budget allocation
    """
    if history_df.empty:
        return pd.DataFrame()

    df = history_df.copy()
    for col in ["channel", "item_name"]:
        if col in df.columns:
            df[col] = df[col].astype(object).fillna("").astype(str).str.strip()
    df = df[(df["channel"] != "") & (df["item_name"] != "")]

    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    if df.empty:
        return pd.DataFrame()

    # Zero-spend (organic) dates from marketing DB
    zero_spend_dates: set[str] = set()
    if spend_by_date:
        all_dates = set(df["date_dt"].dt.strftime("%Y-%m-%d").unique())
        zero_spend_dates = all_dates - set(spend_by_date.keys())
    else:
        zero_spend_dates = set(df["date_dt"].dt.strftime("%Y-%m-%d").unique())

    rows = []
    for (channel, item), grp in df.groupby(["channel", "item_name"]):
        date_range = pd.date_range(grp["date_dt"].min(), grp["date_dt"].max(), freq="D")
        daily_qty  = grp.groupby("date_dt")["qty_sold"].sum().reindex(date_range, fill_value=0)
        daily_rev  = grp.groupby("date_dt")["revenue"].sum().reindex(date_range, fill_value=0)

        history_days = len(date_range)
        if history_days < _MIN_HISTORY_DAYS:
            continue

        # ── Full-period weighted DRR ──────────────────────────────────────────
        drr_qty_total = _weighted_drr(daily_qty)
        drr_rev_total = _weighted_drr(daily_rev)
        avg_price     = round(drr_rev_total / drr_qty_total, 2) if drr_qty_total > 0 else 0.0

        # ── Historical monthly average (simple reference, not used in forecast) ─
        hist_monthly_avg = daily_qty.sum() / max(history_days / 30, 1)

        # ── Recent trend vs history for growth factor ─────────────────────────
        last30_qty  = daily_qty.iloc[-30:].mean() if len(daily_qty) >= 30 else daily_qty.mean()
        older_qty   = daily_qty.mean()
        growth_factor_model = round(last30_qty / older_qty, 3) if older_qty > 0 else 1.0
        growth_factor_model = min(max(growth_factor_model, 0.5), 2.0)   # cap at 50%–200%

        # Apply user override if provided (still cap at 0.1×–5.0× for sanity)
        _override_key = (channel, item)
        if growth_overrides and _override_key in growth_overrides:
            growth_factor = min(max(float(growth_overrides[_override_key]), 0.1), 5.0)
            growth_overridden = True
        else:
            growth_factor = growth_factor_model
            growth_overridden = False

        # ── Organic DRR ────────────────────────────────────────────────────────
        zero_mask         = pd.Series(
            [d.strftime("%Y-%m-%d") in zero_spend_dates for d in date_range],
            index=date_range,
        )
        organic_qty_days  = daily_qty[zero_mask]
        organic_rev_days  = daily_rev[zero_mask]
        n_organic_days    = int(zero_mask.sum())

        if n_organic_days >= _MIN_HISTORY_DAYS:
            # Strategy A — clean days only
            drr_qty_org    = _weighted_drr(organic_qty_days)
            drr_rev_org    = _weighted_drr(organic_rev_days)
            organic_share  = (
                organic_rev_days.sum() / daily_rev.sum()
                if daily_rev.sum() > 0 else 1.0
            )
            strategy = "A"
        elif n_organic_days > 0:
            # Strategy B — scale by organic share
            organic_share  = (
                organic_rev_days.sum() / daily_rev.sum()
                if daily_rev.sum() > 0 else 1.0
            )
            drr_qty_org    = drr_qty_total * organic_share
            drr_rev_org    = drr_rev_total * organic_share
            strategy = "B"
        else:
            # Strategy C — all days are ad days, use total as proxy
            organic_share  = 1.0
            drr_qty_org    = drr_qty_total
            drr_rev_org    = drr_rev_total
            strategy = "C"

        # ── Apply growth factor to organic DRR ────────────────────────────────
        drr_qty_final = drr_qty_org * growth_factor
        drr_rev_final = drr_rev_org * growth_factor

        base_qty = round(drr_qty_final * forecast_days, 1)
        base_rev = round(drr_rev_final * forecast_days, 1)

        # ── Plain-English assumption text ─────────────────────────────────────
        hist_avg_monthly = round(hist_monthly_avg, 1)
        if strategy == "A":
            assumption = (
                f"Using {n_organic_days} zero-ad days out of {history_days} total. "
                f"Organic DRR = {drr_qty_org:.2f} units/day. "
                f"Recent trend vs history: {growth_factor:.2f}× "
                f"({'↑ growth' if growth_factor > 1.02 else '↓ decline' if growth_factor < 0.98 else '≈ flat'}). "
                f"Forecast = {drr_qty_org:.2f} × {growth_factor:.2f} × {forecast_days}d = {base_qty:.0f} units. "
                f"Hist monthly avg: {hist_avg_monthly:.0f} units."
            )
        elif strategy == "B":
            assumption = (
                f"Only {n_organic_days} zero-ad day(s) available — not enough for direct DRR. "
                f"Using total DRR ({drr_qty_total:.2f}/day) × organic share ({organic_share:.1%}) = "
                f"{drr_qty_org:.2f}/day organic. "
                f"Growth factor {growth_factor:.2f}×. "
                f"Forecast = {base_qty:.0f} units. "
                f"Hist monthly avg: {hist_avg_monthly:.0f} units."
            )
        else:
            assumption = (
                f"No zero-ad days found in {history_days} days of history — "
                f"marketing ran every day. Using total DRR ({drr_qty_total:.2f}/day) as organic proxy. "
                f"Growth factor {growth_factor:.2f}×. "
                f"Forecast = {base_qty:.0f} units. "
                f"Hist monthly avg: {hist_avg_monthly:.0f} units. "
                f"⚠️ Upload zero-spend days to improve organic separation."
            )

        rows.append({
            "channel":              channel,
            "item_name":            item,
            "base_qty_30d":         base_qty,
            "base_rev_30d":         base_rev,
            "hist_monthly_avg_qty": round(hist_monthly_avg, 1),
            "growth_factor":        growth_factor,
            "growth_factor_model":  growth_factor_model,
            "growth_overridden":    growth_overridden,
            "organic_share":        round(organic_share, 3),
            "organic_days_used":    n_organic_days,
            "drr_qty":              round(drr_qty_org, 3),
            "avg_price":            avg_price,
            "history_days_used":    history_days,
            "organic_assumption":   assumption,
        })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)

    # Revenue share within channel — used to allocate channel budget across SKUs
    chan_total = result.groupby("channel")["base_rev_30d"].transform("sum")
    result["rev_share_in_channel"] = np.where(
        chan_total > 0,
        (result["base_rev_30d"] / chan_total).round(4),
        (1.0 / result.groupby("channel")["base_rev_30d"].transform("count")),
    )
    return result


def apply_marketing_uplift(
    base_df:           pd.DataFrame,
    channel_budgets:   dict[str, float],
    channel_roas:      dict[str, float],
    product_roas:      dict[tuple, float],
    company_roas:      float | None,
    sku_roas_overrides: dict | None = None,
) -> pd.DataFrame:
    """
    Allocate per-channel budget across SKUs using PRODUCT-LEVEL ROAS from
    the marketing DB, then compute uplift.

    Budget allocation:
        Each SKU within a channel gets a share of the channel budget
        proportional to:
          • Product-level ROAS (if available) × organic revenue share
        This means higher-ROAS products get more budget — which reflects
        actual marketing performance.

    sku_roas_overrides: {(channel, item_name): float}
        User-edited ROAS values that take precedence over all other sources.

    Uplift:
        sku_budget  = channel_budget × sku_weight / Σ(sku_weights)
        uplift_rev  = sku_budget × product_roas (or channel/company fallback)
        uplift_qty  = uplift_rev / avg_price
    """
    if base_df.empty:
        return base_df

    df = base_df.copy()

    # ── Compute allocation weights per SKU ────────────────────────────────────
    def sku_roas(ch, item):
        """Best available ROAS for this SKU (user override takes top priority)."""
        if sku_roas_overrides and (ch, item) in sku_roas_overrides:
            return sku_roas_overrides[(ch, item)]
        if (ch, item) in product_roas:
            return product_roas[(ch, item)]
        if ch in channel_roas:
            return channel_roas[ch]
        if company_roas is not None:
            return company_roas
        return _DEFAULT_ROAS

    df["sku_roas"] = df.apply(lambda r: sku_roas(r["channel"], r["item_name"]), axis=1)
    df["roas_overridden"] = df.apply(
        lambda r: bool(sku_roas_overrides and (r["channel"], r["item_name"]) in sku_roas_overrides),
        axis=1,
    )

    # Weight = organic_rev_share × sku_roas  (higher-performing SKUs get more budget)
    df["alloc_weight"] = df["rev_share_in_channel"] * df["sku_roas"]

    # Normalise within channel so weights sum to 1
    ch_weight_sum = df.groupby("channel")["alloc_weight"].transform("sum")
    df["budget_share"] = np.where(
        ch_weight_sum > 0,
        df["alloc_weight"] / ch_weight_sum,
        df["rev_share_in_channel"],   # fallback: equal split by revenue
    )

    df["channel_total_budget"] = df["channel"].map(channel_budgets).fillna(0.0)
    df["planned_mkt_spend"]    = (df["channel_total_budget"] * df["budget_share"]).round(2)
    df["assumed_roas"]         = df["sku_roas"]

    df["mkt_uplift_rev"] = (df["planned_mkt_spend"] * df["assumed_roas"]).round(2)
    df["mkt_uplift_qty"] = np.where(
        df["avg_price"] > 0,
        (df["mkt_uplift_rev"] / df["avg_price"]).round(1),
        0.0,
    )

    df["total_qty_30d"] = (df["base_qty_30d"] + df["mkt_uplift_qty"]).round(1)
    df["total_rev_30d"] = (df["base_rev_30d"] + df["mkt_uplift_rev"]).round(1)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# ACTUAL vs PREDICTED
# ─────────────────────────────────────────────────────────────────────────────

def build_actuals_vs_plan(
    history_df: pd.DataFrame,
    plan_df:    pd.DataFrame,
    plan_month: str,
) -> pd.DataFrame:
    if history_df.empty or plan_df.empty:
        return pd.DataFrame()

    df = history_df.copy()
    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    yr, mo = int(plan_month[:4]), int(plan_month[5:7])
    mask   = (df["date_dt"].dt.year == yr) & (df["date_dt"].dt.month == mo)
    actuals = (
        df[mask]
        .groupby(["channel", "item_name"])
        .agg(actual_qty=("qty_sold","sum"), actual_rev=("revenue","sum"))
        .reset_index()
    )

    merged = plan_df.merge(actuals, on=["channel","item_name"], how="outer")
    for col in ["base_qty_30d","mkt_uplift_qty","total_qty_30d",
                "base_rev_30d","mkt_uplift_rev","total_rev_30d",
                "planned_mkt_spend","assumed_roas","actual_qty","actual_rev"]:
        if col not in merged.columns:
            merged[col] = 0.0
        merged[col] = merged[col].fillna(0.0)

    today = date.today()
    dim   = _cal.monthrange(yr, mo)[1]
    days_elapsed = today.day if (today.year == yr and today.month == mo) else dim

    merged["days_elapsed"]   = days_elapsed
    merged["prorated_plan"]  = (merged["total_qty_30d"] * days_elapsed / dim).round(1)
    merged["variance"]       = (merged["actual_qty"] - merged["prorated_plan"]).round(1)
    merged["attainment_pct"] = np.where(
        merged["prorated_plan"] > 0,
        (merged["actual_qty"] / merged["prorated_plan"] * 100).round(1), 0.0)
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _month_options(df: pd.DataFrame, future: int = 2) -> list[str]:
    today = date.today()
    end   = date(today.year + (today.month + future - 1) // 12,
                 (today.month + future - 1) % 12 + 1, 1)
    start = df["date_dt"].min().date().replace(day=1) if not df.empty else today.replace(day=1)
    months, cur = [], start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        m = cur.month + 1
        y = cur.year + (m - 1) // 12
        cur = date(y, (m - 1) % 12 + 1, 1)
    return sorted(months, reverse=True)


def _fmt(v: float, prefix="₹") -> str:
    if v >= 1_00_000: return f"{prefix}{v/1_00_000:.1f}L"
    if v >= 1_000:    return f"{prefix}{v/1_000:.1f}K"
    return f"{prefix}{v:,.0f}"


# ─────────────────────────────────────────────────────────────────────────────
# RENDER
# ─────────────────────────────────────────────────────────────────────────────

def render_sop_tab(supabase_client, history_df, master_skus_df, master_chans_df, role):
    st.subheader("📋 S&OP — Sales & Operations Planning")
    st.caption("30-day demand forecast with transparent organic assumptions and marketing uplift per product.")

    if history_df.empty:
        st.info("No sales data yet. Upload data via Smart Upload first.")
        return

    df = history_df.copy()
    for col in ["channel", "item_name"]:
        if col in df.columns:
            df[col] = df[col].astype(object).fillna("").astype(str).str.strip()
    df = df[(df["channel"] != "") & (df["item_name"] != "")]
    if "date_dt" not in df.columns:
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    if df.empty:
        st.warning("Sales data found but no valid dates. Check data format.")
        return

    channels    = sorted(df["channel"].unique())
    items       = sorted(df["item_name"].unique())
    month_opts  = _month_options(df)
    today       = date.today()
    cur_month   = today.strftime("%Y-%m")

    section = st.radio("View:", ["📊 Forecast & Plan", "📈 Actual vs Predicted", "📚 Plan History"],
                       horizontal=True, key="sop_section")
    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — FORECAST & PLAN
    # ══════════════════════════════════════════════════════════════════════════
    if section == "📊 Forecast & Plan":

        plan_month = st.selectbox(
            "Plan month", month_opts,
            index=month_opts.index(cur_month) if cur_month in month_opts else 0,
            key="sop_plan_month",
        )

        # ── Channel filter: seeded from global filter, overridable locally ─────
        # The global filter sets default channels. Users can further narrow
        # channels here for the SOP plan without affecting other tabs.
        # (Channel selection is SOP-specific: you may plan for a subset only.)
        if _GLOBAL_FILTERS_AVAILABLE:
            _gf_sop = _get_global_filters()
            _gf_chans_sop = [c for c in (_gf_sop["channels"] or []) if c in channels] or channels
        else:
            _gf_chans_sop = channels

        sel_channels = st.multiselect(
            "Channels (for this plan)",
            channels, default=_gf_chans_sop,
            key="sop_channels",
            help="Pre-seeded from Global Filter Bar. Adjust here to plan for a specific subset.",
        )
        if not sel_channels:
            st.warning("Select at least one channel.")
            return

        # ── Fetch ALL marketing data upfront ──────────────────────────────────
        with st.spinner("Loading marketing performance data…"):
            mkt = get_marketing_data(sel_channels, items)

        # ── Session-state keys for overrides (persist until page refresh) ─────
        _gf_key   = f"sop_growth_overrides_{plan_month}"
        _roas_key = f"sop_roas_overrides_{plan_month}"
        if _gf_key   not in st.session_state: st.session_state[_gf_key]   = {}
        if _roas_key not in st.session_state: st.session_state[_roas_key] = {}

        # ── Build base forecast (uses session growth overrides) ───────────────
        with st.spinner("Building organic baseline forecast…"):
            try:
                base_df = build_base_forecast(
                    df, forecast_days=30,
                    spend_by_date=mkt["spend_by_date"],
                    growth_overrides=st.session_state[_gf_key],
                )
            except Exception as e:
                st.error(f"Forecast error: {e}")
                base_df = pd.DataFrame()

        if base_df.empty:
            st.warning(f"Not enough history (need ≥ {_MIN_HISTORY_DAYS} days per SKU-channel).")
            return

        base_df = base_df[base_df["channel"].isin(sel_channels)]
        if base_df.empty:
            st.warning("No forecast data for selected channels.")
            return

        # ── Marketing budget inputs ───────────────────────────────────────────
        st.markdown("#### 💰 Marketing Budget Plan")
        st.caption(
            "Enter total planned spend per channel. Budget is automatically allocated "
            "across products proportional to their historical ROAS contribution — "
            "higher-performing products receive more budget."
        )

        ncols = min(len(sel_channels), 4)
        bcols = st.columns(ncols)
        channel_budgets: dict[str, float] = {}
        channel_roas_overrides: dict[str, float] = {}

        for i, ch in enumerate(sel_channels):
            with bcols[i % ncols]:
                st.markdown(f"**{ch}**")
                st.caption(f"📊 {mkt['roas_source'].get(ch, 'No data')}")
                roas_val = mkt["channel_roas"].get(ch,
                           mkt["company_roas"] if mkt["company_roas"] else _DEFAULT_ROAS)
                channel_budgets[ch] = st.number_input(
                    "Budget (₹)", min_value=0.0, step=1000.0, value=0.0,
                    key=f"sop_budget_{ch}",
                )
                channel_roas_overrides[ch] = st.number_input(
                    "Channel ROAS", min_value=0.1, step=0.1,
                    value=float(round(roas_val, 1)),
                    key=f"sop_roas_{ch}",
                    help="Override ROAS if needed. Product-level ROAS from marketing DB is used per SKU.",
                )

        # ── Apply marketing uplift (uses session ROAS overrides per SKU) ──────
        forecast_df = apply_marketing_uplift(
            base_df, channel_budgets, channel_roas_overrides,
            mkt["product_roas"], mkt["company_roas"],
            sku_roas_overrides=st.session_state[_roas_key],
        )

        total_budget = sum(channel_budgets.values())

        # ── Top-line metrics ──────────────────────────────────────────────────
        st.divider()
        st.markdown(f"#### 📊 30-Day Forecast — {plan_month}")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Organic Units",   f"{forecast_df['base_qty_30d'].sum():,.0f}")
        m2.metric("Mkt-Driven Units",f"{forecast_df['mkt_uplift_qty'].sum():,.0f}",
                  delta=f"+{forecast_df['mkt_uplift_qty'].sum():,.0f}" if total_budget > 0 else None)
        m3.metric("Total Units",     f"{forecast_df['total_qty_30d'].sum():,.0f}")
        m4.metric("Total Revenue",   _fmt(forecast_df["total_rev_30d"].sum()))
        m5.metric("Marketing Budget",_fmt(total_budget))

        # ── Product × Channel pivot table ─────────────────────────────────────
        st.divider()
        st.markdown("#### 📦 Product × Channel Forecast Table")

        view = st.radio(
            "Show values as:",
            ["Total Units", "Total Revenue (₹)", "Organic Units", "Organic Revenue (₹)",
             "Marketing Units", "Marketing Revenue (₹)"],
            horizontal=True, key="sop_pivot_view",
        )

        col_map = {
            "Total Units":             "total_qty_30d",
            "Total Revenue (₹)":       "total_rev_30d",
            "Organic Units":           "base_qty_30d",
            "Organic Revenue (₹)":     "base_rev_30d",
            "Marketing Units":         "mkt_uplift_qty",
            "Marketing Revenue (₹)":   "mkt_uplift_rev",
        }
        is_rev = "Revenue" in view
        val_col = col_map[view]

        if val_col in forecast_df.columns:
            piv = (
                forecast_df
                .pivot_table(index="item_name", columns="channel",
                             values=val_col, aggfunc="sum")
                .fillna(0)
                .round(0 if is_rev else 1)
            )
            piv["TOTAL"] = piv.sum(axis=1)
            piv = piv.sort_values("TOTAL", ascending=False)

            ch_totals = piv.sum().rename("CHANNEL TOTAL")
            piv_display = pd.concat([piv, ch_totals.to_frame().T])

            fmt_str = "₹{:,.0f}" if is_rev else "{:,.1f}"
            color_cols = [c for c in piv.columns if c != "TOTAL"]

            st.dataframe(
                _apply_gradient(
                    piv_display.style.format(fmt_str),
                    subset=pd.IndexSlice[piv.index, color_cols],
                    cmap="Blues",
                ),
                use_container_width=True,
            )

        # ── Editable Growth Factor table ──────────────────────────────────────
        st.divider()
        st.markdown("#### ✏️ Growth Factor Assumptions")
        st.caption(
            "**How growth factor is calculated:** The model computes the average daily units "
            "sold over the last 30 days vs. the full history average. "
            "If the last 30d average is 20% higher than the overall average, "
            "growth factor = 1.20×. It is capped between 0.50× and 2.00× to prevent extremes. "
            "You can override it below — your changes persist until the page is refreshed."
        )

        gf_editor_cols = ["channel", "item_name", "growth_factor_model",
                          "growth_factor", "drr_qty", "base_qty_30d", "history_days_used"]
        gf_show = [c for c in gf_editor_cols if c in forecast_df.columns]
        gf_rename = {
            "channel": "Channel", "item_name": "SKU",
            "growth_factor_model": "Model GF",
            "growth_factor": "Applied GF ✏️",
            "drr_qty": "Organic DRR/day",
            "base_qty_30d": "Organic Forecast",
            "history_days_used": "History Days",
        }
        gf_edit_df = (
            forecast_df[gf_show]
            .rename(columns=gf_rename)
            .sort_values(["Channel", "Organic Forecast"], ascending=[True, False])
            .reset_index(drop=True)
        )

        edited_gf = st.data_editor(
            gf_edit_df,
            use_container_width=True,
            hide_index=True,
            key="sop_gf_editor",
            column_config={
                "Channel":          st.column_config.TextColumn(disabled=True),
                "SKU":              st.column_config.TextColumn(disabled=True),
                "Model GF":         st.column_config.NumberColumn(
                                        "Model GF", format="%.3f×",
                                        help="Model-computed growth factor (last 30d avg ÷ full history avg, capped 0.5–2.0)",
                                        disabled=True),
                "Applied GF ✏️":    st.column_config.NumberColumn(
                                        "Applied GF ✏️", format="%.3f×",
                                        min_value=0.1, max_value=5.0, step=0.05,
                                        help="Edit to override. Values outside 0.1–5.0 are clamped."),
                "Organic DRR/day":  st.column_config.NumberColumn(format="%.3f", disabled=True),
                "Organic Forecast": st.column_config.NumberColumn(format="%.1f units", disabled=True),
                "History Days":     st.column_config.NumberColumn(disabled=True),
            },
        )

        # Detect edits and persist to session state
        if edited_gf is not None:
            new_gf_overrides = {}
            for _, row in edited_gf.iterrows():
                ch, sku = row["Channel"], row["SKU"]
                applied = float(row["Applied GF ✏️"])
                # Look up model value from forecast_df
                model_rows = forecast_df[
                    (forecast_df["channel"] == ch) & (forecast_df["item_name"] == sku)
                ]
                model_val = float(model_rows["growth_factor_model"].iloc[0]) if not model_rows.empty else applied
                if abs(applied - model_val) > 0.001:  # user changed it
                    new_gf_overrides[(ch, sku)] = applied
            if new_gf_overrides != st.session_state[_gf_key]:
                st.session_state[_gf_key] = new_gf_overrides
                st.rerun()

        # ── Detailed assumptions table ─────────────────────────────────────────
        st.divider()
        st.markdown("#### 🔍 Organic Forecast Assumptions — How Each Number Was Built")
        st.caption(
            "Every organic forecast is built from actual history. "
            "The 'Assumption' column explains the exact logic used for that SKU × Channel."
        )

        assump_cols = [
            "channel", "item_name",
            "base_qty_30d", "hist_monthly_avg_qty", "growth_factor",
            "drr_qty", "organic_share", "organic_days_used", "history_days_used",
            "organic_assumption",
        ]
        assump_show = [c for c in assump_cols if c in forecast_df.columns]
        assump_rename = {
            "channel": "Channel", "item_name": "SKU",
            "base_qty_30d": "Organic Forecast",
            "hist_monthly_avg_qty": "Hist Monthly Avg",
            "growth_factor": "Growth Factor",
            "drr_qty": "Organic DRR/day",
            "organic_share": "Organic %",
            "organic_days_used": "Organic Days",
            "history_days_used": "Total History Days",
            "organic_assumption": "Assumption",
        }
        assump_df = (
            forecast_df[assump_show]
            .rename(columns=assump_rename)
            .sort_values(["Channel", "Organic Forecast"], ascending=[True, False])
        )
        st.dataframe(
            assump_df.style.format({
                "Organic Forecast": "{:,.1f}",
                "Hist Monthly Avg": "{:,.1f}",
                "Growth Factor": "{:.3f}×",
                "Organic DRR/day": "{:.3f}",
                "Organic %": "{:.1%}",
            }),
            use_container_width=True, hide_index=True,
        )

        # ── Marketing allocation table (editable ROAS per SKU) ────────────────
        if total_budget > 0:
            st.divider()
            st.markdown("#### 💸 Marketing Budget Allocation by Product")
            st.caption(
                "Budget is split across products by ROAS contribution "
                "(product ROAS × organic revenue share). "
                "Higher-performing products receive more budget. "
                "**Edit the ROAS column** to test assumptions — changes persist until page refresh."
            )
            mkt_cols = ["channel", "item_name", "planned_mkt_spend", "sku_roas",
                        "mkt_uplift_rev", "mkt_uplift_qty", "rev_share_in_channel", "roas_overridden"]
            mkt_show = [c for c in mkt_cols if c in forecast_df.columns]
            mkt_edit_df = (
                forecast_df[mkt_show]
                .rename(columns={
                    "channel": "Channel", "item_name": "SKU",
                    "planned_mkt_spend": "Budget Allocated (₹)",
                    "sku_roas": "ROAS ✏️",
                    "mkt_uplift_rev": "Incremental Rev (₹)",
                    "mkt_uplift_qty": "Incremental Units",
                    "rev_share_in_channel": "Revenue Share in Ch",
                    "roas_overridden": "User Edited?",
                })
                .sort_values(["Channel", "Budget Allocated (₹)"], ascending=[True, False])
                .reset_index(drop=True)
            )

            edited_mkt = st.data_editor(
                mkt_edit_df,
                use_container_width=True,
                hide_index=True,
                key="sop_mkt_editor",
                column_config={
                    "Channel":              st.column_config.TextColumn(disabled=True),
                    "SKU":                  st.column_config.TextColumn(disabled=True),
                    "Budget Allocated (₹)": st.column_config.NumberColumn(format="₹%.0f", disabled=True),
                    "ROAS ✏️":              st.column_config.NumberColumn(
                                                "ROAS ✏️", format="%.2f×",
                                                min_value=0.1, max_value=50.0, step=0.1,
                                                help="Edit to override ROAS for this SKU. "
                                                     "Affects budget allocation weight and incremental revenue."),
                    "Incremental Rev (₹)":  st.column_config.NumberColumn(format="₹%.0f", disabled=True),
                    "Incremental Units":    st.column_config.NumberColumn(format="%.1f", disabled=True),
                    "Revenue Share in Ch":  st.column_config.NumberColumn(format="%.1%", disabled=True),
                    "User Edited?":         st.column_config.CheckboxColumn(disabled=True),
                },
            )

            # Detect ROAS edits and persist to session state
            if edited_mkt is not None:
                new_roas_overrides = dict(st.session_state[_roas_key])  # start from existing
                for _, row in edited_mkt.iterrows():
                    ch, sku = row["Channel"], row["SKU"]
                    edited_roas = float(row["ROAS ✏️"])
                    # Compare against what the model computed (before any override)
                    base_rows = forecast_df[
                        (forecast_df["channel"] == ch) & (forecast_df["item_name"] == sku)
                    ]
                    if not base_rows.empty:
                        # model ROAS = sku_roas ignoring overrides
                        def _model_roas(r):
                            if (r["channel"], r["item_name"]) in mkt["product_roas"]:
                                return mkt["product_roas"][(r["channel"], r["item_name"])]
                            if r["channel"] in channel_roas_overrides:
                                return channel_roas_overrides[r["channel"]]
                            if mkt["company_roas"] is not None:
                                return mkt["company_roas"]
                            return _DEFAULT_ROAS
                        model_roas_val = float(_model_roas(base_rows.iloc[0]))
                        if abs(edited_roas - model_roas_val) > 0.005:
                            new_roas_overrides[(ch, sku)] = edited_roas
                        elif (ch, sku) in new_roas_overrides:
                            # User reset back to model value — remove override
                            del new_roas_overrides[(ch, sku)]
                if new_roas_overrides != st.session_state[_roas_key]:
                    st.session_state[_roas_key] = new_roas_overrides
                    st.rerun()

            # Show override summary if any
            if st.session_state[_roas_key]:
                n_ov = len(st.session_state[_roas_key])
                st.info(f"ℹ️ {n_ov} SKU ROAS override(s) active for {plan_month}. "
                        "Refresh the page to reset all overrides.")

        # ── Save plan ──────────────────────────────────────────────────────────
        if role == "admin":
            st.divider()
            if st.button("💾 Save Plan to Cloud", key="sop_save_plan"):
                rows = []
                for _, r in forecast_df.iterrows():
                    rows.append({
                        "plan_month":           plan_month,
                        "channel":              r["channel"],
                        "item_name":            r["item_name"],
                        "base_qty_30d":         float(r["base_qty_30d"]),
                        "mkt_uplift_qty":       float(r.get("mkt_uplift_qty", 0)),
                        "total_qty_30d":        float(r.get("total_qty_30d", r["base_qty_30d"])),
                        "base_rev_30d":         float(r["base_rev_30d"]),
                        "mkt_uplift_rev":       float(r.get("mkt_uplift_rev", 0)),
                        "total_rev_30d":        float(r.get("total_rev_30d", r["base_rev_30d"])),
                        "planned_mkt_spend":    float(channel_budgets.get(r["channel"], 0)),
                        "assumed_roas":         float(r.get("sku_roas", _DEFAULT_ROAS)),
                        "organic_assumption":   str(r.get("organic_assumption", "")),
                        # ── Store assumption overrides for audit trail ───────
                        "growth_factor":        float(r.get("growth_factor", 1.0)),
                        "growth_factor_model":  float(r.get("growth_factor_model", 1.0)),
                        "growth_overridden":    bool(r.get("growth_overridden", False)),
                        "roas_overridden":      bool(r.get("roas_overridden", False)),
                    })
                ok, err = _save_plan(supabase_client, rows)
                if ok:
                    st.success(f"✅ Plan for {plan_month} saved — {len(rows)} records.")
                    st.caption(
                        "Growth factor overrides and ROAS overrides are embedded in "
                        "the saved plan for comparison against actuals."
                    )
                else:
                    st.error(f"Save failed: {err}")
                    st.info("Ensure the `sop_plans` table exists in Supabase (see module docstring).")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — ACTUAL vs PREDICTED
    # ══════════════════════════════════════════════════════════════════════════
    elif section == "📈 Actual vs Predicted":

        plan_month_avp = st.selectbox(
            "Select plan month", month_opts, key="sop_avp_month",
            index=month_opts.index(cur_month) if cur_month in month_opts else 0,
        )

        plan_df = _load_plan(supabase_client, plan_month_avp)
        if plan_df.empty:
            st.info(f"No saved plan for {plan_month_avp}. Build and save a plan first.")
            return

        yr, mo      = int(plan_month_avp[:4]), int(plan_month_avp[5:7])
        dim         = _cal.monthrange(yr, mo)[1]
        is_current  = (today.year == yr and today.month == mo)
        days_el     = today.day if is_current else dim
        pct_el      = days_el / dim * 100

        st.info(
            f"📅 {_cal.month_name[mo]} {yr} — "
            f"**{days_el}/{dim} days elapsed ({pct_el:.0f}%)**"
            + (" ← in progress" if is_current else " ← complete")
        )

        avp_df = build_actuals_vs_plan(df, plan_df, plan_month_avp)
        if avp_df.empty:
            st.warning("Could not build comparison.")
            return

        tot_plan   = avp_df["prorated_plan"].sum()
        tot_actual = avp_df["actual_qty"].sum()
        attain     = (tot_actual / tot_plan * 100) if tot_plan > 0 else 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Pro-rated Plan", f"{tot_plan:,.0f} units")
        m2.metric("Actual to Date", f"{tot_actual:,.0f} units")
        m3.metric("Variance",       f"{tot_actual - tot_plan:+,.0f} units",
                  delta=f"{tot_actual - tot_plan:+,.0f}", delta_color="normal")
        m4.metric("Attainment",     f"{attain:.1f}%",
                  delta=f"{attain - 100:+.1f}pp")

        st.divider()

        # Attainment pivot — Product × Channel
        st.markdown("#### 📋 Attainment: Actual vs Plan by Product × Channel")
        avp_view = st.radio(
            "Show:", ["Actual Units", "Plan Units", "Variance", "Attainment %"],
            horizontal=True, key="sop_avp_view",
        )
        avp_col_map = {
            "Actual Units":  "actual_qty",
            "Plan Units":    "prorated_plan",
            "Variance":      "variance",
            "Attainment %":  "attainment_pct",
        }
        vc = avp_col_map[avp_view]
        if vc in avp_df.columns:
            apiv = (
                avp_df.pivot_table(index="item_name", columns="channel",
                                   values=vc, aggfunc="sum")
                .fillna(0).round(1)
            )
            apiv["TOTAL"] = apiv.sum(axis=1)
            apiv = apiv.sort_values("TOTAL", ascending=False)
            is_pct = "%" in avp_view
            afmt   = "{:.1f}%" if is_pct else "{:+.1f}" if "Variance" in avp_view else "{:,.1f}"
            color_cols = [c for c in apiv.columns if c != "TOTAL"]
            cmap = "RdYlGn" if "Variance" in avp_view or "Attainment" in avp_view else "Blues"
            st.dataframe(
                _apply_gradient(
                    apiv.style.format(afmt),
                    subset=pd.IndexSlice[apiv.index, color_cols],
                    cmap=cmap,
                ),
                use_container_width=True,
            )

        # Daily trajectory chart
        st.markdown("#### 📅 Daily Actuals vs Plan Trajectory")
        mm = (df["date_dt"].dt.year == yr) & (df["date_dt"].dt.month == mo)
        da = df[mm].groupby("date_dt")[["qty_sold","revenue"]].sum().reset_index().sort_values("date_dt")
        if not da.empty:
            daily_plan = avp_df["total_qty_30d"].sum() / dim
            da["cum_actual"] = da["qty_sold"].cumsum()
            cum_plan = [(i+1)*daily_plan for i in range(len(da))]
            fig = go.Figure()
            fig.add_bar(x=da["date_dt"], y=da["qty_sold"], name="Actual", marker_color="#4C8BF5", opacity=0.7)
            fig.add_hline(y=daily_plan, line_dash="dash", line_color="#E63946",
                          annotation_text=f"Daily Plan ({daily_plan:.1f})")
            fig.add_scatter(x=da["date_dt"], y=da["cum_actual"], name="Cumulative Actual",
                            mode="lines+markers", line=dict(color="#2E9E4F", width=2), yaxis="y2")
            fig.add_scatter(x=da["date_dt"], y=cum_plan, name="Cumulative Plan",
                            mode="lines", line=dict(color="#E63946", width=2, dash="dot"), yaxis="y2")
            fig.update_layout(height=400, yaxis=dict(title="Daily Units"),
                              yaxis2=dict(title="Cumulative", overlaying="y", side="right"),
                              legend=dict(orientation="h", y=1.1), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No sales data yet for {_cal.month_name[mo]} {yr}.")

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — PLAN HISTORY
    # ══════════════════════════════════════════════════════════════════════════
    else:
        st.markdown("#### 📚 All Saved S&OP Plans")
        all_plans = _load_all_plans(supabase_client)
        if all_plans.empty:
            st.info("No plans saved yet.")
            return

        summary = (
            all_plans.groupby("plan_month").agg(
                records     =("item_name", "count"),
                organic_qty =("base_qty_30d", "sum"),
                mkt_qty     =("mkt_uplift_qty", "sum"),
                total_qty   =("total_qty_30d", "sum"),
                total_rev   =("total_rev_30d", "sum"),
                budget      =("planned_mkt_spend", "sum"),
            ).reset_index().sort_values("plan_month", ascending=False)
        )
        st.dataframe(
            summary.rename(columns={
                "plan_month":"Month","records":"SKU-Ch",
                "organic_qty":"Organic Units","mkt_qty":"Mkt Units",
                "total_qty":"Total Units","total_rev":"Total Rev (₹)","budget":"Budget (₹)",
            }).style.format({
                "Organic Units":"{:,.0f}","Mkt Units":"{:,.0f}","Total Units":"{:,.0f}",
                "Total Rev (₹)":"₹{:,.0f}","Budget (₹)":"₹{:,.0f}",
            }),
            use_container_width=True, hide_index=True,
        )

        drill = st.selectbox("Drill into month", all_plans["plan_month"].unique(), key="sop_drill")
        ddf   = all_plans[all_plans["plan_month"] == drill]

        drill_view = st.radio(
            "Show:", ["Total Units","Total Revenue (₹)","Organic Units","Organic Revenue (₹)"],
            horizontal=True, key="sop_drill_view",
        )
        dvcm = {"Total Units":"total_qty_30d","Total Revenue (₹)":"total_rev_30d",
                "Organic Units":"base_qty_30d","Organic Revenue (₹)":"base_rev_30d"}
        dvc  = dvcm[drill_view]
        if dvc in ddf.columns:
            dpiv = (
                ddf.pivot_table(index="item_name", columns="channel",
                                values=dvc, aggfunc="sum")
                .fillna(0).round(1 if "Units" in drill_view else 0)
            )
            dpiv["TOTAL"] = dpiv.sum(axis=1)
            dpiv = dpiv.sort_values("TOTAL", ascending=False)
            dfmt = "₹{:,.0f}" if "Revenue" in drill_view else "{:,.1f}"
            st.dataframe(dpiv.style.format(dfmt), use_container_width=True)

        if role == "admin":
            if st.button("🗑️ Delete this plan", key="sop_delete"):
                try:
                    supabase_client.table(_PLAN_TABLE).delete().eq("plan_month", drill).execute()
                    st.success(f"Deleted plan for {drill}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")
