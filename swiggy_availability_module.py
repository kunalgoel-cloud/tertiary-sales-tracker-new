"""
swiggy_availability_module.py
──────────────────────────────────────────────────────────────────────────────
Swiggy Instamart On-Shelf-Availability (OSA) sub-tab.

WHY A SEPARATE SUBMODULE (not folded into channel_performance_module's
_render_dashboard):
  - The OSA report has a fundamentally different shape from the unified
    channel_sku / inventory / str / doc / drr / location schema used by
    _parse_swiggy(), _parse_blinkit(), _parse_amazon(), _parse_bigbasket().
    Those model ONE stock pool per SKU-location. This report models TWO
    tiers (Warehouse -> Pod/dark-store) plus a listing-visibility metric
    (Coverage) that has no equivalent on any other channel today.
  - It's Swiggy-only. No other channel currently exports Wh/Pod split +
    Coverage, so generalising the shared quadrant engine for it now would
    add branching complexity for zero other channels.
  - It comes in two grains from Swiggy's export tool: an item-level file
    (CityName x ItemName, has WhStock/PodStock) and a category-level roll-up
    (CityName x L1CategoryName, no stock counts — trend/coverage only).
    Both are handled here; only the item-level file supports the
    PO / Visibility / Liquidation triage below since that needs unit counts.

HOW IT PLUGS IN (channel_performance_module.py):
  from swiggy_availability_module import render_swiggy_availability_subtab
  ...
  # inside render_channel_performance_tab(), after the existing 4-channel
  # uploader row / _render_dashboard() call:
  st.divider()
  with st.expander("📶 Swiggy Availability (OSA) — beta", expanded=False):
      render_swiggy_availability_subtab(supabase_client)

Kept channel-specific and opt-in (expander, not a hard new top-level tab) so
it doesn't disrupt the existing Reorder/Promotion/Visibility/Rationalise
quadrant flow that Amazon/Blinkit/Swiggy-inventory/BigBasket already share.
If Blinkit or BigBasket ever start exporting a similar Wh->Pod OSA report,
_classify_actions() below is channel-agnostic and can be reused directly —
only the file-upload wiring in render_swiggy_availability_subtab() would
need to become a per-channel loop.
"""

import pandas as pd
import streamlit as st

# Reuse the same file-loading helper already used for inventory files.
# NOTE: _csv_bytes in channel_performance_module.py is defined *inside*
# _render_dashboard() (a local closure), not at module level, so it can't be
# imported — this module defines its own equivalent below instead.
from channel_performance_module import _load_file  # existing top-level helper

# NOT wired up yet — see the note on render_swiggy_availability_subtab()'s
# supabase_client parameter below. Left as the intended table name for
# whoever implements save/load next, but nothing in this file reads or
# writes it today.
SNAPSHOT_TABLE = "swiggy_osa_snapshots"


def _csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# City → warehouse clustering
# ─────────────────────────────────────────────────────────────────────────────
# Swiggy's OSA export reports WhStock per CityName, not per physical
# warehouse — so there's no warehouse identifier in the file itself. This is
# a GEOGRAPHIC APPROXIMATION (state/region proximity to a hub city), not
# Swiggy's actual fulfilment routing, built because that real mapping wasn't
# available when this was added. It covers every CityName seen in the two
# sample exports this sub-tab was built against; an unrecognised city falls
# back to its own name as a single-city "warehouse" rather than silently
# mis-clubbing it. If you have the real routing, replace _WAREHOUSE_CITIES
# below — everything downstream keys off _warehouse_for_city().

_WAREHOUSE_CITIES: dict[str, list[str]] = {
    "Noida":       ["DELHI", "NOIDA", "GURGAON", "FARIDABAD", "MEERUT", "AGRA",
                     "ALIGARH", "MATHURA", "MORADABAD", "ROORKEE", "SAHARANPUR", "DEHRADUN"],
    "Chandigarh":  ["CHANDIGARH", "AMBALA", "KARNAL", "PANIPAT", "SONIPAT", "ROHTAK",
                     "LUDHIANA", "JALANDHAR", "AMRITSAR", "PATIALA", "BATHINDA"],
    "Jaipur":      ["JAIPUR", "BIKANER", "UDAIPUR", "SRI GANGANAGAR"],
    "Lucknow":     ["LUCKNOW"],
    "Bhopal":      ["BHOPAL", "INDORE", "JABALPUR", "GWALIOR", "UJJAIN"],
    "Ahmedabad":   ["AHMEDABAD", "SURAT", "VADODARA", "RAJKOT", "ANAND", "VAPI"],
    "Mumbai":      ["MUMBAI", "PUNE", "NASHIK", "AURANGABAD", "KOLHAPUR", "LATUR",
                     "NANDED", "AMRAVATI", "LONAVLA", "NAGPUR", "CENTRAL GOA"],
    "Bangalore":   ["BANGALORE", "MYSORE", "HUBLI", "BELGAUM", "DAVANAGERE",
                     "SHIVAMOGGA", "TUMAKURU", "MANGALURU", "MANIPAL"],
    "Chennai":     ["CHENNAI", "COIMBATORE", "MADURAI", "TRICHY", "SALEM", "ERODE",
                     "TIRUPUR", "VELLORE", "KARUR", "DINDIGUL", "THANJAVUR",
                     "TIRUNELVELI", "THOOTHUKUDI", "KANCHIPURAM", "KARAIKKUDI",
                     "NAGERCOIL", "PONDICHERRY", "THIRUVALLUR"],
    "Kochi":       ["KOCHI", "THIRUVANANTHAPURAM", "KOZHIKODE", "THRISSUR", "KOLLAM",
                     "KOTTAYAM", "ALAPPUZHA", "PALAKKAD", "KANNUR", "THIRUVALLA"],
    "Hyderabad":   ["HYDERABAD", "KHAMMAM"],
    "Vijayawada":  ["VIJAYAWADA", "GUNTUR", "VIZAG", "VIZIANAGARAM", "RAJAHMUNDRY",
                     "KAKINADA", "ELURU", "BHIMAVARAM", "NELLORE", "TIRUPATI", "ANANTAPUR"],
    "Bhubaneswar": ["BHUBANESWAR", "BERHAMPUR"],
    "Kolkata":     ["KOLKATA", "SILIGURI"],
    "Guwahati":    ["GUWAHATI", "DIBRUGARH", "SILCHAR"],
    "Patna":       ["PATNA", "RANCHI", "JAMSHEDPUR"],
    "Raipur":      ["RAIPUR", "BILASPUR", "BHILAI"],
}

_CITY_TO_WAREHOUSE: dict[str, str] = {
    city: wh for wh, cities in _WAREHOUSE_CITIES.items() for city in cities
}


def _warehouse_for_city(city) -> str:
    """CityName -> warehouse cluster. Unrecognised cities map to themselves
    (title-cased) so they're still visibly grouped as their own bucket
    instead of silently disappearing into an "Other" catch-all."""
    key = str(city).strip().upper()
    return _CITY_TO_WAREHOUSE.get(key, str(city).strip().title())


# ─────────────────────────────────────────────────────────────────────────────
# Format detection + parsing
# ─────────────────────────────────────────────────────────────────────────────

def _detect_format(df: pd.DataFrame) -> str:
    """
    'item'     -> CityName x ItemName grain, has WhStock/PodStock (full triage possible)
    'category' -> CityName x L1CategoryName grain, no stock counts (trend/coverage only)
    """
    cols = set(df.columns)
    if {"ItemName", "WhStock", "PodStock"}.issubset(cols):
        return "item"
    if {"L1CategoryName", "CategoryId"}.issubset(cols) and "ItemName" not in cols:
        return "category"
    raise ValueError(
        "Unrecognised Swiggy OSA format — expected either the item-level "
        "export (ItemName, WhStock, PodStock, ...) or the category-level "
        "export (L1CategoryName, CategoryId, ...)."
    )


def _load_osa_file(uploaded_file) -> tuple[pd.DataFrame, str]:
    df = _load_file(uploaded_file)
    df.columns = [c.strip() for c in df.columns]
    kind = _detect_format(df)
    df["Date"] = pd.to_datetime(df["Date"])
    for c in ["WhAvailability", "WhDOH", "PodAvailability", "Sales", "FillRate", "Coverage",
              "PodStock", "WhStock"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["Warehouse"] = df["CityName"].apply(_warehouse_for_city)
    return df, kind


# ─────────────────────────────────────────────────────────────────────────────
# Category-manager action triage  (item-level file only)
# ─────────────────────────────────────────────────────────────────────────────

def _classify_actions(
    df: pd.DataFrame,
    coverage_thresh: float = 50.0,
    liquidation_availability_thresh: float = 80.0,
) -> pd.DataFrame:
    """
    Collapses the item-level OSA file to one row per (CityName, ItemName)
    over the uploaded date range, then tags each with a single action bucket:

      Need PO          — Warehouse stock ~0 (root-cause: nothing to send to pods)
      Need Visibility   — Pod HAS stock, but Coverage < threshold: it's sitting
                          in the dark store yet not listed/live to customers.
      Need Liquidation  — Pod HAS stock, IS covered (listed), availability is
                          high, but generated zero sales all period — dead
                          stock at the shelf, not a supply or listing problem.
      Healthy           — none of the above.

    Buckets are evaluated in this priority order (WH > Visibility >
    Liquidation) because a warehouse stockout is the upstream root cause even
    if a pod row also happens to look overstocked from a prior fill.
    """
    agg = (
        df.groupby(["CityName", "ItemName"], as_index=False)
        .agg(
            avg_WhStock=("WhStock", "mean"),
            avg_WhAvailability=("WhAvailability", "mean"),
            avg_PodStock=("PodStock", "mean"),
            avg_PodAvailability=("PodAvailability", "mean"),
            avg_Coverage=("Coverage", "mean"),
            total_Sales=("Sales", "sum"),
            days_reported=("Date", "nunique"),
        )
    )

    def _tag(r):
        if r.avg_WhStock <= 0.5:
            return "Need PO"
        if r.avg_PodStock > 0 and r.avg_Coverage < coverage_thresh:
            return "Need Visibility"
        if (
            r.avg_PodStock > 0
            and r.avg_Coverage >= coverage_thresh
            and r.avg_PodAvailability >= liquidation_availability_thresh
            and r.total_Sales == 0
        ):
            return "Need Liquidation"
        return "Healthy"

    agg["action"] = agg.apply(_tag, axis=1)
    agg["Warehouse"] = agg["CityName"].apply(_warehouse_for_city)
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# Rendering
# ─────────────────────────────────────────────────────────────────────────────

_ACTION_META = {
    "Need PO": (
        "🔴",
        "Warehouse stock is ~0 for this SKU-city. The pod may still be selling "
        "through leftover stock, but nothing is being replenished from the WH.",
        "Raise a purchase order / expedite the next inbound.",
    ),
    "Need Visibility": (
        "🟡",
        "Stock exists at the dark store, but Coverage is below threshold — the "
        "item isn't listed/live across most pods it should be in.",
        "File a listing/visibility ticket with the Swiggy account team; this is "
        "lost sales with stock already sitting in the city.",
    ),
    "Need Liquidation": (
        "🟣",
        "Stock exists, it's listed and available to customers, and it still "
        "sold zero units over the period.",
        "Push a local promo/discount, or reallocate the stock to a city with "
        "actual demand.",
    ),
    "Healthy": ("🟢", "No flagged issue for this SKU-city in the uploaded period.", "No action needed."),
}


def _rollup_by_warehouse(bucket_df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapses a bucket's per-(CityName, ItemName) rows to one row per
    Warehouse — the shape a category manager actually raises a PO against,
    since Swiggy is replenished warehouse by warehouse, not city by city.
    """
    return (
        bucket_df.groupby("Warehouse", as_index=False)
        .agg(
            sku_cities=("ItemName", "size"),
            cities_affected=("CityName", "nunique"),
            skus_affected=("ItemName", "nunique"),
            total_avg_WhStock=("avg_WhStock", "sum"),
            total_avg_PodStock=("avg_PodStock", "sum"),
            total_Sales=("total_Sales", "sum"),
        )
        .sort_values("sku_cities", ascending=False)
        .reset_index(drop=True)
    )


def _render_bucket_tab(bucket_df: pd.DataFrame, action: str):
    icon, desc, action_txt = _ACTION_META[action]
    st.markdown(desc)
    st.caption(f"Action: **{action_txt}**")
    if bucket_df.empty:
        st.success("✅ Nothing in this bucket.")
        return
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("SKU-cities", len(bucket_df))
    c2.metric("Cities affected", bucket_df["CityName"].nunique())
    c3.metric("SKUs affected", bucket_df["ItemName"].nunique())
    c4.metric("Warehouses affected", bucket_df["Warehouse"].nunique())

    group_by = st.radio(
        "Group by:", ["City (detail)", "Warehouse (for raising POs)"],
        horizontal=True, index=0, key=f"osa_groupby_{action}",
    )

    if group_by.startswith("Warehouse"):
        display_df = _rollup_by_warehouse(bucket_df)
        st.caption(
            "One row per warehouse — sums the underlying city-level rows. "
            "City→warehouse clustering is a geographic approximation, not "
            "Swiggy's actual routing (see the module docstring); recheck "
            "before raising a PO against an unfamiliar grouping."
        )
        st.dataframe(
            display_df.style.format({
                "total_avg_WhStock": "{:.0f}", "total_avg_PodStock": "{:.0f}",
                "total_Sales": "{:.0f}",
            }),
            use_container_width=True,
        )
        download_df = display_df
    else:
        show_cols = ["Warehouse", "CityName", "ItemName", "avg_WhStock", "avg_PodStock",
                     "avg_Coverage", "avg_PodAvailability", "total_Sales"]
        display_df = bucket_df[show_cols].sort_values(["Warehouse", "CityName"]).reset_index(drop=True)
        st.dataframe(
            display_df.style.format({
                "avg_WhStock": "{:.0f}", "avg_PodStock": "{:.0f}",
                "avg_Coverage": "{:.1f}%", "avg_PodAvailability": "{:.1f}%",
                "total_Sales": "{:.0f}",
            }),
            use_container_width=True,
        )
        download_df = display_df

    fname_suffix = "by_warehouse" if group_by.startswith("Warehouse") else "by_city"
    fname = f"swiggy_{action.lower().replace(' ', '_')}_{fname_suffix}.csv"
    st.download_button(
        f"⬇️ Download '{action}' list as CSV ({fname_suffix.replace('_', ' ')})",
        data=_csv_bytes(download_df),
        file_name=fname,
        mime="text/csv",
        use_container_width=True,
        key=f"osa_dl_{action}",
    )


def render_swiggy_availability_subtab(supabase_client=None):
    """
    supabase_client is accepted (and unused) to match the call in
    render_channel_performance_tab() and keep this a drop-in call. Snapshot
    persistence — auto-restoring the last upload the way _save_snapshot() /
    _load_snapshots() do for the 4 inventory channels — is intentionally out
    of scope for this pass: unlike those, this report is raw daily rows (not
    one current-state row per SKU), the item-level and category-level grains
    don't share a column set, and there's no confirmed swiggy_osa_snapshots
    table/migration yet. Wiring it up is a reasonable follow-up once that
    table exists.
    """
    st.markdown("#### 📶 Swiggy Instamart — On-Shelf Availability")
    st.caption(
        "Separate from the inventory file above — this is Swiggy's Wh→Pod "
        "availability export. Upload either the item-level file (enables the "
        "category-manager action triage) or the category roll-up (trend view only)."
    )

    up_file = st.file_uploader(
        "Swiggy Availability Report", type=["csv", "xlsx", "xls"], key="cp_swg_osa"
    )
    if not up_file:
        st.info("Upload a Swiggy OSA export to see availability trend and action buckets.")
        return

    try:
        df, kind = _load_osa_file(up_file)
    except ValueError as e:
        st.error(str(e))
        return

    date_min, date_max = df["Date"].min().date(), df["Date"].max().date()
    st.success(f"✅ {len(df):,} rows loaded ({date_min} → {date_max}) — detected **{kind}-level** format")

    # ── Warehouse filter (applies to trend + action buckets below) ──────────
    # See _WAREHOUSE_CITIES near the top of this file: cities are clustered
    # into warehouses by geographic approximation, not confirmed Swiggy
    # routing.
    warehouses = sorted(df["Warehouse"].unique())
    wh_filter = st.multiselect("Filter warehouses", warehouses, default=[], key="cp_swg_osa_wh_filter")
    if wh_filter:
        df = df[df["Warehouse"].isin(wh_filter)]

    # ── National trend (both formats) ───────────────────────────────────────
    trend = (
        df.groupby("Date")
        .agg(WhAvailability=("WhAvailability", "mean"),
             PodAvailability=("PodAvailability", "mean"),
             Coverage=("Coverage", "mean"),
             Sales=("Sales", "sum"))
        .round(1)
    )
    st.markdown("##### Availability trend")
    st.line_chart(trend[["WhAvailability", "PodAvailability", "Coverage"]])
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Pod Availability", f"{trend['PodAvailability'].mean():.1f}%",
              delta=f"{trend['PodAvailability'].iloc[-1] - trend['PodAvailability'].iloc[0]:.1f}pp over period")
    c2.metric("Avg Warehouse Availability", f"{trend['WhAvailability'].mean():.1f}%")
    c3.metric("Avg Coverage (listed %)", f"{trend['Coverage'].mean():.1f}%")

    if kind == "category":
        st.info(
            "This is the category-level export — no WhStock/PodStock unit counts, "
            "so the PO / Visibility / Liquidation triage below needs the item-level "
            "file instead. Upload that to see it."
        )
        return

    # ── Category-manager filtering ──────────────────────────────────────────
    st.divider()
    st.markdown("##### 🎯 Category Manager Action Buckets")

    fc1, fc2, fc3 = st.columns(3)
    cities = sorted(df["CityName"].unique())
    city_filter = fc1.multiselect("Filter cities", cities, default=[])
    coverage_thresh = fc2.slider("‘No coverage’ threshold (%)", 0, 100, 50, 5)
    avail_thresh = fc3.slider("‘Overstocked’ pod-availability floor (%)", 0, 100, 80, 5)

    scoped = df[df["CityName"].isin(city_filter)] if city_filter else df
    tagged = _classify_actions(
        scoped, coverage_thresh=coverage_thresh, liquidation_availability_thresh=avail_thresh
    )

    counts = tagged["action"].value_counts()
    tabs = st.tabs([
        f"🔴 Need PO ({counts.get('Need PO', 0)})",
        f"🟡 Need Visibility ({counts.get('Need Visibility', 0)})",
        f"🟣 Need Liquidation ({counts.get('Need Liquidation', 0)})",
        f"🟢 Healthy ({counts.get('Healthy', 0)})",
    ])
    for tab, action in zip(tabs, ["Need PO", "Need Visibility", "Need Liquidation", "Healthy"]):
        with tab:
            _render_bucket_tab(tagged[tagged["action"] == action], action)
