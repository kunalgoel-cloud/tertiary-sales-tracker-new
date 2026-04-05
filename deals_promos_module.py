"""
deals_promos_module.py
──────────────────────
Deals & Promos tab for the Mamanourish Executive Tracker.
Currently supports BigBasket as the beta channel.

Logic:
  1. Pulls sales history from the shared `history_df` (already loaded in main app).
  2. Computes per-SKU × per-city performance signals:
       - STR  (sell-through rate)  = units sold / (units sold + closing stock proxy)
         ↳ Since we don't have direct inventory, we approximate via revenue velocity:
           STR ≈ days_with_sales / total_days_in_window
       - DOC  (days of cover)      = avg daily qty sold  →  inverted as coverage days
         ↳ High DOC (>90 percentile) ≈ low velocity / lots of cover left
         ↳ Low  DOC (<10 percentile) ≈ fast moving / thin cover
     These two signals classify each SKU-city into:
         Liq     : STR < 0.20 AND DOC > 90th pct  → needs liquidation pricing
         SVD     : STR > 0.20 AND DOC > 90th pct  → slow mover, run SVD on first 10 days
         Weekend : STR < 0.20 AND DOC < 90th pct  → push on weekends
         BAU     : everything else
  3. User enters target prices per SKU × tier.
  4. Generates a BigBasket-format promo CSV (date-range rows, city columns).
"""

import streamlit as st
import pandas as pd
import calendar
import numpy as np
from datetime import date, timedelta
import io

# ──────────────────────────────────────────────────────────────
# BIG BASKET FORMAT CONSTANTS
# ──────────────────────────────────────────────────────────────
BB_HEADERS = [
    "Code", "Product Description",
    "Start Date (DD-MM-YYYY)", "End Date (DD-MM-YYYY)",
    "Discount Type", "Discount Value",
    "Redemption Limit - Qty Per Campaign", "Pan India",
    "ANDHRA PRADESH", "TELANGANA", "ASSAM", "BIHAR", "CHHATTISGARH",
    "GUJARAT", "HARYANA_DELHI&GURGAON", "JHARKHAND", "KARNATAKA",
    "KERALA", "MADHYA PRADESH", "MAHARASHTRA - Mumbai", "MAHARASHTRA - Pune",
    "ORISSA", "PUNJAB", "RAJASTHAN", "TAMIL NADU",
    "UTTAR PRADESH_Noida", "WEST BENGAL",
]
BB_STATE_COLUMNS = BB_HEADERS[8:]

# Default city → BB state mapping (user can override in UI)
DEFAULT_CITY_MAP = {
    "Mumbai-DC":    "MAHARASHTRA - Mumbai",
    "Pune-DC":      "MAHARASHTRA - Pune",
    "Bangalore-DC":  "KARNATAKA",
    "Bangalore-DC2": "KARNATAKA",
    "Hyderabad-DC":  "TELANGANA",
    "Kolkata-DC":    "WEST BENGAL",
    "Chennai-DC":    "TAMIL NADU",
    "Ahmedabad-DC":  "GUJARAT",
    "Delhi-DC":      "HARYANA_DELHI&GURGAON",
    "Gurgaon-DC":    "HARYANA_DELHI&GURGAON",
}

TIER_LABELS = ["BAU", "SVD", "Weekend", "Liq"]
TIER_HELP = {
    "BAU":     "Business-as-usual: standard everyday price.",
    "SVD":     "Slow-mover deal: used on days 1–10 of month for overstocked / slow cities.",
    "Weekend": "Weekend push: used Sat–Sun for cities that under-index on weekdays.",
    "Liq":     "Liquidation: deepest price for cities with very thin velocity.",
}


# ──────────────────────────────────────────────────────────────
# HELPER: Compute STR & DOC from sales history
# ──────────────────────────────────────────────────────────────
def _compute_signals(bb_hist: pd.DataFrame, lookback_days: int = 30) -> pd.DataFrame:
    """
    Given BB sales history (item_name, city, date, qty_sold),
    return a DataFrame with columns:
        channel_sku (= item_name), location (= city), str, doc
    """
    if bb_hist.empty:
        return pd.DataFrame(columns=["channel_sku", "location", "str", "doc"])

    bb_hist = bb_hist.copy()
    bb_hist["date_dt"] = pd.to_datetime(bb_hist["date"], errors="coerce")
    cutoff = bb_hist["date_dt"].max() - timedelta(days=lookback_days - 1)
    bb_hist = bb_hist[bb_hist["date_dt"] >= cutoff]

    if bb_hist.empty:
        return pd.DataFrame(columns=["channel_sku", "location", "str", "doc"])

    grp = bb_hist.groupby(["item_name", "city"])

    rows = []
    for (sku, city), g in grp:
        total_days   = lookback_days
        days_w_sales = g[g["qty_sold"] > 0]["date_dt"].nunique()
        total_qty    = g["qty_sold"].sum()
        avg_daily    = total_qty / total_days if total_days > 0 else 0

        # STR: fraction of days that had any sale  (0→1)
        str_val = days_w_sales / total_days if total_days > 0 else 0

        # DOC: inverse of velocity — we express it as days-of-cover index (0–100)
        # Higher = slower moving = more cover left
        # We normalise across the dataset later; store raw avg_daily for now
        rows.append({
            "channel_sku": sku,
            "location":    city,
            "str":         round(str_val, 4),
            "_avg_daily":  avg_daily,
        })

    sig_df = pd.DataFrame(rows)
    if sig_df.empty:
        return pd.DataFrame(columns=["channel_sku", "location", "str", "doc"])

    # Normalise avg_daily → DOC index (0–100); lower velocity = higher DOC
    max_vel = sig_df["_avg_daily"].max()
    if max_vel > 0:
        sig_df["doc"] = ((1 - sig_df["_avg_daily"] / max_vel) * 100).round(1)
    else:
        sig_df["doc"] = 50.0

    sig_df.drop(columns=["_avg_daily"], inplace=True)
    return sig_df


# ──────────────────────────────────────────────────────────────
# HELPER: Classify a single row into a pricing tier
# ──────────────────────────────────────────────────────────────
def _classify(str_val: float, doc_val: float, is_svd_day: bool, is_weekend: bool,
              doc_threshold: float = 70.0) -> str:
    """
    Mirrors the logic from app(27):
        Liq     : str < 0.20 AND doc > threshold
        SVD     : str >= 0.20 AND doc > threshold   → only on SVD days (1-10)
        Weekend : str < 0.20 AND doc <= threshold   → only on weekends
        BAU     : everything else
    """
    if str_val < 0.20 and doc_val > doc_threshold:
        return "Liq"
    if str_val >= 0.20 and doc_val > doc_threshold:
        return "SVD" if is_svd_day else "BAU"
    if str_val < 0.20 and doc_val <= doc_threshold:
        return "Weekend" if is_weekend else "BAU"
    return "BAU"


# ──────────────────────────────────────────────────────────────
# MAIN RENDER FUNCTION
# ──────────────────────────────────────────────────────────────
def render_deals_promos_tab(history_df: pd.DataFrame, role: str):
    st.subheader("🏷️ Deals & Promos Generator")
    st.caption(
        "Uses channel performance signals (sell-through rate & velocity) from your "
        "historical sales data to automatically assign pricing tiers per city, then "
        "generates the channel upload file."
    )

    # ── Channel selector (only BB for now) ────────────────────────────────────
    channel = st.selectbox(
        "Select Channel",
        ["BigBasket"],
        help="More channels coming soon. BigBasket is the beta integration.",
    )

    st.divider()

    # ── Filter history to BigBasket only ─────────────────────────────────────
    BB_CHANNEL_NAMES = {"BigBasket", "Big Basket", "bigbasket", "big basket"}
    bb_hist = history_df[history_df["channel"].str.strip().isin(BB_CHANNEL_NAMES)].copy()

    if bb_hist.empty:
        st.warning(
            "No BigBasket sales data found. Upload BigBasket data via the Smart Upload tab first. "
            "Make sure the channel name matches 'BigBasket' or 'Big Basket'."
        )
        return

    # Check city column exists and is populated
    if "city" not in bb_hist.columns or bb_hist["city"].isna().all():
        st.warning(
            "No city-level data found for BigBasket. "
            "Re-upload BigBasket data with the City Column mapped (DC column) in Smart Upload."
        )
        return

    bb_hist = bb_hist.dropna(subset=["city"])

    # ── Compute performance signals ───────────────────────────────────────────
    col_lkb, col_doc_thresh = st.columns(2)
    with col_lkb:
        lookback = st.slider(
            "Lookback Window (days)",
            min_value=7, max_value=90, value=30, step=7,
            help="How many recent days of sales data to use for computing STR and velocity signals.",
        )
    with col_doc_thresh:
        doc_thresh = st.slider(
            "DOC Threshold for Slow-Mover Classification",
            min_value=30, max_value=90, value=70, step=5,
            help=(
                "Cities scoring above this DOC index are flagged as slow-movers. "
                "DOC index = 0 (fastest) → 100 (slowest). Default 70 keeps the top 30% slowest."
            ),
        )

    sig_df = _compute_signals(bb_hist, lookback_days=lookback)

    if sig_df.empty:
        st.error("Could not compute signals — check that BigBasket data has qty_sold and city columns.")
        return

    # ── Show computed signals ─────────────────────────────────────────────────
    with st.expander("📊 View Channel Performance Signals (STR & DOC per SKU × City)", expanded=False):
        st.caption(
            "**STR** = fraction of days with at least one sale in the lookback window. "
            "**DOC Index** = 0 (fastest velocity) → 100 (slowest / most cover)."
        )
        display_sig = sig_df.copy()
        display_sig["Tier (if SVD day)"] = display_sig.apply(
            lambda r: _classify(r["str"], r["doc"], True, False, doc_thresh), axis=1
        )
        display_sig["Tier (if Weekend)"] = display_sig.apply(
            lambda r: _classify(r["str"], r["doc"], False, True, doc_thresh), axis=1
        )
        display_sig.rename(columns={"channel_sku": "SKU", "location": "City",
                                     "str": "STR", "doc": "DOC Index"}, inplace=True)
        st.dataframe(
            display_sig.style.format({"STR": "{:.2%}", "DOC Index": "{:.1f}"}),
            hide_index=True, use_container_width=True,
        )

    st.divider()

    # ── Step 1: City → BB State Mapping ──────────────────────────────────────
    st.subheader("Step 1 — Map Cities to BigBasket State Columns")
    unique_cities = sorted(sig_df["location"].dropna().unique())

    map_data = []
    for city in unique_cities:
        map_data.append({
            "City (from sales data)": city,
            "BB State Column": DEFAULT_CITY_MAP.get(city, BB_STATE_COLUMNS[0]),
        })

    edited_map_df = st.data_editor(
        pd.DataFrame(map_data),
        column_config={
            "BB State Column": st.column_config.SelectboxColumn(
                "BB State Column",
                options=BB_STATE_COLUMNS,
                required=True,
            )
        },
        num_rows="fixed",
        hide_index=True,
        key="bb_city_map",
    )
    city_to_state = dict(
        zip(edited_map_df["City (from sales data)"], edited_map_df["BB State Column"])
    )

    st.divider()

    # ── Step 2: Month/Year selection ──────────────────────────────────────────
    st.subheader("Step 2 — Select Promo Month")
    cy1, cy2 = st.columns(2)
    with cy1:
        promo_year = st.selectbox("Year", [2025, 2026, 2027], index=1, key="promo_year")
    with cy2:
        promo_month = st.selectbox(
            "Month", list(range(1, 13)),
            format_func=lambda x: calendar.month_name[x],
            key="promo_month",
        )

    st.divider()

    # ── Step 3: SKU-level target prices ───────────────────────────────────────
    st.subheader("Step 3 — Enter Target Prices per SKU × Tier")
    st.caption(
        "Enter the **fixed price (₹)** you want BigBasket to charge for each tier. "
        "Leave as 0 to skip that tier. Tiers are auto-assigned per city based on "
        "performance signals above."
    )

    unique_skus = sorted(sig_df["channel_sku"].unique())

    # Header row
    h_cols = st.columns([2, 1, 1, 1, 1])
    h_cols[0].markdown("**SKU**")
    for i, tier in enumerate(TIER_LABELS):
        h_cols[i + 1].markdown(f"**{tier}**", help=TIER_HELP[tier])

    sku_prices: dict[str, dict[str, float]] = {}
    for sku in unique_skus:
        row_cols = st.columns([2, 1, 1, 1, 1])
        row_cols[0].write(sku)
        prices = {}
        for i, tier in enumerate(TIER_LABELS):
            prices[tier] = row_cols[i + 1].number_input(
                tier, key=f"price_{sku}_{tier}", label_visibility="collapsed",
                min_value=0.0, step=0.5, format="%.2f",
            )
        sku_prices[sku] = prices

    st.divider()

    # ── Step 4: Generate ──────────────────────────────────────────────────────
    if st.button("🚀 Generate BigBasket Promo File", type="primary"):
        num_days = calendar.monthrange(promo_year, promo_month)[1]
        start_dt = date(promo_year, promo_month, 1)
        end_dt   = date(promo_year, promo_month, num_days)

        # Build a lookup: (sku, city) → {str, doc}
        sig_lookup = {
            (row["channel_sku"], row["location"]): row
            for _, row in sig_df.iterrows()
        }

        final_rows = []

        for sku in unique_skus:
            # Get all cities for this SKU
            sku_cities = sig_df[sig_df["channel_sku"] == sku]["location"].unique()

            # Day-by-day price+states build
            day_by_day = []
            curr = start_dt
            while curr <= end_dt:
                is_svd_day = curr.day <= 10
                is_weekend = curr.weekday() >= 5  # Saturday=5, Sunday=6

                # For each tier, collect which states are active today
                tier_states: dict[str, list] = {t: [] for t in TIER_LABELS}

                for city in sku_cities:
                    key = (sku, city)
                    if key not in sig_lookup:
                        continue
                    sig_row = sig_lookup[key]
                    tier = _classify(
                        sig_row["str"], sig_row["doc"],
                        is_svd_day, is_weekend, doc_thresh,
                    )
                    bb_state = city_to_state.get(city)
                    if bb_state:
                        tier_states[tier].append(bb_state)

                for tier, states in tier_states.items():
                    if states and sku_prices[sku][tier] > 0:
                        day_by_day.append({
                            "Date":   curr,
                            "Tier":   tier,
                            "Price":  sku_prices[sku][tier],
                            "States": sorted(set(states)),
                        })

                curr += timedelta(days=1)

            # Consolidate consecutive days with same price+states into date ranges
            if not day_by_day:
                continue

            # Group by tier first, then consolidate within each tier
            from itertools import groupby

            tier_days: dict[str, list] = {}
            for entry in day_by_day:
                tier_days.setdefault(entry["Tier"], []).append(entry)

            for tier, entries in tier_days.items():
                # Sort by date
                entries = sorted(entries, key=lambda x: x["Date"])
                grouped = []
                seg_start = entries[0]
                prev = entries[0]

                for i in range(1, len(entries)):
                    nxt = entries[i]
                    # Break if price/states change or dates are not consecutive
                    if (
                        nxt["Price"]  != prev["Price"]  or
                        nxt["States"] != prev["States"] or
                        nxt["Date"]   != prev["Date"] + timedelta(days=1)
                    ):
                        grouped.append({
                            "s": seg_start["Date"], "e": prev["Date"],
                            "p": prev["Price"],     "st": prev["States"],
                        })
                        seg_start = nxt
                    prev = nxt

                grouped.append({
                    "s": seg_start["Date"], "e": entries[-1]["Date"],
                    "p": entries[-1]["Price"],  "st": entries[-1]["States"],
                })

                for g in grouped:
                    out_row = {col: "" for col in BB_HEADERS}
                    out_row["Code"]                         = sku
                    out_row["Product Description"]          = sku
                    out_row["Start Date (DD-MM-YYYY)"]      = g["s"].strftime("%d-%m-%Y")
                    out_row["End Date (DD-MM-YYYY)"]        = g["e"].strftime("%d-%m-%Y")
                    out_row["Discount Type"]                = "fixed"
                    out_row["Discount Value"]               = g["p"]
                    out_row["Redemption Limit - Qty Per Campaign"] = ""
                    out_row["Pan India"]                    = "No"
                    for state in g["st"]:
                        if state in out_row:
                            out_row[state] = "Yes"
                    final_rows.append(out_row)

        if not final_rows:
            st.warning(
                "No promo rows generated. Make sure you've entered at least one non-zero price "
                "and that the signals qualify cities into a pricing tier."
            )
            return

        output_df = pd.DataFrame(final_rows, columns=BB_HEADERS)

        st.success(f"✅ Generated **{len(output_df)}** promo lines for BigBasket!")

        # Summary
        summary = output_df.groupby("Code")["Discount Value"].value_counts().reset_index()
        with st.expander("📋 Promo Summary", expanded=True):
            st.dataframe(output_df, hide_index=True, use_container_width=True)

        # Download
        csv_bytes = output_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download BigBasket Promo CSV",
            data=csv_bytes,
            file_name=f"BB_Promo_{calendar.month_abbr[promo_month]}_{promo_year}.csv",
            mime="text/csv",
            type="primary",
        )
