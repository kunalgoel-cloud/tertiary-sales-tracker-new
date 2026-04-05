"""
deals_promos_module.py
──────────────────────
Deals & Promos tab for the Mamanourish Executive Tracker.

Supported channels:
  • BigBasket  (beta) — generates a CSV with date-range rows and state columns
  • Amazon            — fills SVD PRICE and BAU PRICE into the deal sheet template (.xlsx)

Shared signal logic (both channels):
  STR (sell-through rate) = fraction of days in lookback window that had ≥1 sale
  DOC Index (0–100)       = inverted velocity; 0 = fastest mover, 100 = slowest

  Classification per SKU:
    Liq      : STR < 0.20 AND DOC > threshold   → liquidation pricing
    SVD      : STR ≥ 0.20 AND DOC > threshold   → slow-mover deal (days 1-10 of month)
    Weekend  : STR < 0.20 AND DOC ≤ threshold   → weekend push
    BAU      : everything else

Amazon-specific:
  Amazon is a national channel (no city breakdown needed).
  Signals are aggregated across all cities for each SKU.
  SVD PRICE  → price for slow-mover / SVD tier
  BAU PRICE  → standard everyday price
  Output is the uploaded deal sheet template with M & N columns filled in,
  preserving all original formatting and formulas.
"""

import streamlit as st
import pandas as pd
import calendar
import numpy as np
from datetime import date, timedelta
import io

from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font

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

DEFAULT_CITY_MAP = {
    "Mumbai-DC":     "MAHARASHTRA - Mumbai",
    "Pune-DC":       "MAHARASHTRA - Pune",
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
    "SVD":     "Slow-mover deal: used on days 1–10 of month for overstocked / slow SKUs.",
    "Weekend": "Weekend push: used Sat–Sun for SKUs that under-index on weekdays.",
    "Liq":     "Liquidation: deepest price for very slow-moving SKUs.",
}

# Amazon deal sheet column indices (1-based openpyxl convention)
AMZN_COL_SVD        = 13   # M — SVD PRICE
AMZN_COL_BAU        = 14   # N — BAU PRICE
AMZN_DATA_START_ROW = 2    # row 1 is the header


# ──────────────────────────────────────────────────────────────
# SHARED SIGNAL HELPERS
# ──────────────────────────────────────────────────────────────

def _compute_signals_with_city(chan_hist: pd.DataFrame, lookback_days: int = 30) -> pd.DataFrame:
    """STR & DOC per SKU × city (BigBasket)."""
    if chan_hist.empty:
        return pd.DataFrame(columns=["channel_sku", "location", "str", "doc"])

    h = chan_hist.copy()
    h["date_dt"] = pd.to_datetime(h["date"], errors="coerce")
    cutoff = h["date_dt"].max() - timedelta(days=lookback_days - 1)
    h = h[h["date_dt"] >= cutoff]
    if h.empty:
        return pd.DataFrame(columns=["channel_sku", "location", "str", "doc"])

    rows = []
    for (sku, city), g in h.groupby(["item_name", "city"]):
        days_w_sales = g[g["qty_sold"] > 0]["date_dt"].nunique()
        total_qty    = g["qty_sold"].sum()
        rows.append({
            "channel_sku": sku, "location": city,
            "str": round(days_w_sales / lookback_days, 4),
            "_avg_daily": total_qty / lookback_days,
        })

    sig = pd.DataFrame(rows)
    max_vel = sig["_avg_daily"].max()
    sig["doc"] = ((1 - sig["_avg_daily"] / max_vel) * 100).round(1) if max_vel > 0 else 50.0
    return sig.drop(columns=["_avg_daily"])


def _compute_signals_national(chan_hist: pd.DataFrame, lookback_days: int = 30) -> pd.DataFrame:
    """STR & DOC per SKU aggregated nationally (Amazon)."""
    if chan_hist.empty:
        return pd.DataFrame(columns=["channel_sku", "str", "doc"])

    h = chan_hist.copy()
    h["date_dt"] = pd.to_datetime(h["date"], errors="coerce")
    cutoff = h["date_dt"].max() - timedelta(days=lookback_days - 1)
    h = h[h["date_dt"] >= cutoff]
    if h.empty:
        return pd.DataFrame(columns=["channel_sku", "str", "doc"])

    rows = []
    for sku, g in h.groupby("item_name"):
        days_w_sales = g[g["qty_sold"] > 0]["date_dt"].nunique()
        total_qty    = g["qty_sold"].sum()
        rows.append({
            "channel_sku": sku,
            "str": round(days_w_sales / lookback_days, 4),
            "_avg_daily": total_qty / lookback_days,
        })

    sig = pd.DataFrame(rows)
    max_vel = sig["_avg_daily"].max()
    sig["doc"] = ((1 - sig["_avg_daily"] / max_vel) * 100).round(1) if max_vel > 0 else 50.0
    return sig.drop(columns=["_avg_daily"])


def _classify(str_val: float, doc_val: float, is_svd_day: bool, is_weekend: bool,
              doc_threshold: float = 70.0) -> str:
    if str_val < 0.20 and doc_val > doc_threshold:
        return "Liq"
    if str_val >= 0.20 and doc_val > doc_threshold:
        return "SVD" if is_svd_day else "BAU"
    if str_val < 0.20 and doc_val <= doc_threshold:
        return "Weekend" if is_weekend else "BAU"
    return "BAU"


def _signals_expander(sig_df: pd.DataFrame, doc_thresh: float, has_city: bool = True):
    with st.expander("📊 View Performance Signals (STR & DOC)", expanded=False):
        st.caption(
            "**STR** = fraction of days with at least one sale in the lookback window. "
            "**DOC Index** = 0 (fastest mover) → 100 (slowest). "
            "These drive the tier suggestion for each SKU."
        )
        display = sig_df.copy()
        display["Suggested Tier"] = display.apply(
            lambda r: _classify(r["str"], r["doc"], True, False, doc_thresh), axis=1
        )
        rename = {"channel_sku": "SKU", "str": "STR", "doc": "DOC Index"}
        if has_city:
            rename["location"] = "City"
        display.rename(columns=rename, inplace=True)
        st.dataframe(
            display.style.format({"STR": "{:.2%}", "DOC Index": "{:.1f}"}),
            hide_index=True, use_container_width=True,
        )


# ──────────────────────────────────────────────────────────────
# BIGBASKET SECTION
# ──────────────────────────────────────────────────────────────

def _render_bigbasket(history_df: pd.DataFrame):
    st.markdown("### 🛒 BigBasket Promo Generator")
    st.caption(
        "Generates a date-range CSV with state columns for BigBasket bulk upload. "
        "Each city is classified into a pricing tier based on sell-through rate and velocity."
    )

    BB_CHANNEL_NAMES = {"BigBasket", "Big Basket", "bigbasket", "big basket"}
    bb_hist = history_df[history_df["channel"].str.strip().isin(BB_CHANNEL_NAMES)].copy()

    if bb_hist.empty:
        st.warning("No BigBasket sales data found. Upload BigBasket data via Smart Upload first.")
        return

    if "city" not in bb_hist.columns or bb_hist["city"].isna().all():
        st.warning(
            "No city-level data found for BigBasket. "
            "Re-upload BigBasket data with the City/DC column mapped in Smart Upload."
        )
        return

    bb_hist = bb_hist.dropna(subset=["city"])

    # ── Signal controls ───────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        lookback = st.slider("Lookback Window (days)", 7, 90, 30, 7, key="bb_lookback",
                             help="How many recent days of data to use for STR and velocity.")
    with c2:
        doc_thresh = st.slider("DOC Threshold", 30, 90, 70, 5, key="bb_doc_thresh",
                               help="Cities with DOC above this are treated as slow-movers.")

    sig_df = _compute_signals_with_city(bb_hist, lookback)
    if sig_df.empty:
        st.error("Could not compute signals — check that BigBasket data has qty_sold and city.")
        return

    _signals_expander(sig_df, doc_thresh, has_city=True)
    st.divider()

    # ── Step 1: City → BB State Mapping ──────────────────────────────────────
    st.markdown("#### Step 1 — Map Cities to BigBasket State Columns")
    unique_cities = sorted(sig_df["location"].dropna().unique())
    map_data = [{"City (from sales data)": c,
                 "BB State Column": DEFAULT_CITY_MAP.get(c, BB_STATE_COLUMNS[0])}
                for c in unique_cities]

    edited_map_df = st.data_editor(
        pd.DataFrame(map_data),
        column_config={"BB State Column": st.column_config.SelectboxColumn(
            "BB State Column", options=BB_STATE_COLUMNS, required=True)},
        num_rows="fixed", hide_index=True, key="bb_city_map",
    )
    city_to_state = dict(zip(edited_map_df["City (from sales data)"],
                             edited_map_df["BB State Column"]))
    st.divider()

    # ── Step 2: Promo month ───────────────────────────────────────────────────
    st.markdown("#### Step 2 — Select Promo Month")
    cy1, cy2 = st.columns(2)
    with cy1:
        promo_year = st.selectbox("Year", [2025, 2026, 2027], index=1, key="bb_year")
    with cy2:
        promo_month = st.selectbox("Month", list(range(1, 13)),
                                   format_func=lambda x: calendar.month_name[x],
                                   key="bb_month")
    st.divider()

    # ── Step 3: Prices ────────────────────────────────────────────────────────
    st.markdown("#### Step 3 — Enter Target Prices per SKU × Tier")
    st.caption("Enter the fixed price (₹) per tier. Leave 0 to skip that tier.")

    unique_skus = sorted(sig_df["channel_sku"].unique())
    h_cols = st.columns([2, 1, 1, 1, 1])
    h_cols[0].markdown("**SKU**")
    for i, tier in enumerate(TIER_LABELS):
        h_cols[i + 1].markdown(f"**{tier}**", help=TIER_HELP[tier])

    sku_prices: dict = {}
    for sku in unique_skus:
        rc = st.columns([2, 1, 1, 1, 1])
        rc[0].write(sku)
        sku_prices[sku] = {
            tier: rc[i + 1].number_input(
                tier, key=f"bb_price_{sku}_{tier}", label_visibility="collapsed",
                min_value=0.0, step=0.5, format="%.2f")
            for i, tier in enumerate(TIER_LABELS)
        }
    st.divider()

    # ── Generate ──────────────────────────────────────────────────────────────
    if st.button("🚀 Generate BigBasket Promo File", type="primary", key="bb_generate"):
        num_days = calendar.monthrange(promo_year, promo_month)[1]
        start_dt = date(promo_year, promo_month, 1)
        end_dt   = date(promo_year, promo_month, num_days)

        sig_lookup = {(r["channel_sku"], r["location"]): r for _, r in sig_df.iterrows()}
        final_rows = []

        for sku in unique_skus:
            sku_cities = sig_df[sig_df["channel_sku"] == sku]["location"].unique()
            day_by_day = []
            curr = start_dt

            while curr <= end_dt:
                is_svd  = curr.day <= 10
                is_wknd = curr.weekday() >= 5
                tier_states: dict = {t: [] for t in TIER_LABELS}

                for city in sku_cities:
                    key = (sku, city)
                    if key not in sig_lookup:
                        continue
                    r = sig_lookup[key]
                    tier = _classify(r["str"], r["doc"], is_svd, is_wknd, doc_thresh)
                    state = city_to_state.get(city)
                    if state:
                        tier_states[tier].append(state)

                for tier, states in tier_states.items():
                    if states and sku_prices[sku][tier] > 0:
                        day_by_day.append({
                            "Date": curr, "Tier": tier,
                            "Price": sku_prices[sku][tier],
                            "States": sorted(set(states)),
                        })
                curr += timedelta(days=1)

            if not day_by_day:
                continue

            tier_days: dict = {}
            for entry in day_by_day:
                tier_days.setdefault(entry["Tier"], []).append(entry)

            for tier, entries in tier_days.items():
                entries = sorted(entries, key=lambda x: x["Date"])
                grouped, seg_start, prev = [], entries[0], entries[0]

                for i in range(1, len(entries)):
                    nxt = entries[i]
                    if (nxt["Price"] != prev["Price"] or
                            nxt["States"] != prev["States"] or
                            nxt["Date"] != prev["Date"] + timedelta(days=1)):
                        grouped.append({"s": seg_start["Date"], "e": prev["Date"],
                                        "p": prev["Price"], "st": prev["States"]})
                        seg_start = nxt
                    prev = nxt
                grouped.append({"s": seg_start["Date"], "e": entries[-1]["Date"],
                                "p": entries[-1]["Price"], "st": entries[-1]["States"]})

                for g in grouped:
                    out = {col: "" for col in BB_HEADERS}
                    out.update({
                        "Code": sku, "Product Description": sku,
                        "Start Date (DD-MM-YYYY)": g["s"].strftime("%d-%m-%Y"),
                        "End Date (DD-MM-YYYY)":   g["e"].strftime("%d-%m-%Y"),
                        "Discount Type": "fixed", "Discount Value": g["p"], "Pan India": "No",
                    })
                    for s in g["st"]:
                        if s in out:
                            out[s] = "Yes"
                    final_rows.append(out)

        if not final_rows:
            st.warning("No promo rows generated. Check that prices are non-zero and signals "
                       "qualify cities into a tier.")
            return

        output_df = pd.DataFrame(final_rows, columns=BB_HEADERS)
        st.success(f"✅ Generated **{len(output_df)}** promo lines for BigBasket!")

        with st.expander("📋 Preview", expanded=True):
            st.dataframe(output_df, hide_index=True, use_container_width=True)

        csv_bytes = output_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download BigBasket Promo CSV",
            data=csv_bytes,
            file_name=f"BB_Promo_{calendar.month_abbr[promo_month]}_{promo_year}.csv",
            mime="text/csv", type="primary",
        )


# ──────────────────────────────────────────────────────────────
# AMAZON SECTION
# ──────────────────────────────────────────────────────────────

def _render_amazon(history_df: pd.DataFrame):
    st.markdown("### 📦 Amazon Deal Sheet Filler")
    st.caption(
        "Upload your Amazon deal sheet template (.xlsx). "
        "SVD PRICE and BAU PRICE columns are auto-filled using sales performance signals, "
        "with all original formatting, formulas, and data preserved exactly."
    )

    AMZN_CHANNEL_NAMES = {"Amazon", "amazon", "Amazon.in", "amazon.in"}

    # ── Step 1: Upload template ───────────────────────────────────────────────
    st.markdown("#### Step 1 — Upload Amazon Deal Sheet Template")
    deal_file = st.file_uploader(
        "Upload deal sheet (.xlsx)", type=["xlsx"], key="amzn_template",
        help="The standard RK WORLD deal sheet with ASIN, Title, MRP, ASP, Margin columns."
    )

    if not deal_file:
        st.info("Upload your deal sheet template (.xlsx) to continue.")
        return

    try:
        template_bytes = deal_file.read()
        wb_check = load_workbook(io.BytesIO(template_bytes))
        ws_check = wb_check.active
    except Exception as e:
        st.error(f"Could not read deal sheet: {e}")
        return

    # Read header + data rows into a DataFrame for display / mapping
    header_row = [ws_check.cell(1, c).value for c in range(1, ws_check.max_column + 1)]
    data_rows  = []
    for row in ws_check.iter_rows(min_row=AMZN_DATA_START_ROW, values_only=True):
        if any(v is not None for v in row):
            data_rows.append(dict(zip(header_row, row)))

    if not data_rows:
        st.error("No data rows found in the deal sheet.")
        return

    template_df = pd.DataFrame(data_rows).dropna(subset=["ASIN"])
    st.success(f"Loaded **{len(template_df)}** ASINs from template.")

    with st.expander("Preview uploaded template", expanded=False):
        cols_to_show = [c for c in ["ASIN", "Title", "MRP", "ASP", "Margin",
                                    "SVD PRICE", "BAU PRICE"] if c in template_df.columns]
        st.dataframe(template_df[cols_to_show], hide_index=True, use_container_width=True)

    st.divider()

    # ── Step 2: Signal controls ───────────────────────────────────────────────
    st.markdown("#### Step 2 — Configure Performance Signals")

    amzn_hist     = history_df[history_df["channel"].str.strip().isin(AMZN_CHANNEL_NAMES)].copy()
    has_amzn_data = not amzn_hist.empty

    if not has_amzn_data:
        st.warning(
            "No Amazon sales history found in the database. "
            "Prices can still be entered manually — signals will not be auto-suggested."
        )

    c1, c2 = st.columns(2)
    with c1:
        lookback = st.slider("Lookback Window (days)", 7, 90, 30, 7, key="amzn_lookback",
                             help="Recent days of sales used for STR and velocity signals.")
    with c2:
        doc_thresh = st.slider("DOC Threshold", 30, 90, 70, 5, key="amzn_doc_thresh",
                               help="SKUs with DOC index above this are flagged as slow-movers.")

    sig_df = pd.DataFrame(columns=["channel_sku", "str", "doc"])
    if has_amzn_data:
        sig_df = _compute_signals_national(amzn_hist, lookback)
        if not sig_df.empty:
            _signals_expander(sig_df, doc_thresh, has_city=False)

    sig_lookup = {r["channel_sku"]: r for _, r in sig_df.iterrows()} if not sig_df.empty else {}

    st.divider()

    # ── Step 3: Map ASINs → Master SKUs ──────────────────────────────────────
    st.markdown("#### Step 3 — Map Deal Sheet Rows to Master SKUs")
    st.caption(
        "Match each ASIN to the master SKU name used in your sales database. "
        "This links performance signals to the correct deal sheet row."
    )

    master_skus_in_history = (
        sorted(history_df["item_name"].dropna().unique()) if not history_df.empty else []
    )
    sku_options = ["— skip this row —"] + master_skus_in_history

    asin_to_master: dict = {}
    for _, trow in template_df.iterrows():
        asin  = str(trow.get("ASIN", "")).strip()
        title = str(trow.get("Title", ""))[:80]

        # Auto-suggest: find a master SKU whose name appears anywhere in the title
        auto_match = next(
            (s for s in master_skus_in_history if s.lower() in title.lower()), None
        )
        default_idx = sku_options.index(auto_match) if auto_match else 0

        asin_to_master[asin] = st.selectbox(
            f"`{asin}` — {title[:70]}…",
            sku_options,
            index=default_idx,
            key=f"amzn_map_{asin}",
        )

    st.divider()

    # ── Step 4: Review & confirm prices ──────────────────────────────────────
    st.markdown("#### Step 4 — Review & Confirm Prices")
    st.caption(
        "Prices are auto-suggested from performance signals. "
        "🟡 SVD = slow-mover deal price (days 1–10) · 🟢 BAU = everyday price. "
        "Override any value before generating."
    )

    TIER_COLOUR = {"SVD": "🟡", "Liq": "🔴", "BAU": "🟢", "Weekend": "🔵"}
    SVD_DISCOUNT = 0.08   # default 8% below ASP for SVD tier

    # Header
    h = st.columns([1.2, 2.8, 0.8, 0.8, 1, 1])
    for lbl, col in zip(["ASIN", "Title", "ASP", "Tier", "SVD Price ✏️", "BAU Price ✏️"], h):
        col.markdown(f"**{lbl}**")

    final_prices: dict = {}

    for _, trow in template_df.iterrows():
        asin   = str(trow.get("ASIN", "")).strip()
        title  = str(trow.get("Title", ""))[:65]
        asp    = float(trow.get("ASP") or 0)
        master = asin_to_master.get(asin, "— skip this row —")

        # Determine tier from signals if available
        tier = "BAU"
        if master != "— skip this row —" and master in sig_lookup:
            sig  = sig_lookup[master]
            tier = _classify(sig["str"], sig["doc"], is_svd_day=True,
                             is_weekend=False, doc_threshold=doc_thresh)

        # Suggest prices
        # SVD tier / Liq → apply discount; BAU / Weekend → ASP as-is
        svd_suggest = round(asp * (1 - SVD_DISCOUNT)) if tier in ("SVD", "Liq") else asp
        bau_suggest = asp

        row_cols = st.columns([1.2, 2.8, 0.8, 0.8, 1, 1])
        row_cols[0].caption(asin)
        row_cols[1].caption(title + "…")
        row_cols[2].write(f"₹{asp:.0f}")
        row_cols[3].write(f"{TIER_COLOUR.get(tier, '')} {tier}")

        svd_price = row_cols[4].number_input(
            "SVD", key=f"amzn_svd_{asin}", value=float(svd_suggest),
            min_value=0.0, step=1.0, format="%.0f", label_visibility="collapsed",
        )
        bau_price = row_cols[5].number_input(
            "BAU", key=f"amzn_bau_{asin}", value=float(bau_suggest),
            min_value=0.0, step=1.0, format="%.0f", label_visibility="collapsed",
        )
        final_prices[asin] = {"svd": svd_price, "bau": bau_price, "master": master}

    st.divider()

    # ── Generate filled xlsx ──────────────────────────────────────────────────
    if st.button("🚀 Generate Filled Amazon Deal Sheet", type="primary", key="amzn_generate"):

        # Reload workbook fresh from original bytes to preserve all formatting & formulas
        wb_out = load_workbook(io.BytesIO(template_bytes))
        ws_out = wb_out.active

        blue_font = Font(color="0000FF")   # Blue = hardcoded input (industry convention)

        filled_count = 0
        skipped_asins = []

        for row_idx in range(AMZN_DATA_START_ROW, ws_out.max_row + 1):
            asin_val = str(ws_out.cell(row_idx, 1).value or "").strip()

            if not asin_val or asin_val not in final_prices:
                continue

            prices = final_prices[asin_val]

            if prices["master"] == "— skip this row —":
                skipped_asins.append(asin_val)
                continue

            # Write SVD PRICE → col M (13)
            svd_cell = ws_out.cell(row_idx, AMZN_COL_SVD)
            svd_cell.value        = prices["svd"]
            svd_cell.font         = blue_font
            svd_cell.number_format = "#,##0"

            # Write BAU PRICE → col N (14)
            bau_cell = ws_out.cell(row_idx, AMZN_COL_BAU)
            bau_cell.value        = prices["bau"]
            bau_cell.font         = blue_font
            bau_cell.number_format = "#,##0"

            filled_count += 1

        if filled_count == 0:
            st.warning(
                "No rows were filled. Map at least one ASIN to a master SKU and try again."
            )
            return

        # Save to in-memory buffer
        out_buf = io.BytesIO()
        wb_out.save(out_buf)
        out_buf.seek(0)

        st.success(f"✅ Filled prices for **{filled_count}** ASINs.")
        if skipped_asins:
            st.info(f"Skipped {len(skipped_asins)} unmapped rows: {', '.join(skipped_asins)}")

        # Preview table
        preview_rows = []
        for _, trow in template_df.iterrows():
            asin = str(trow.get("ASIN", "")).strip()
            if asin in final_prices and final_prices[asin]["master"] != "— skip this row —":
                preview_rows.append({
                    "ASIN":           asin,
                    "Title":          str(trow.get("Title", ""))[:70],
                    "ASP (₹)":        trow.get("ASP"),
                    "Mapped SKU":     final_prices[asin]["master"],
                    "SVD PRICE ✏️":  final_prices[asin]["svd"],
                    "BAU PRICE ✏️":  final_prices[asin]["bau"],
                })

        with st.expander("📋 Filled Price Preview", expanded=True):
            st.dataframe(pd.DataFrame(preview_rows), hide_index=True, use_container_width=True)

        st.download_button(
            label="⬇️ Download Filled Amazon Deal Sheet (.xlsx)",
            data=out_buf,
            file_name=f"Amazon_DealSheet_Filled_{date.today().strftime('%b%Y')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )


# ──────────────────────────────────────────────────────────────
# MAIN RENDER FUNCTION  (called from app.py)
# ──────────────────────────────────────────────────────────────

def render_deals_promos_tab(history_df: pd.DataFrame, role: str):
    st.subheader("🏷️ Deals & Promos Generator")
    st.caption(
        "Uses channel performance signals (sell-through rate & velocity) from your "
        "historical sales data to auto-suggest pricing tiers, then generates the "
        "channel-specific upload or deal file."
    )

    channel = st.selectbox(
        "Select Channel",
        ["BigBasket", "Amazon"],
        help="BigBasket generates a date-range CSV. Amazon fills your deal sheet template.",
        key="deals_channel_select",
    )

    st.divider()

    if channel == "BigBasket":
        _render_bigbasket(history_df)
    elif channel == "Amazon":
        _render_amazon(history_df)
