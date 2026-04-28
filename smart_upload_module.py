"""
smart_upload_module.py
────────────────────────────────────────────────────────────────────────────
Auto-detecting Smart Upload for the Mamanourish Sales Tracker.

FLOW
  1. User drops one or more sales files (any channel, all at once).
  2. Channel auto-detected from filename keywords → column fingerprints
     → saved Supabase templates (in that priority order).
  3. All columns pre-filled; user only adjusts overrides if needed.
  4. Only *new* SKUs (not yet in item_map) are surfaced for mapping.
     Previously mapped SKUs are pre-filled automatically.
  5. Single "Sync All" button uploads every file sequentially.

LEARNING NEW CHANNELS
  First upload of an unknown channel → user confirms channel + columns →
  system saves template to `channel_upload_templates` in Supabase →
  every subsequent upload is fully automatic.

KNOWN CHANNELS (built-in schemas, zero config needed)
  Big Basket · Swiggy · Blinkit · Amazon Seller · Amazon RKW · Shopify
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import pandas as pd
import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChannelSchema:
    """Describes how to read and parse one channel's sales export."""
    channel_name:        str
    filename_signals:    list[str]          # Lowercase substrings to match in filename
    col_signals:         list[str]          # Lowercase substrings to match in column names
    col_product:         Optional[str]      # Column for product / SKU name
    col_product2:        Optional[str]      # Optional 2nd column to append (e.g. variant)
    col_channel_sku:     Optional[str]      # Column for channel-side SKU ID
    col_qty:             Optional[str]      # Column for quantity sold
    col_revenue:         Optional[str]      # Column for revenue
    col_date:            Optional[str]      # Column for date  (None → manual entry)
    col_city:            Optional[str]      # Column for city  (None → national/None)
    date_in_file:        bool = True
    city_in_file:        bool = True
    skiprows:            int  = 0           # Header rows to skip before real header
    sheet_name:          Optional[str] = None  # Excel sheet (None = first sheet)
    date_parse_fn:       Optional[str] = None  # "range_start" | "standard"
    revenue_strip_symbol: Optional[str] = None # Symbol to strip (e.g. "₹")
    filter_col:          Optional[str] = None  # Column to filter rows on
    filter_value:        Optional[str] = None  # Keep rows where filter_col == this
    filter_min_qty:      bool = False          # Drop rows where qty ≤ 0


# ─────────────────────────────────────────────────────────────────────────────
# BUILT-IN CHANNEL SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

KNOWN_SCHEMAS: dict[str, ChannelSchema] = {

    "Big Basket": ChannelSchema(
        channel_name     = "Big Basket",
        filename_signals = ["analytics_manufacturer_sales-report", "bigbasket", "big_basket"],
        col_signals      = ["source_sku_id", "sku_description", "source_city_name",
                            "total_quantity", "date_range", "total_sales"],
        col_product      = "sku_description",
        col_product2     = None,
        col_channel_sku  = "source_sku_id",
        col_qty          = "total_quantity",
        col_revenue      = "total_sales",
        col_date         = "date_range",
        col_city         = "source_city_name",
        date_in_file     = True,
        city_in_file     = True,
        date_parse_fn    = "range_start",   # "20260427 - 20260427" → 2026-04-27
    ),

    "Swiggy": ChannelSchema(
        channel_name     = "Swiggy",
        filename_signals = ["swiggy"],
        col_signals      = ["ordered_date", "item_code", "combo", "combo_units_sold",
                            "area_name", "gmv", "units_sold"],
        col_product      = "PRODUCT_NAME",
        col_product2     = "VARIANT",       # Concatenated: PRODUCT_NAME + " " + VARIANT
        col_channel_sku  = "ITEM_CODE",
        col_qty          = "UNITS_SOLD",
        col_revenue      = "GMV",
        col_date         = "ORDERED_DATE",
        col_city         = "CITY",
        date_in_file     = True,
        city_in_file     = True,
        sheet_name       = "Sales Report",
        date_parse_fn    = "standard",
    ),

    "Blinkit": ChannelSchema(
        channel_name     = "Blinkit",
        filename_signals = ["blinkit"],
        col_signals      = ["supply city", "customer city", "selling price (rs)",
                            "total gross bill amount", "item id", "order status"],
        col_product      = "Product Name",
        col_product2     = None,
        col_channel_sku  = "Item Id",
        col_qty          = "Quantity",
        col_revenue      = "Total Gross Bill Amount",
        col_date         = "Order Date",
        col_city         = "Customer City",
        date_in_file     = True,
        city_in_file     = True,
        date_parse_fn    = "standard",
        filter_col       = "Order Status",
        filter_value     = "DELIVERED",
    ),

    "Amazon Seller": ChannelSchema(
        channel_name      = "Amazon Seller",
        filename_signals  = ["businessreport"],
        col_signals       = ["(parent) asin", "(child) asin",
                             "units ordered", "ordered product sales"],
        col_product       = "Title",
        col_product2      = None,
        col_channel_sku   = "(Child) ASIN",
        col_qty           = "Units Ordered",
        col_revenue       = "Ordered Product Sales",
        col_date          = None,           # Manual date — no date column
        col_city          = None,           # National — no city column
        date_in_file      = False,
        city_in_file      = False,
        revenue_strip_symbol = "₹",
    ),

    "Amazon RKW": ChannelSchema(
        channel_name      = "Amazon RKW",
        filename_signals  = ["sales_asin_manufacturing", "sales_asin_retail"],
        col_signals       = ["ordered revenue", "ordered units", "shipped revenue",
                             "shipped cogs", "shipped units", "customer returns"],
        col_product       = "Product Title",
        col_product2      = None,
        col_channel_sku   = "ASIN",
        col_qty           = "Ordered Units",
        col_revenue       = "Ordered Revenue",
        col_date          = None,           # Manual date
        col_city          = None,           # National
        date_in_file      = False,
        city_in_file      = False,
        skiprows          = 1,              # First row is metadata header
        revenue_strip_symbol = "₹",
    ),

    "Shopify": ChannelSchema(
        channel_name      = "Shopify",
        filename_signals  = ["shopify"],
        col_signals       = ["product variant sku", "shipping city",
                             "net items sold", "product vendor", "product title"],
        col_product       = "Product title",
        col_product2      = None,
        col_channel_sku   = "Product variant SKU",
        col_qty           = "Net items sold",
        col_revenue       = "Total sales",
        col_date          = None,           # Manual date
        col_city          = "Shipping city",
        date_in_file      = False,
        city_in_file      = True,
        filter_min_qty    = True,           # Drop rows where qty ≤ 0
    ),

    "Firstclub": ChannelSchema(
        channel_name      = "Firstclub",
        filename_signals  = ["firstclub"],
        col_signals       = ["fcn", "sum of units_sold", "sum of gmv",
                             "product_name", "sale_date"],
        col_product       = "Product_name",
        col_product2      = None,
        col_channel_sku   = "FCN",
        col_qty           = "Sum of units_sold",
        col_revenue       = "Sum of gmv",
        col_date          = "sale_date",    # "Wed Apr 01 2026 00:00:00 GMT+0530..." — JS-style date
        col_city          = None,           # National — no city column
        date_in_file      = True,
        city_in_file      = False,
        date_parse_fn     = "js_date",      # Custom parser strips JS timezone cruft
    ),
}

SKIP_LABELS = {"total", "grand total", "subtotal", "nan", "", "none"}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _clean_num(val) -> float:
    """Convert messy numeric strings (₹, commas, parentheses) to float."""
    if pd.isna(val) or str(val).strip() in ("", "nan"):
        return 0.0
    s = str(val).strip()
    s = re.sub(r"[₹$€£,\s]", "", s)
    s = re.sub(r"\(([0-9.]+)\)", r"-\1", s)   # accounting negatives: (123) → -123
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_date(val, parse_fn: str) -> str | None:
    s = str(val).strip()
    if parse_fn == "range_start":
        # "20260427 - 20260427" — take first part
        part = s.split(" - ")[0].strip()
        try:
            return datetime.strptime(part, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            return pd.to_datetime(part).strftime("%Y-%m-%d")
        except Exception:
            return None
    elif parse_fn == "js_date":
        # JavaScript-style: "Wed Apr 01 2026 00:00:00 GMT+0530 (India Standard Time)"
        # Extract the "Mon DD YYYY" portion and parse it.
        m = re.search(r'(\w{3})\s+(\d{1,2})\s+(\d{4})', s)
        if m:
            try:
                return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None
    else:  # "standard"
        try:
            return pd.to_datetime(s).strftime("%Y-%m-%d")
        except Exception:
            return None


def _read_file(uf, skiprows: int = 0, sheet_name=None) -> pd.DataFrame | None:
    """Read an uploaded file (CSV or Excel) into a DataFrame."""
    name = uf.name.lower()
    try:
        if name.endswith(".csv"):
            return pd.read_csv(uf, skiprows=skiprows)
        kw: dict = {"skiprows": skiprows}
        if sheet_name:
            kw["sheet_name"] = sheet_name
        return pd.read_excel(uf, **kw)
    except Exception as e:
        st.error(f"Could not read {uf.name}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CHANNEL DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _detect_channel(
    peek_df: pd.DataFrame,
    filename: str,
    known_channels: list[str],
    saved_templates: dict,
) -> tuple[str | None, str, float]:
    """
    Returns (channel_name, detection_method, confidence 0–1).
    Priority: filename → built-in column fingerprint →
              saved-template filename → saved-template columns.
    """
    fname = filename.lower().replace(" ", "").replace("-", "_")
    cols_lower = [c.lower() for c in peek_df.columns]
    # Also scan first-row values (catches Amazon RKW where real cols are in row 0)
    first_row_vals: list[str] = []
    if not peek_df.empty:
        first_row_vals = [str(v).lower() for v in peek_df.iloc[0].tolist()]

    # 1. Filename → KNOWN_SCHEMAS
    for ch, schema in KNOWN_SCHEMAS.items():
        if ch not in known_channels:
            continue
        for sig in schema.filename_signals:
            if sig.replace(" ", "").replace("-", "_") in fname:
                return ch, "filename", 0.95

    # 2. Column fingerprints → KNOWN_SCHEMAS
    best_ch, best_score = None, 0.0
    for ch, schema in KNOWN_SCHEMAS.items():
        if ch not in known_channels:
            continue
        hits = 0
        for sig in schema.col_signals:
            if any(sig in c for c in cols_lower) or any(sig in v for v in first_row_vals):
                hits += 1
        score = hits / max(len(schema.col_signals), 1)
        if score > best_score:
            best_score, best_ch = score, ch
    if best_score >= 0.45:
        return best_ch, "columns", best_score

    # 3. Filename → saved Supabase templates
    for ch, tmpl in saved_templates.items():
        if ch not in known_channels:
            continue
        for sig in tmpl.get("filename_signals_list", []):
            if sig.replace(" ", "").replace("-", "_") in fname:
                return ch, "saved template (filename)", 0.85

    # 4. Column fingerprints → saved Supabase templates
    for ch, tmpl in saved_templates.items():
        if ch not in known_channels:
            continue
        col_sigs = tmpl.get("col_signals_list", [])
        if not col_sigs:
            continue
        hits = sum(1 for sig in col_sigs if any(sig.lower() in c for c in cols_lower))
        score = hits / max(len(col_sigs), 1)
        if score > best_score:
            best_score = score
            if score >= 0.4:
                return ch, "saved template (columns)", score

    return None, "undetected", 0.0


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN AUTO-MAPPER
# ─────────────────────────────────────────────────────────────────────────────

_FIELD_FALLBACKS: dict[str, list[str]] = {
    "product":     ["item name", "product name", "sku description", "title", "name", "sku"],
    "product2":    ["variant", "size", "weight", "pack size"],
    "channel_sku": ["asin", "sku id", "item id", "item_id", "sku"],
    "qty":         ["qty sold", "quantity sold", "units sold", "units ordered",
                    "net items sold", "quantity", "qty", "units"],
    "revenue":     ["ordered product sales", "total gross bill amount", "net sales",
                    "total sales", "gmv", "revenue", "net revenue", "sales"],
    "date":        ["ordered_date", "order date", "sale date", "date"],
    "city":        ["customer city", "shipping city", "source_city_name",
                    "city", "location", "region"],
    "filter":      [],
}


def _auto_map(df: pd.DataFrame, schema: ChannelSchema) -> dict[str, str | None]:
    """Find best matching column for each field using schema hints then fallbacks."""
    orig_by_lower = {c.lower(): c for c in df.columns}

    def find(target: str | None, fallbacks: list[str]) -> str | None:
        candidates = ([target] if target else []) + fallbacks
        for cand in candidates:
            if cand is None:
                continue
            cl = cand.lower()
            if cl in orig_by_lower:
                return orig_by_lower[cl]
            # Substring match
            for lc, orig in orig_by_lower.items():
                if cl in lc:
                    return orig
        return None

    return {
        "product":     find(schema.col_product,     _FIELD_FALLBACKS["product"]),
        "product2":    find(schema.col_product2,    _FIELD_FALLBACKS["product2"]),
        "channel_sku": find(schema.col_channel_sku, _FIELD_FALLBACKS["channel_sku"]),
        "qty":         find(schema.col_qty,         _FIELD_FALLBACKS["qty"]),
        "revenue":     find(schema.col_revenue,     _FIELD_FALLBACKS["revenue"]),
        "date":        find(schema.col_date,        _FIELD_FALLBACKS["date"]) if schema.date_in_file else None,
        "city":        find(schema.col_city,        _FIELD_FALLBACKS["city"]) if schema.city_in_file else None,
        "filter":      find(schema.filter_col,      _FIELD_FALLBACKS["filter"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _load_saved_templates(supabase) -> dict:
    """Load channel upload templates from `channel_upload_templates` table."""
    try:
        res = supabase.table("channel_upload_templates").select("*").execute()
        if not res.data:
            return {}
        result = {}
        for row in res.data:
            ch = row["channel"]
            row["filename_signals_list"] = json.loads(row.get("filename_signals") or "[]")
            row["col_signals_list"]      = json.loads(row.get("col_signals")      or "[]")
            result[ch] = row
        return result
    except Exception:
        return {}   # Table may not exist yet — fine


def _save_template(supabase, channel: str, col_map: dict,
                   fname_sigs: list[str], col_sigs: list[str],
                   skiprows: int, sheet_name: str | None):
    """Upsert a column template so new channels are auto-detected next time."""
    try:
        supabase.table("channel_upload_templates").upsert({
            "channel":          channel,
            "col_product":      col_map.get("product"),
            "col_product2":     col_map.get("product2"),
            "col_channel_sku":  col_map.get("channel_sku"),
            "col_qty":          col_map.get("qty"),
            "col_revenue":      col_map.get("revenue"),
            "col_date":         col_map.get("date"),
            "col_city":         col_map.get("city"),
            "filename_signals": json.dumps(fname_sigs),
            "col_signals":      json.dumps(col_sigs),
            "skiprows":         skiprows,
            "sheet_name":       sheet_name,
            "date_in_file":     col_map.get("date") is not None,
            "city_in_file":     col_map.get("city") is not None,
        }, on_conflict="channel").execute()
    except Exception:
        pass   # Non-critical — don't block upload if this fails


def _load_item_map(supabase) -> dict[str, str]:
    """Return {raw_name: master_name} from item_map table."""
    try:
        res = supabase.table("item_map").select("raw_name,master_name").execute()
        if not res.data:
            return {}
        return {r["raw_name"]: r["master_name"] for r in res.data}
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# WORK-DF BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _build_work_df(
    raw_df:      pd.DataFrame,
    schema:      ChannelSchema,
    col_map:     dict,
    manual_date: date | None,
    item_map:    dict[str, str],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Apply filters, build composite product key, parse dates/cities/nums.
    Returns (work_df, new_sku_keys).
    work_df columns: m_key, __date__, __city__, __qty__, __rev__
    new_sku_keys: raw product names not yet in item_map.
    """
    df = raw_df.copy()

    # ── Row filter: Order Status ──────────────────────────────────────────────
    f_col = col_map.get("filter")
    if schema.filter_col and schema.filter_value and f_col and f_col in df.columns:
        df = df[df[f_col].astype(str).str.strip().str.upper() == schema.filter_value.upper()]

    # ── Row filter: qty > 0 ───────────────────────────────────────────────────
    if schema.filter_min_qty:
        q = col_map.get("qty")
        if q and q in df.columns:
            df = df[pd.to_numeric(df[q], errors="coerce").fillna(0) > 0]

    # ── Product key ───────────────────────────────────────────────────────────
    p_col  = col_map.get("product")
    p2_col = col_map.get("product2")
    if not p_col or p_col not in df.columns:
        return pd.DataFrame(), []

    df["__prod__"] = df[p_col].astype(str).str.strip()
    if p2_col and p2_col in df.columns:
        df["m_key"] = df["__prod__"] + " " + df[p2_col].astype(str).str.strip()
    else:
        df["m_key"] = df["__prod__"]

    # Drop total / blank rows
    df = df[~df["__prod__"].str.lower().str.strip().isin(SKIP_LABELS)].copy()
    if df.empty:
        return pd.DataFrame(), []

    # ── Date ──────────────────────────────────────────────────────────────────
    d_col = col_map.get("date")
    if d_col and d_col in df.columns:
        fn = schema.date_parse_fn or "standard"
        df["__date__"] = df[d_col].apply(lambda v: _parse_date(v, fn))
        fallback_date  = str(manual_date) if manual_date else str(date.today())
        df["__date__"] = df["__date__"].fillna(fallback_date)
    else:
        df["__date__"] = str(manual_date) if manual_date else str(date.today())

    # ── City ──────────────────────────────────────────────────────────────────
    c_col = col_map.get("city")
    if c_col and c_col in df.columns:
        # Has a city column — use it, fall back to "National" for blank/missing values
        df["__city__"] = (
            df[c_col].astype(str).str.strip()
            .replace({"nan": "National", "none": "National", "": "National"})
        )
    else:
        # No city column for this channel — all rows are National
        df["__city__"] = "National"

    # ── Qty & Revenue ─────────────────────────────────────────────────────────
    q_col  = col_map.get("qty")
    r_col  = col_map.get("revenue")
    df["__qty__"] = df[q_col].apply(_clean_num) if q_col and q_col in df.columns else 0.0
    df["__rev__"] = df[r_col].apply(_clean_num) if r_col and r_col in df.columns else 0.0

    # ── New SKU keys ──────────────────────────────────────────────────────────
    all_keys = sorted(df["m_key"].dropna().astype(str).unique().tolist())
    new_keys = [k for k in all_keys if k not in item_map]

    return df[["m_key", "__date__", "__city__", "__qty__", "__rev__"]].copy(), new_keys


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA FROM SAVED TEMPLATE
# ─────────────────────────────────────────────────────────────────────────────

def _schema_from_template(ch: str, tmpl: dict) -> ChannelSchema:
    """Reconstruct a minimal ChannelSchema from a saved Supabase template row."""
    return ChannelSchema(
        channel_name     = ch,
        filename_signals = tmpl.get("filename_signals_list", []),
        col_signals      = tmpl.get("col_signals_list", []),
        col_product      = tmpl.get("col_product"),
        col_product2     = tmpl.get("col_product2"),
        col_channel_sku  = tmpl.get("col_channel_sku"),
        col_qty          = tmpl.get("col_qty"),
        col_revenue      = tmpl.get("col_revenue"),
        col_date         = tmpl.get("col_date"),
        col_city         = tmpl.get("col_city"),
        date_in_file     = bool(tmpl.get("date_in_file", True)),
        city_in_file     = bool(tmpl.get("city_in_file", True)),
        skiprows         = int(tmpl.get("skiprows") or 0),
        sheet_name       = tmpl.get("sheet_name"),
        date_parse_fn    = "standard",
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN RENDER FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def render_smart_upload_tab(
    supabase,
    master_skus_df:   pd.DataFrame,
    master_chans_df:  pd.DataFrame,
    upload_progress_bar,   # context-manager from performance.py
):
    """
    Renders the Smart Upload tab.
    Call from app.py:
        from smart_upload_module import render_smart_upload_tab
        render_smart_upload_tab(supabase, master_skus, master_chans, upload_progress_bar)
    """
    channels = (master_chans_df["name"].tolist()
                if not master_chans_df.empty and "name" in master_chans_df.columns else [])
    masters  = (master_skus_df["name"].tolist()
                if not master_skus_df.empty  and "name" in master_skus_df.columns  else [])

    if not channels:
        st.warning("No channels configured. Add channels in the Configuration tab first.")
        return
    if not masters:
        st.warning("No master SKUs configured. Add SKUs in the Configuration tab first.")
        return

    # ── Load saved state ──────────────────────────────────────────────────────
    saved_templates = _load_saved_templates(supabase)
    item_map        = _load_item_map(supabase)

    # ── Help text ─────────────────────────────────────────────────────────────
    known_ch_str = ", ".join(KNOWN_SCHEMAS.keys())
    st.caption(
        f"**Zero-config channels:** {known_ch_str}. "
        "For any other channel, map columns once and the app remembers forever. "
        "Drop all files at once — each is detected independently."
    )

    # ── Multi-file uploader ───────────────────────────────────────────────────
    uploaded_files = st.file_uploader(
        "Upload sales files — any channel, any number of files",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="su2_files",
    )

    if not uploaded_files:
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1 — Detect & configure each file
    # ─────────────────────────────────────────────────────────────────────────
    resolved: list[dict] = []      # One entry per file, fully configured

    for i, uf in enumerate(uploaded_files):

        # Quick read for detection (no skiprows, first sheet)
        try:
            nl = uf.name.lower()
            peek_df = (
                pd.read_csv(uf, nrows=3) if nl.endswith(".csv")
                else pd.read_excel(uf, nrows=3)
            )
            uf.seek(0)
        except Exception:
            peek_df = pd.DataFrame()
            try:
                uf.seek(0)
            except Exception:
                pass

        detected_ch, method, confidence = _detect_channel(
            peek_df, uf.name, channels, saved_templates
        )

        # Resolve schema
        if detected_ch in KNOWN_SCHEMAS:
            base_schema = KNOWN_SCHEMAS[detected_ch]
        elif detected_ch and detected_ch in saved_templates:
            base_schema = _schema_from_template(detected_ch, saved_templates[detected_ch])
        else:
            # Unknown channel — blank schema
            base_schema = ChannelSchema(
                channel_name="", filename_signals=[], col_signals=[],
                col_product=None, col_product2=None, col_channel_sku=None,
                col_qty=None, col_revenue=None, col_date=None, col_city=None,
            )

        # ── Per-file expander ─────────────────────────────────────────────────
        conf_badge = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.4 else "🔴"

        with st.expander(f"📄 **{uf.name}**", expanded=True):

            # Channel selector (pre-filled from detection)
            ch_default = channels.index(detected_ch) if detected_ch in channels else 0
            st.caption(
                f"{conf_badge} Auto-detected: **{detected_ch or 'Unknown'}** "
                f"via {method} ({confidence:.0%} confidence) — "
                "override below if incorrect."
            )
            sel_ch = st.selectbox("Channel", channels, index=ch_default, key=f"su2_ch_{i}")

            # Recompute schema if channel changed via override
            if sel_ch in KNOWN_SCHEMAS:
                active_schema = KNOWN_SCHEMAS[sel_ch]
            elif sel_ch in saved_templates:
                active_schema = _schema_from_template(sel_ch, saved_templates[sel_ch])
            else:
                active_schema = ChannelSchema(
                    channel_name=sel_ch, filename_signals=[], col_signals=[],
                    col_product=None, col_product2=None, col_channel_sku=None,
                    col_qty=None, col_revenue=None, col_date=None, col_city=None,
                )

            # Read file with correct skiprows + sheet
            uf.seek(0)
            raw_df = _read_file(uf, skiprows=active_schema.skiprows,
                                sheet_name=active_schema.sheet_name)
            try:
                uf.seek(0)
            except Exception:
                pass

            if raw_df is None or raw_df.empty:
                st.error("File appears empty or unreadable — skipping.")
                continue

            col_map = _auto_map(raw_df, active_schema)
            df_cols = ["None"] + raw_df.columns.tolist()

            def _idx(col: str | None) -> int:
                return df_cols.index(col) if col and col in df_cols else 0

            # ── Column mapping UI ─────────────────────────────────────────────
            st.markdown("**Column mapping** — pre-filled from auto-detection:")
            c1, c2, c3 = st.columns(3)

            with c1:
                p_col  = st.selectbox("Product column *",      df_cols,
                                      index=_idx(col_map["product"]),  key=f"su2_p_{i}")
                p2_col = st.selectbox("Variant column (opt.)", df_cols,
                                      index=_idx(col_map["product2"]), key=f"su2_p2_{i}")
            with c2:
                q_col  = st.selectbox("Qty column *",     df_cols,
                                      index=_idx(col_map["qty"]),     key=f"su2_q_{i}")
                r_col  = st.selectbox("Revenue column *", df_cols,
                                      index=_idx(col_map["revenue"]), key=f"su2_r_{i}")
            with c3:
                # Date: either a column from the file, or "Manual date"
                date_options  = ["Manual date"] + raw_df.columns.tolist()
                date_col_orig = col_map.get("date")
                date_default  = (
                    raw_df.columns.tolist().index(date_col_orig) + 1
                    if date_col_orig and date_col_orig in raw_df.columns else 0
                )
                d_sel = st.selectbox("Date column (or manual)", date_options,
                                     index=date_default, key=f"su2_d_{i}")
                d_col = None if d_sel == "Manual date" else d_sel

                city_col = st.selectbox("City column (opt.)", df_cols,
                                        index=_idx(col_map["city"]), key=f"su2_city_{i}")

            # Manual date picker — shown whenever no date column is selected
            manual_date: date | None = None
            if d_col is None:
                manual_date = st.date_input(f"Date for this file *", key=f"su2_fd_{i}")

            # Validate mandatory columns
            if p_col == "None" or q_col == "None" or r_col == "None":
                st.warning("⚠️ Product, Qty, and Revenue columns are required before upload.")
                continue

            if d_col is None and manual_date is None:
                st.warning("⚠️ A date is required — select a column or enter a manual date.")
                continue

            # Preview of first 3 rows
            preview_cols = [
                c for c in [p_col,
                             p2_col if p2_col != "None" else None,
                             q_col, r_col,
                             d_col,
                             city_col if city_col != "None" else None]
                if c and c != "None" and c in raw_df.columns
            ]
            if preview_cols:
                st.dataframe(raw_df[preview_cols].head(3), use_container_width=True,
                             hide_index=True)

            is_new_channel = (
                sel_ch not in KNOWN_SCHEMAS and sel_ch not in saved_templates
            )

            resolved.append({
                "uf":            uf,
                "raw_df":        raw_df,
                "sel_ch":        sel_ch,
                "active_schema": active_schema,
                "col_map": {
                    "product":     p_col     if p_col     != "None" else None,
                    "product2":    p2_col    if p2_col    != "None" else None,
                    "qty":         q_col     if q_col     != "None" else None,
                    "revenue":     r_col     if r_col     != "None" else None,
                    "date":        d_col,
                    "city":        city_col  if city_col  != "None" else None,
                    "filter":      col_map.get("filter"),
                },
                "manual_date":   manual_date,
                "is_new_channel": is_new_channel,
            })

    if not resolved:
        return

    st.divider()

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 — Build work DataFrames & collect new SKU keys
    # ─────────────────────────────────────────────────────────────────────────
    all_new_keys: dict[str, None] = {}

    for cfg in resolved:
        work_df, new_keys = _build_work_df(
            cfg["raw_df"], cfg["active_schema"], cfg["col_map"],
            cfg["manual_date"], item_map,
        )
        cfg["work_df"]  = work_df
        cfg["new_keys"] = new_keys
        for k in new_keys:
            all_new_keys[k] = None

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 3 — SKU mapping (only new/unknown names, batched across all files)
    # ─────────────────────────────────────────────────────────────────────────
    if all_new_keys:
        n = len(all_new_keys)
        st.warning(
            f"🗺 **{n} new product name{'s' if n > 1 else ''}** found across your files — "
            "map them to master SKUs to continue. "
            "Already-known products are pre-filled automatically."
        )
        with st.form("su2_sku_form"):
            new_mappings: dict[str, str] = {}
            cols_per_row = 2
            keys_list = sorted(all_new_keys.keys())
            for row_start in range(0, len(keys_list), cols_per_row):
                row_keys = keys_list[row_start: row_start + cols_per_row]
                form_cols = st.columns(len(row_keys))
                for col_widget, k in zip(form_cols, row_keys):
                    with col_widget:
                        new_mappings[k] = st.selectbox(
                            f"`{k[:60]}{'…' if len(k)>60 else ''}`",
                            masters, key=f"su2_sku_{row_start}_{k[:30]}",
                        )

            if st.form_submit_button("💾 Save mappings & continue", type="primary"):
                saved_count = 0
                for raw_name, master_name in new_mappings.items():
                    try:
                        supabase.table("item_map").upsert(
                            {"raw_name": raw_name, "master_name": master_name},
                            on_conflict="raw_name",
                        ).execute()
                        saved_count += 1
                    except Exception as e:
                        st.error(f"Failed to save mapping for '{raw_name}': {e}")
                if saved_count:
                    st.success(f"✅ Saved {saved_count} mapping{'s' if saved_count>1 else ''}.")
                    st.rerun()
        return   # Wait for user to map before proceeding to upload

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 4 — Build all final DataFrames in memory (no DB writes yet)
    # ─────────────────────────────────────────────────────────────────────────
    ch_summary = ", ".join(sorted({r["sel_ch"] for r in resolved}))
    n_files    = len(resolved)
    st.success(
        f"✅ **{n_files} file{'s' if n_files>1 else ''} ready** — {ch_summary}. "
        "All products mapped. Building preview…"
    )

    # Reload item_map in case it was just updated in this session
    fresh_item_map = _load_item_map(supabase)

    staged_data: list[dict] = []   # one entry per resolved file

    for cfg in resolved:
        sel_ch        = cfg["sel_ch"]
        active_schema = cfg["active_schema"]
        work_df       = cfg.get("work_df", pd.DataFrame())

        if work_df.empty:
            st.warning(f"⚠️ {cfg['uf'].name}: No valid rows after filtering — skipped.")
            continue

        # Build upload rows (same logic as before, but no DB touch)
        rows: list[dict] = []
        for _, r in work_df.iterrows():
            mk         = str(r["m_key"])
            master_sku = fresh_item_map.get(mk, mk)
            city_val   = str(r["__city__"]).strip() if pd.notna(r["__city__"]) else "National"
            if city_val.lower() in ("", "nan", "none"):
                city_val = "National"
            rows.append({
                "date":      str(r["__date__"]),
                "channel":   sel_ch,
                "item_name": master_sku,
                "qty_sold":  float(r["__qty__"]),
                "revenue":   float(r["__rev__"]),
                "city":      city_val,
            })

        # Aggregate (handles order-level files like Blinkit)
        group_cols = ["date", "channel", "item_name", "city"]
        final_df = (
            pd.DataFrame(rows)
            .groupby(group_cols, dropna=False)
            .agg({"qty_sold": "sum", "revenue": "sum"})
            .reset_index()
        )
        final_df["qty_sold"] = final_df["qty_sold"].fillna(0.0)
        final_df["revenue"]  = final_df["revenue"].fillna(0.0)
        final_df["city"] = (
            final_df["city"].fillna("National")
            .astype(str).str.strip()
            .replace({"": "National", "nan": "National", "none": "National"})
        )

        staged_data.append({
            "cfg":      cfg,
            "sel_ch":   sel_ch,
            "final_df": final_df,
        })

    if not staged_data:
        st.error("No valid data to upload after processing all files.")
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 5 — Preview: compare incoming data vs current DB state
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📋 Preview — Review before writing to DB")
    st.info(
        "⚠️ Nothing has been written to the database yet. "
        "Review the comparison below and click **Confirm** to proceed, "
        "or **Cancel** to abort with no changes."
    )

    preview_rows: list[dict] = []

    for entry in staged_data:
        sel_ch   = entry["sel_ch"]
        final_df = entry["final_df"]

        new_rows    = len(final_df)
        new_revenue = final_df["revenue"].sum()
        new_qty     = final_df["qty_sold"].sum()

        unique_dates = final_df["date"].dropna().unique().tolist()
        date_min = min(unique_dates) if unique_dates else "—"
        date_max = max(unique_dates) if unique_dates else "—"
        date_range_str = date_min if date_min == date_max else f"{date_min} → {date_max}"

        # Query DB for existing rows covering the same (channel, dates)
        db_rows    = 0
        db_revenue = 0.0
        db_qty     = 0.0
        try:
            DEL_CHUNK = 50
            all_db_records: list[dict] = []
            for di in range(0, len(unique_dates), DEL_CHUNK):
                date_batch = unique_dates[di: di + DEL_CHUNK]
                res = (
                    supabase.table("sales")
                    .select("qty_sold,revenue")
                    .eq("channel", sel_ch)
                    .in_("date", date_batch)
                    .execute()
                )
                if res.data:
                    all_db_records.extend(res.data)
            db_rows    = len(all_db_records)
            db_revenue = sum(float(r.get("revenue", 0) or 0) for r in all_db_records)
            db_qty     = sum(float(r.get("qty_sold", 0) or 0) for r in all_db_records)
        except Exception as e:
            db_rows    = -1
            db_revenue = 0.0
            db_qty     = 0.0
            st.warning(f"Could not fetch existing DB data for {sel_ch}: {e}")

        delta_rev = new_revenue - db_revenue
        delta_qty = new_qty     - db_qty

        preview_rows.append({
            "Channel":       sel_ch,
            "File":          entry["cfg"]["uf"].name,
            "Date Range":    date_range_str,
            "DB Rows":       db_rows if db_rows >= 0 else "error",
            "DB Revenue ₹":  f"{db_revenue:,.0f}",
            "DB Qty":        f"{db_qty:,.1f}",
            "New Rows":      new_rows,
            "New Revenue ₹": f"{new_revenue:,.0f}",
            "New Qty":       f"{new_qty:,.1f}",
            "Δ Revenue ₹":   f"{delta_rev:+,.0f}",
        })

    preview_df = pd.DataFrame(preview_rows)
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

    st.markdown(
        "**What will happen on Confirm:**  \n"
        "For each channel above, all existing DB rows for the listed dates will be "
        "**deleted** and replaced with the new rows from your file. "
        "Dates not present in your file are untouched."
    )

    col_confirm, col_cancel, _ = st.columns([1, 1, 4])

    with col_cancel:
        if st.button("❌ Cancel", key="su2_cancel"):
            st.info("Upload cancelled — no changes were made to the database.")
            return

    with col_confirm:
        confirmed = st.button("✅ Confirm & Push to Live DB", type="primary", key="su2_confirm")

    if not confirmed:
        return

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 6 — Execute: delete-then-insert for each staged file
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("🚀 Writing to database…")

    overall_errors: list[str] = []
    total_records  = 0

    for entry in staged_data:
        sel_ch   = entry["sel_ch"]
        final_df = entry["final_df"]
        cfg      = entry["cfg"]
        active_schema = cfg["active_schema"]

        CHUNK   = 500
        records = final_df.to_dict(orient="records")

        try:
            # Delete existing rows for the dates being re-uploaded
            unique_dates = final_df["date"].dropna().unique().tolist()
            with st.spinner(
                f"Clearing existing {sel_ch} data for "
                f"{len(unique_dates)} date(s) before re-insert…"
            ):
                DEL_CHUNK = 50
                for di in range(0, len(unique_dates), DEL_CHUNK):
                    date_batch = unique_dates[di: di + DEL_CHUNK]
                    supabase.table("sales").delete()\
                        .eq("channel", sel_ch)\
                        .in_("date", date_batch)\
                        .execute()

            with upload_progress_bar(
                len(records), chunk_size=CHUNK, label=f"Uploading {sel_ch}"
            ) as tick:
                for j in range(0, len(records), CHUNK):
                    res = supabase.table("sales").insert(
                        records[j: j + CHUNK],
                    ).execute()
                    if hasattr(res, "error") and res.error:
                        overall_errors.append(f"{sel_ch} chunk {j//CHUNK+1}: {res.error}")
                    tick()

            total_records += len(final_df)
            st.success(f"✅ **{sel_ch}** — {len(final_df):,} records synced")

        except Exception as e:
            overall_errors.append(f"{sel_ch}: {e}")
            continue

        # Save column template for new channels
        if cfg["is_new_channel"]:
            col_sigs  = [c.lower() for c in cfg["raw_df"].columns.tolist()[:10]]
            fname_sig = re.sub(r"[_\-\s\.]+\d+.*$", "", cfg["uf"].name.lower())[:30]
            _save_template(
                supabase, sel_ch, cfg["col_map"],
                fname_sigs=[fname_sig],
                col_sigs=col_sigs,
                skiprows=active_schema.skiprows,
                sheet_name=active_schema.sheet_name,
            )
            st.toast(
                f"📚 Template saved for '{sel_ch}' — "
                "future uploads will be auto-detected",
                icon="💾",
            )

    # ── Final result ──────────────────────────────────────────────────────────
    if overall_errors:
        for err in overall_errors:
            st.error(err)
        st.warning(
            "⚠️ Some records may not have synced. "
            "Ensure `sales` table has a UNIQUE constraint on (date, channel, item_name, city)."
        )
    elif total_records > 0:
        st.balloons()
        st.cache_data.clear()
        st.rerun()
