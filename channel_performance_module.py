"""
channel_performance_module.py
──────────────────────────────────────────────────────────────────────────────
Inventory Control Hub — rendered as a tab inside the main Sales Tracker app.

Sales data is pulled from Supabase (same DB as app_24).
  - Rows with city IS NOT NULL  → city-level join (Blinkit, Swiggy, BigBasket)
  - Rows with city IS NULL      → national aggregate fallback (Amazon, legacy)

Only inventory files are uploaded here — no separate sales upload needed.

HOW IT PLUGS IN (app_24.py):
  from channel_performance_module import render_channel_performance_tab
  ...
  with tabs[N]:
      render_channel_performance_tab(supabase, master_skus, role)
"""

import re
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# ── Global Filter Notes ───────────────────────────────────────────────────────
# Channel Performance does NOT use the global date/channel filter.
# Reason: this tab's "Sales Window" (N days back) is an inventory-specific
# control — it determines Days of Cover (DOC) calculation, not a view filter.
# Applying a global date range here would break DOC/DRR math.
# The tab is self-contained; no global_filters import is needed.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers — SKU mapping persistence
# ─────────────────────────────────────────────────────────────────────────────

def _load_mappings(supabase_client) -> pd.DataFrame:
    """Load SKU mappings from Supabase channel_sku_mappings table."""
    try:
        res = supabase_client.table("channel_sku_mappings").select("*").execute()
        if not res.data:
            return pd.DataFrame(columns=["channel", "channel_sku", "master_sku"])
        return pd.DataFrame(res.data)[["channel", "channel_sku", "master_sku"]].astype(str)
    except Exception:
        return pd.DataFrame(columns=["channel", "channel_sku", "master_sku"])


def _save_mappings(supabase_client, new_entries):
    """Upsert SKU mappings into Supabase channel_sku_mappings table."""
    for entry in new_entries:
        supabase_client.table("channel_sku_mappings").upsert(
            entry, on_conflict="channel,channel_sku"
        ).execute()


# ─────────────────────────────────────────────────────────────────────────────
# Supabase sales fetch
# ─────────────────────────────────────────────────────────────────────────────

def _load_item_map(supabase_client) -> pd.DataFrame:
    """
    Load the item_map table (used by Smart Upload in app_24).
    raw_name → master_name — used as ASIN→master_sku lookup for Amazon.
    """
    try:
        res = supabase_client.table("item_map").select("raw_name, master_name").execute()
        if not res.data:
            return pd.DataFrame(columns=["raw_name", "master_name"])
        return pd.DataFrame(res.data).astype(str)
    except Exception:
        return pd.DataFrame(columns=["raw_name", "master_name"])


def _get_sales(_supabase, days: int) -> pd.DataFrame:
    """
    Pull last `days` days of sales from Supabase.
    Returns: date, channel, item_name, city (nullable), qty_sold, revenue
    """
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        all_rows, page, PAGE_SIZE = [], 0, 1000
        while True:
            res = (
                _supabase.table("sales")
                .select("date, channel, item_name, city, qty_sold, revenue")
                .gte("date", cutoff)
                .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                .execute()
            )
            if not res.data:
                break
            all_rows.extend(res.data)
            if len(res.data) < PAGE_SIZE:
                break
            page += 1

        if not all_rows:
            return pd.DataFrame(
                columns=["date", "channel", "item_name", "city", "qty_sold", "revenue"]
            )
        df = pd.DataFrame(all_rows)
        df["date"]     = pd.to_datetime(df["date"], errors="coerce")
        df["qty_sold"] = pd.to_numeric(df["qty_sold"], errors="coerce").fillna(0)
        df["revenue"]  = pd.to_numeric(df["revenue"],  errors="coerce").fillna(0)
        return df.dropna(subset=["date"])
    except Exception as e:
        st.warning(f"Could not fetch sales from Supabase: {e}")
        return pd.DataFrame(
            columns=["date", "channel", "item_name", "city", "qty_sold", "revenue"]
        )


# ─────────────────────────────────────────────────────────────────────────────
# Sales aggregation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _channel_sales(sales_df: pd.DataFrame, channel_keyword: str) -> pd.DataFrame:
    """
    Filter sales for one channel and return aggregated by item_name + city.
    NULL-city (legacy) rows are tagged city='__national__' for fallback use.
    """
    if sales_df.empty:
        return pd.DataFrame(columns=["item_name", "city", "qty_sold", "revenue"])

    mask = sales_df["channel"].str.lower().str.contains(channel_keyword.lower(), na=False)
    ch   = sales_df[mask].copy()
    if ch.empty:
        return pd.DataFrame(columns=["item_name", "city", "qty_sold", "revenue"])

    city_rows    = ch[ch["city"].notna() & (ch["city"].str.strip() != "")]
    no_city_rows = ch[ch["city"].isna()  | (ch["city"].str.strip() == "")]

    parts = []
    if not city_rows.empty:
        agg = (
            city_rows.groupby(["item_name", "city"])
            .agg(qty_sold=("qty_sold", "sum"), revenue=("revenue", "sum"))
            .reset_index()
        )
        parts.append(agg)

    if not no_city_rows.empty:
        agg_nat          = (
            no_city_rows.groupby("item_name")
            .agg(qty_sold=("qty_sold", "sum"), revenue=("revenue", "sum"))
            .reset_index()
        )
        agg_nat["city"]  = "__national__"
        parts.append(agg_nat)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(
        columns=["item_name", "city", "qty_sold", "revenue"]
    )


# ─────────────────────────────────────────────────────────────────────────────
# File loader
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Inventory snapshot helpers — persist parsed inventory to Supabase
# ─────────────────────────────────────────────────────────────────────────────

SNAPSHOT_COLS = ["channel", "channel_sku", "inventory", "str", "doc",
                 "drr", "units_sold", "n_days", "location"]

def _save_snapshot(supabase_client, parsed_df: pd.DataFrame, channel: str):
    """
    Save parsed inventory rows for a channel to channel_inventory_snapshots.
    Deletes existing rows for this channel first, then inserts fresh ones.
    """
    try:
        # Delete old snapshot for this channel
        supabase_client.table("channel_inventory_snapshots")             .delete().eq("channel", channel).execute()

        # Insert new rows
        from datetime import timezone
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in parsed_df.iterrows():
            rows.append({
                "channel":     channel,
                "uploaded_at": now,
                "channel_sku": str(r.get("channel_sku", "")),
                "inventory":   float(r.get("inventory", 0)),
                "str":         float(r.get("str", 0)),
                "doc":         float(r.get("doc", 0)),
                "drr":         float(r.get("drr", 0)),
                "units_sold":  float(r.get("units_sold", 0)),
                "n_days":      int(r.get("n_days", 30)),
                "location":    str(r.get("location", "")),
            })
        # Insert in chunks
        CHUNK = 500
        for i in range(0, len(rows), CHUNK):
            supabase_client.table("channel_inventory_snapshots")                 .insert(rows[i:i+CHUNK]).execute()
    except Exception as e:
        st.warning(f"Could not save inventory snapshot for {channel}: {e}")


def _load_snapshots(supabase_client) -> dict:
    """
    Load latest saved inventory snapshots from Supabase.
    Returns dict: { channel_name -> (uploaded_at str, DataFrame) }
    """
    try:
        res = supabase_client.table("channel_inventory_snapshots")             .select("*").execute()
        if not res.data:
            return {}
        df = pd.DataFrame(res.data)
        result = {}
        for channel, grp in df.groupby("channel"):
            uploaded_at = grp["uploaded_at"].iloc[0]
            # Parse uploaded_at to a readable string
            try:
                dt = pd.to_datetime(uploaded_at)
                uploaded_at_str = dt.strftime("%-d %b %Y, %I:%M %p")
            except Exception:
                uploaded_at_str = str(uploaded_at)
            snap_df = grp[SNAPSHOT_COLS].copy()
            for col in ["inventory", "str", "doc", "drr", "units_sold"]:
                snap_df[col] = pd.to_numeric(snap_df[col], errors="coerce").fillna(0)
            snap_df["n_days"] = pd.to_numeric(snap_df["n_days"], errors="coerce").fillna(30).astype(int)
            result[channel] = (uploaded_at_str, snap_df)
        return result
    except Exception as e:
        st.warning(f"Could not load inventory snapshots: {e}")
        return {}


def _load_file(uploaded_file, skiprows: int = 0) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file, skiprows=skiprows)
    return pd.read_excel(uploaded_file, skiprows=skiprows)


def _find_col(df: pd.DataFrame, options: list):
    for opt in options:
        if opt in df.columns:
            return opt
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Channel parsers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_amazon(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int, db_mappings: pd.DataFrame = None, item_map_df: pd.DataFrame = None) -> pd.DataFrame:
    inv_df = inv_df.copy()
    sku_c  = _find_col(inv_df, ["ASIN", "asin", "sku"])
    inv_df["channel_sku"] = inv_df[sku_c].astype(str).str.strip() if sku_c else ""
    inv_df["inventory"]   = pd.to_numeric(inv_df["Sellable On Hand Units"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    inv_df["location"]    = "National"

    inv_df["str"] = 0.0
    if "Sell-Through %" in inv_df.columns:
        inv_df["str"] = (
            pd.to_numeric(
                inv_df["Sell-Through %"].astype(str).str.replace("%", "").str.replace(",", ""), errors="coerce"
            ).fillna(0) / 100
        )

    # Translate ASIN → master_sku using two sources:
    # 1. channel_sku_mappings (mapped via Channel Performance tab)
    # 2. item_map (mapped via Smart Upload — raw_name = ASIN string)
    amz_map = {}
    if db_mappings is not None and not db_mappings.empty:
        ch_entries = db_mappings[db_mappings["channel"] == "Amazon"]
        amz_map.update(ch_entries.set_index("channel_sku")["master_sku"].to_dict())
    if item_map_df is not None and not item_map_df.empty:
        amz_map.update(item_map_df.set_index("raw_name")["master_name"].to_dict())
    inv_df["master_sku"] = inv_df["channel_sku"].map(amz_map).fillna(inv_df["channel_sku"]).astype(str)

    if not sales_df.empty:
        nat = (
            sales_df[sales_df["city"] == "__national__"]
            .groupby("item_name")[["qty_sold"]]
            .sum()
            .reset_index()
            .rename(columns={"qty_sold": "units_sold"})
        )
        inv_df = inv_df.merge(nat, left_on="master_sku", right_on="item_name", how="left").fillna(0)
        sales_val = pd.to_numeric(inv_df["units_sold"], errors="coerce").fillna(0)
    else:
        sales_val = pd.Series(0.0, index=inv_df.index)

    inv_df["drr"]        = (sales_val / n_days).round(2)
    inv_df["doc"]        = inv_df["inventory"] / inv_df["drr"].replace(0, 0.001)
    inv_df["units_sold"] = sales_val
    inv_df["n_days"]     = n_days
    return inv_df[["channel_sku", "inventory", "str", "doc", "drr", "units_sold", "n_days", "location"]]


def _parse_blinkit(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int, db_mappings: pd.DataFrame = None) -> pd.DataFrame:
    inv_df = inv_df.copy()
    inv_df["channel_sku"] = inv_df["Item ID"].astype(str).str.strip()
    f_col  = _find_col(inv_df, ["Warehouse Facility Name", "Facility Name", "Store"])
    inv_df["fac_id"]    = inv_df[f_col].astype(str).str.strip() if f_col else "Unknown"
    inv_df["location"]  = inv_df["fac_id"]
    inv_df["inventory"] = pd.to_numeric(inv_df["Total sellable"], errors="coerce").fillna(0)

    NCR_CITY_MAP = {
        "Farukhnagar": "HR-NCR", "Kundli": "HR-NCR", "Faridabad": "HR-NCR",
        "Gurgaon": "HR-NCR",     "Gurugram": "HR-NCR",
        "Noida": "UP-NCR",       "Ghaziabad": "UP-NCR", "Gr.Noida": "UP-NCR",
    }
    def extract_city(fac):
        first_word = str(fac).split()[0]
        return NCR_CITY_MAP.get(first_word, first_word)

    inv_df["_city_key"] = inv_df["fac_id"].apply(extract_city)

    last30 = pd.to_numeric(
        inv_df.get("Last 30 days", pd.Series(0, index=inv_df.index)), errors="coerce"
    ).fillna(0)

    # Translate channel_sku → master_sku for sales join
    if db_mappings is not None and not db_mappings.empty:
        blk_map = db_mappings[db_mappings["channel"] == "Blinkit"].set_index("channel_sku")["master_sku"].to_dict()
        inv_df["master_sku"] = inv_df["channel_sku"].map(blk_map).fillna(inv_df["channel_sku"]).astype(str)
    else:
        inv_df["master_sku"] = inv_df["channel_sku"].astype(str)

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "city", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["master_sku", "_city_key"],
                right_on=["item_name", "city"],
                how="left",
            ).fillna(0)
            sales_val = pd.to_numeric(inv_df["units_sold"], errors="coerce").fillna(0)
        else:
            sales_val = pd.Series(0.0, index=inv_df.index)

        daily_rate    = (sales_val / n_days).replace(0, 0.001)
        sales_30d     = sales_val * (30 / n_days)
        inv_df["str"] = sales_30d / (sales_30d + inv_df["inventory"]).replace(0, 1)
        fallback_doc  = inv_df["inventory"] / (last30 / 30).replace(0, 0.001)
        inv_df["doc"] = (inv_df["inventory"] / daily_rate).where(sales_val > 0, fallback_doc)
        fallback_drr  = (last30 / 30).round(2)
        inv_df["drr"] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df["units_sold"] = sales_val
    else:
        inv_df["str"]        = last30 / (last30 + inv_df["inventory"]).replace(0, 1)
        inv_df["doc"]        = inv_df["inventory"] / (last30 / 30).replace(0, 0.001)
        inv_df["drr"]        = (last30 / 30).round(2)
        inv_df["units_sold"] = last30

    inv_df["n_days"] = n_days
    return inv_df[["channel_sku", "inventory", "str", "doc", "drr", "units_sold", "n_days", "location"]]


def _parse_swiggy(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int, db_mappings: pd.DataFrame = None) -> pd.DataFrame:
    inv_df = inv_df.copy()
    inv_df["channel_sku"] = inv_df["SkuCode"].astype(str).str.strip()
    inv_df["location"]    = inv_df["City"] + " (" + inv_df["FacilityName"] + ")"
    inv_df["inventory"]   = pd.to_numeric(inv_df["WarehouseQtyAvailable"], errors="coerce").fillna(0)
    # Normalise to UPPER for join — Supabase stores Swiggy CITY exactly as in the sales file
    inv_df["_city_key"]   = inv_df["City"].astype(str).str.strip().str.upper()

    doh_fallback = (
        pd.to_numeric(inv_df["DaysOnHand"], errors="coerce").fillna(0).clip(upper=365)
        if "DaysOnHand" in inv_df.columns else pd.Series(0.0, index=inv_df.index)
    )

    # Translate channel_sku → master_sku for sales join
    if db_mappings is not None and not db_mappings.empty:
        swg_map = db_mappings[db_mappings["channel"] == "Swiggy"].set_index("channel_sku")["master_sku"].to_dict()
        inv_df["master_sku"] = inv_df["channel_sku"].map(swg_map).fillna(inv_df["channel_sku"]).astype(str)
    else:
        inv_df["master_sku"] = inv_df["channel_sku"].astype(str)

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        city_sales["_city_upper"] = city_sales["city"].astype(str).str.strip().str.upper()

        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "_city_upper", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["master_sku", "_city_key"],
                right_on=["item_name", "_city_upper"],
                how="left",
            ).fillna(0)
            sales_val = pd.to_numeric(inv_df["units_sold"], errors="coerce").fillna(0)
        else:
            sales_val = pd.Series(0.0, index=inv_df.index)

        daily_rate    = (sales_val / n_days).replace(0, 0.001)
        sales_30d     = sales_val * (30 / n_days)
        inv_df["str"] = sales_30d / (sales_30d + inv_df["inventory"]).replace(0, 1)
        computed_doc  = inv_df["inventory"] / daily_rate
        inv_df["doc"] = computed_doc.where(sales_val > 0, doh_fallback.values)
        fallback_drr  = (inv_df["inventory"] / doh_fallback.where(doh_fallback > 0, other=float("nan"))).fillna(0).round(2)
        inv_df["drr"] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df["units_sold"] = sales_val
    else:
        inv_df["doc"]        = doh_fallback.values
        inv_df["str"]        = 0.0
        inv_df["drr"]        = (inv_df["inventory"] / doh_fallback.where(doh_fallback > 0, other=float("nan"))).fillna(0).round(2)
        inv_df["units_sold"] = 0.0

    inv_df["n_days"] = n_days
    return inv_df[["channel_sku", "inventory", "str", "doc", "drr", "units_sold", "n_days", "location"]]


def _parse_bigbasket(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int, db_mappings: pd.DataFrame = None) -> pd.DataFrame:
    inv_df = inv_df.copy()
    inv_df["channel_sku"] = inv_df["SKU_Id"].astype(str).str.strip()
    inv_df["location"]    = inv_df["DC"].astype(str).str.strip() if "DC" in inv_df.columns else "Unknown"
    inv_df["inventory"]   = pd.to_numeric(inv_df["Total SOH"], errors="coerce").fillna(0)

    doh_col = _find_col(inv_df, ["SOH Day of Cover (HO)", "SOH Day of Cover", "Day of Cover"])
    doh_fallback = (
        pd.to_numeric(inv_df[doh_col], errors="coerce").fillna(0).clip(upper=365)
        if doh_col else pd.Series(0.0, index=inv_df.index)
    )

    BB_DC_CITY_MAP = {
        "Ahmedabad": "Ahmedabad-Gandhinagar", "Bhubaneswar": "Bhubaneshwar-Cuttack",
        "Kundli": "Gurgaon", "Lucknow": "Lucknow-Kanpur",
        "Vadodara": "Ahmedabad-Gandhinagar", "Vijayawada": "Vijayawada-Guntur",
    }
    def dc_to_city(dc_name):
        city = re.sub(r"[-\s]?DC\d*$", "", str(dc_name), flags=re.IGNORECASE).strip()
        return BB_DC_CITY_MAP.get(city, city)

    inv_df["_city_key"] = inv_df["location"].apply(dc_to_city)

    # Translate channel_sku → master_sku for sales join
    if db_mappings is not None and not db_mappings.empty:
        bb_map = db_mappings[db_mappings["channel"] == "Big Basket"].set_index("channel_sku")["master_sku"].to_dict()
        inv_df["master_sku"] = inv_df["channel_sku"].map(bb_map).fillna(inv_df["channel_sku"]).astype(str)
    else:
        inv_df["master_sku"] = inv_df["channel_sku"].astype(str)

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "city", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["master_sku", "_city_key"],
                right_on=["item_name", "city"],
                how="left",
            ).fillna(0)
            sales_val = pd.to_numeric(inv_df["units_sold"], errors="coerce").fillna(0)
        else:
            sales_val = pd.Series(0.0, index=inv_df.index)

        daily_rate    = (sales_val / n_days).replace(0, 0.001)
        sales_30d     = sales_val * (30 / n_days)
        inv_df["str"] = sales_30d / (sales_30d + inv_df["inventory"]).replace(0, 1)
        computed_doc  = inv_df["inventory"] / daily_rate
        inv_df["doc"] = computed_doc.where(sales_val > 0, doh_fallback.values)
        fallback_drr  = (inv_df["inventory"] / doh_fallback.where(doh_fallback > 0, other=float("nan"))).fillna(0).round(2)
        inv_df["drr"] = (sales_val / n_days).where(sales_val > 0, fallback_drr).round(2)
        inv_df["units_sold"] = sales_val
    else:
        inv_df["str"]        = 0.0
        inv_df["doc"]        = doh_fallback.values
        inv_df["drr"]        = (inv_df["inventory"] / doh_fallback.where(doh_fallback > 0, other=float("nan"))).fillna(0).round(2)
        inv_df["units_sold"] = 0.0

    inv_df["n_days"] = n_days
    return inv_df[["channel_sku", "inventory", "str", "doc", "drr", "units_sold", "n_days", "location"]]


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard renderer — identical output to app_25
# ─────────────────────────────────────────────────────────────────────────────

def _reapply_sales(snap_df: pd.DataFrame, raw_sales: pd.DataFrame,
                   channel: str, db_mappings: pd.DataFrame, n_days: int) -> pd.DataFrame:
    """
    When loading a saved inventory snapshot, re-compute DRR / STR / DOC
    using the current sales window instead of the stale values in the snapshot.
    This keeps metrics fresh even when the inventory file hasn't changed.
    """
    snap_df = snap_df.copy()

    if raw_sales.empty:
        return snap_df

    keyword_map = {
        "Amazon":     "amazon",
        "Blinkit":    "blinkit",
        "Swiggy":     "swiggy",
        "Big Basket": "big basket",
    }
    keyword = keyword_map.get(channel, channel.lower())
    sales_df = _channel_sales(raw_sales, keyword)

    if sales_df.empty:
        return snap_df

    # Build master_sku → city → qty lookup from current sales
    city_sales = sales_df[sales_df["city"] != "__national__"].copy()

    # Translate channel_sku → master_sku using db_mappings
    if db_mappings is not None and not db_mappings.empty:
        ch_map = db_mappings[db_mappings["channel"] == channel].set_index("channel_sku")["master_sku"].to_dict()
        snap_df["master_sku_tmp"] = snap_df["channel_sku"].map(ch_map).fillna(snap_df["channel_sku"]).astype(str)
    else:
        snap_df["master_sku_tmp"] = snap_df["channel_sku"].astype(str)

    if not city_sales.empty and channel != "Amazon":
        # City-level join for Blinkit / Swiggy / BigBasket
        # Normalise city keys the same way parsers do
        if channel == "Swiggy":
            city_sales["_ckey"] = city_sales["city"].astype(str).str.strip().str.upper()
            snap_df["_ckey"]    = snap_df["location"].astype(str).str.split(" (", regex=False).str[0].str.strip().str.upper()
        elif channel == "Big Basket":
            # Strip "-DC" / "-DC2" suffix and apply city aliases, same as _parse_bigbasket
            BB_DC_CITY_MAP = {
                "Ahmedabad": "Ahmedabad-Gandhinagar", "Bhubaneswar": "Bhubaneshwar-Cuttack",
                "Kundli": "Gurgaon", "Lucknow": "Lucknow-Kanpur",
                "Vadodara": "Ahmedabad-Gandhinagar", "Vijayawada": "Vijayawada-Guntur",
            }
            def _dc_to_city(dc_name):
                city = re.sub(r"[-\s]?DC\d*$", "", str(dc_name), flags=re.IGNORECASE).strip()
                return BB_DC_CITY_MAP.get(city, city)
            city_sales["_ckey"] = city_sales["city"].astype(str).str.strip()
            snap_df["_ckey"]    = snap_df["location"].astype(str).apply(_dc_to_city)
        else:
            city_sales["_ckey"] = city_sales["city"].astype(str).str.strip()
            snap_df["_ckey"]    = snap_df["location"].astype(str).str.strip()

        merged = snap_df.merge(
            city_sales[["item_name", "_ckey", "qty_sold"]].rename(columns={"qty_sold": "fresh_units"}),
            left_on=["master_sku_tmp", "_ckey"],
            right_on=["item_name", "_ckey"],
            how="left",
        ).fillna(0)
        fresh_units = pd.to_numeric(merged["fresh_units"], errors="coerce").fillna(0)
        snap_df = merged.drop(columns=["item_name", "_ckey", "fresh_units"], errors="ignore")
    else:
        # Amazon — national aggregate
        nat = (
            sales_df[sales_df["city"] == "__national__"]
            .groupby("item_name")["qty_sold"].sum()
            .reset_index().rename(columns={"qty_sold": "fresh_units"})
        )
        merged = snap_df.merge(nat, left_on="master_sku_tmp", right_on="item_name", how="left").fillna(0)
        fresh_units = pd.to_numeric(merged["fresh_units"], errors="coerce").fillna(0)
        snap_df = merged.drop(columns=["item_name", "fresh_units"], errors="ignore")

    # Re-compute DRR / DOC / STR where we have fresh sales
    old_drr  = snap_df["drr"].copy()
    old_doc  = snap_df["doc"].copy()

    new_drr = (fresh_units / n_days).round(2)
    new_doc = (snap_df["inventory"] / new_drr.replace(0, 0.001)).round(1)

    sales_30d = fresh_units * (30 / n_days)
    new_str   = sales_30d / (sales_30d + snap_df["inventory"]).replace(0, 1)

    has_sales = fresh_units > 0
    snap_df["drr"]        = new_drr.where(has_sales, old_drr)
    snap_df["doc"]        = new_doc.where(has_sales, old_doc)
    snap_df["str"]        = new_str.where(has_sales, snap_df["str"])
    snap_df["units_sold"] = fresh_units.where(has_sales, snap_df["units_sold"])
    snap_df["n_days"]     = n_days

    # Clean up temp columns
    snap_df = snap_df.drop(columns=["master_sku_tmp", "_ckey"], errors="ignore")
    return snap_df


def _render_dashboard(merged: pd.DataFrame):

    st.sidebar.divider()
    st.sidebar.header("📦 Inventory Filters")

    u_channels   = sorted(merged["channel"].unique().tolist())
    sel_channels = st.sidebar.multiselect("Channel", u_channels, default=u_channels, key="cp_channels")
    u_products   = sorted(merged["master_sku"].dropna().unique().tolist())
    sel_products = st.sidebar.multiselect("Product", u_products, default=u_products, key="cp_products")

    filtered_df = merged[
        merged["channel"].isin(sel_channels) & merged["master_sku"].isin(sel_products)
    ]
    filtered_df = filtered_df[filtered_df["inventory"] > 0].copy()

    u_locations   = sorted(filtered_df["location"].dropna().unique().tolist())
    sel_locations = st.sidebar.multiselect("Location", u_locations, default=u_locations, key="cp_locations")
    filtered_df   = filtered_df[filtered_df["location"].isin(sel_locations)]

    st.sidebar.divider()
    st.sidebar.header("🎯 Actionable Filters")
    st.sidebar.caption("Metrics and table both update based on these thresholds.")

    st.sidebar.markdown("**Days of Cover (DOC) range**")
    _dc1, _dc2  = st.sidebar.columns(2)
    _doc_min    = _dc1.number_input("Min days", min_value=0, max_value=9999, value=0,    step=1, key="cp_doc_min")
    _doc_max    = _dc2.number_input("Max days", min_value=0, max_value=9999, value=9999, step=1, key="cp_doc_max")
    _doc_slider = st.sidebar.slider("DOC slider", 0, 9999, (int(_doc_min), int(_doc_max)),
                                    step=1, label_visibility="collapsed", key="cp_doc_slider")
    min_doc, max_doc = (int(_doc_min), int(_doc_max)) if (_doc_min != 0 or _doc_max != 9999) else _doc_slider

    st.sidebar.markdown("**Sell-Through Rate (STR) range %**")
    _sc1, _sc2  = st.sidebar.columns(2)
    _str_min    = _sc1.number_input("Min %", min_value=0, max_value=200, value=0,   step=1, key="cp_str_min")
    _str_max    = _sc2.number_input("Max %", min_value=0, max_value=200, value=200, step=1, key="cp_str_max")
    _str_slider = st.sidebar.slider("STR slider", 0, 200, (int(_str_min), int(_str_max)),
                                    step=1, label_visibility="collapsed", key="cp_str_slider")
    min_str, max_str = (int(_str_min), int(_str_max)) if (_str_min != 0 or _str_max != 200) else _str_slider

    action_active = min_doc > 0 or max_doc < 9999 or min_str > 0 or max_str < 200
    table_df = filtered_df[
        (filtered_df["doc"] >= min_doc) & (filtered_df["doc"] <= max_doc) &
        (filtered_df["str"] >= min_str / 100) & (filtered_df["str"] <= max_str / 100)
    ].copy()

    doc_label    = f" | DOC {min_doc}–{max_doc}d" if (min_doc > 0 or max_doc < 9999) else ""
    str_label    = f" | STR {min_str}–{max_str}%" if (min_str > 0 or max_str < 200) else ""
    filter_label = doc_label + str_label

    # ── Metrics ───────────────────────────────────────────────────────────────
    st.divider()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(f"Total Inventory{filter_label}", f"{table_df['inventory'].sum():,.0f} units")

    valid_doc = table_df[(table_df["doc"] > 0) & (table_df["doc"] < 9999)]
    if not valid_doc.empty and valid_doc["inventory"].sum() > 0:
        w_doc = (valid_doc["doc"] * valid_doc["inventory"]).sum() / valid_doc["inventory"].sum()
        m2.metric(f"Avg Days of Cover{filter_label}", f"{w_doc:.1f} days")
    else:
        m2.metric(f"Avg Days of Cover{filter_label}", "N/A")

    if not table_df.empty and table_df["inventory"].sum() > 0:
        w_str = (table_df["str"] * table_df["inventory"]).sum() / table_df["inventory"].sum()
        m3.metric(f"Avg Sell-Through %{filter_label}", f"{w_str:.2%}")
    else:
        m3.metric(f"Avg Sell-Through %{filter_label}", "0.00%")

    if not table_df.empty and table_df["units_sold"].sum() > 0:
        total_u = total_d = 0
        for ch, grp in table_df.groupby("channel"):
            total_u += grp["units_sold"].sum()
            total_d  = max(total_d, grp["n_days"].max())
        m4.metric(f"Avg DRR{filter_label}", f"{total_u/total_d:.2f} units/day" if total_d > 0 else "N/A")
    else:
        m4.metric(f"Avg DRR{filter_label}", "N/A")

    # ── Table ─────────────────────────────────────────────────────────────────
    st.subheader("📊 Inventory Performance by Location")
    total_rows, shown_rows = len(filtered_df), len(table_df)
    if action_active:
        parts = []
        if min_doc > 0 or max_doc < 9999: parts.append(f"DOC **{min_doc}–{max_doc}** days")
        if min_str > 0 or max_str < 200:  parts.append(f"STR **{min_str}–{max_str}%**")
        st.caption(f"🎯 Showing **{shown_rows}** of {total_rows} rows — filtered by {', '.join(parts)}")
    else:
        st.caption(f"Showing all **{total_rows}** rows. Use Actionable Filters in the sidebar to narrow down.")

    group_by = st.radio("Group by:", ["None", "Channel", "Product", "Location"],
                        horizontal=True, index=0, key="cp_groupby")

    def color_doc(val):
        if val < 7:  return "color: red; font-weight: bold"
        if val < 15: return "color: orange; font-weight: bold"
        return ""

    display_cols = [c for c in ["master_sku", "channel_sku", "channel", "location",
                                 "inventory", "drr", "doc", "str"] if c in table_df.columns]
    fmt = {"str": "{:.2%}", "doc": "{:.1f}", "inventory": "{:,.0f}", "drr": "{:.2f}"}

    if group_by == "None":
        st.dataframe(
            table_df[display_cols].sort_values("inventory", ascending=False)
            .style.format(fmt).map(color_doc, subset=["doc"]),
            use_container_width=True,
        )
    else:
        gcm     = {"Channel": "channel", "Product": "master_sku", "Location": "location"}
        grp_col = gcm[group_by]
        agg_df  = table_df.groupby(grp_col).agg(
            inventory=("inventory", "sum"), units_sold=("units_sold", "sum")
        ).reset_index()

        def _w_doc(grp):
            v = grp[(grp["doc"] > 0) & (grp["doc"] < 9999)]
            return (v["doc"] * v["inventory"]).sum() / v["inventory"].sum() \
                if not v.empty and v["inventory"].sum() > 0 else float("nan")

        def _w_str(grp):
            return (grp["str"] * grp["inventory"]).sum() / grp["inventory"].sum() \
                if grp["inventory"].sum() > 0 else 0.0

        def _g_drr(grp):
            # pandas 2.x drops the grouping key from the sub-frame when using
            # include_groups=False (or implicitly in newer versions).
            # Re-attach "channel" from the index if it has been removed.
            if "channel" not in grp.columns:
                grp = grp.copy()
                grp["channel"] = grp.index
            u = d = 0
            for ch, sub in grp.groupby("channel"):
                u += sub["units_sold"].sum()
                d  = max(d, sub["n_days"].max())
            return u / d if d > 0 else 0.0

        # pandas 2.2+ deprecates passing the grouping column into the applied
        # function; use include_groups=False to silence the warning and avoid
        # the KeyError that occurs in pandas built against Python 3.13.
        _apply_kwargs = {}
        try:
            import pandas as _pd
            if tuple(int(x) for x in _pd.__version__.split(".")[:2]) >= (2, 2):
                _apply_kwargs = {"include_groups": False}
        except Exception:
            pass

        agg_df = (
            agg_df
            .join(table_df.groupby(grp_col).apply(_w_doc, **_apply_kwargs).rename("doc"), on=grp_col)
            .join(table_df.groupby(grp_col).apply(_w_str, **_apply_kwargs).rename("str"), on=grp_col)
            .join(table_df.groupby(grp_col).apply(_g_drr, **_apply_kwargs).rename("drr"), on=grp_col)
            .sort_values("inventory", ascending=False).reset_index(drop=True)
        )
        st.dataframe(
            agg_df.style.format({**fmt, "units_sold": "{:,.1f}"})
            .map(color_doc, subset=["doc"]),
            use_container_width=True,
        )

    # ── Actionable Quadrants ──────────────────────────────────────────────────
    st.divider()
    st.subheader("🎯 Actionable Quadrants")
    DOC_THRESH, STR_THRESH = 90, 20

    quad_df           = filtered_df[(filtered_df["doc"] > 0) & (filtered_df["doc"] < 9999)].copy()
    quad_df["str_pct"] = quad_df["str"] * 100

    q1 = quad_df[(quad_df["str_pct"] >= STR_THRESH) & (quad_df["doc"] <  DOC_THRESH)].sort_values("doc")
    q2 = quad_df[(quad_df["str_pct"] <  STR_THRESH) & (quad_df["doc"] >= DOC_THRESH)].sort_values("doc", ascending=False)
    q3 = quad_df[(quad_df["str_pct"] >= STR_THRESH) & (quad_df["doc"] >= DOC_THRESH)].sort_values("doc", ascending=False)
    q4 = quad_df[(quad_df["str_pct"] <  STR_THRESH) & (quad_df["doc"] <  DOC_THRESH)].sort_values("doc")

    t1, t2, t3, t4 = st.tabs([
        f"🔴 Reorder Now ({len(q1)})",
        f"🟡 Run Promotion ({len(q2)})",
        f"🟢 Improve Visibility ({len(q3)})",
        f"⚫ SKU Rationalise ({len(q4)})",
    ])

    qdcols = [c for c in ["master_sku", "channel_sku", "channel", "location",
                           "inventory", "drr", "doc", "str"] if c in quad_df.columns]

    def _download(df, label, fname):
        export        = df[qdcols].copy()
        export["str"] = (export["str"] * 100).round(2).astype(str) + "%"
        export["doc"] = export["doc"].round(1)
        export["drr"] = export["drr"].round(2)
        st.download_button(f"⬇️ Download {label} as CSV",
                           export.to_csv(index=False).encode("utf-8"),
                           file_name=fname, mime="text/csv", use_container_width=True)

    def _summary(df):
        inv = df["inventory"].sum()
        return len(df), inv, (df["doc"] * df["inventory"]).sum() / inv if inv > 0 else 0

    for tab, qdf, label, fname, desc, action in [
        (t1, q1, "Reorder Now",        "reorder_now.csv",
         f"**High STR + Low DOC** — Hot sellers running out fast. STR ≥ {STR_THRESH}%, DOC < {DOC_THRESH}d.",
         "**Reorder immediately**; consider increasing next order size."),
        (t2, q2, "Run Promotion",      "run_promotion.csv",
         f"**Low STR + High DOC** — Slow movers; overstocked. STR < {STR_THRESH}%, DOC ≥ {DOC_THRESH}d.",
         "**Run a promotion**, bundle the item, or stop future orders."),
        (t3, q3, "Improve Visibility", "improve_visibility.csv",
         f"**High STR + High DOC** — High demand but oversupplied. STR ≥ {STR_THRESH}%, DOC ≥ {DOC_THRESH}d.",
         "**Improve visibility/merchandising** to maintain the high sales pace."),
        (t4, q4, "SKU Rationalise",    "sku_rationalise.csv",
         f"**Low STR + Low DOC** — Poor demand and low stock. STR < {STR_THRESH}%, DOC < {DOC_THRESH}d.",
         "Likely a candidate for **SKU rationalisation**."),
    ]:
        with tab:
            st.markdown(desc)
            st.caption(f"Action: {action}")
            if qdf.empty:
                st.success("✅ No SKUs in this quadrant.")
            else:
                n, inv, avg_doc = _summary(qdf)
                c1, c2, c3 = st.columns(3)
                c1.metric("SKU-locations", n)
                c2.metric("Total Inventory", f"{inv:,.0f} units")
                c3.metric("Avg DOC", f"{avg_doc:.1f} days")
                st.dataframe(
                    qdf[qdcols].reset_index(drop=True)
                    .style.format(fmt).map(color_doc, subset=["doc"]),
                    use_container_width=True,
                )
                _download(qdf, label, fname)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def render_channel_performance_tab(supabase_client, master_skus_df: pd.DataFrame, role: str):
    st.subheader("🛡️ Channel Performance — Inventory Control Hub")
    st.caption(
        "Upload inventory files per channel. "
        "Sales data (qty + city) is pulled automatically from the Sales Tracker database."
    )

    # ── PostgreSQL for SKU mapping persistence ────────────────────────────────
    # Load SKU mappings from Supabase (same connection already in use)
    db_mappings  = _load_mappings(supabase_client)
    # Load item_map — Smart Upload's ASIN→master_sku table for Amazon
    item_map_df  = _load_item_map(supabase_client)

    master_list = (
        master_skus_df["name"].unique().tolist()
        if not master_skus_df.empty and "name" in master_skus_df.columns else []
    )
    if not master_list:
        st.warning("No master SKUs found. Add SKUs in the Configuration tab first.")
        return

    # ── Sales window ──────────────────────────────────────────────────────────
    st.markdown("#### 📅 Sales Window")
    sales_days = st.selectbox(
        "Pull sales from last N days (used for DRR / DOC calculation)",
        [7, 14, 30, 60, 90], index=2, key="cp_sales_days",
    )

    with st.spinner("Fetching sales data from database…"):
        raw_sales = _get_sales(supabase_client, sales_days)

    n_days = sales_days

    if raw_sales.empty:
        st.info("No sales data found for the selected window. Upload sales via Smart Upload first.")
    else:
        city_rows    = raw_sales[raw_sales["city"].notna() & (raw_sales["city"].str.strip() != "")]
        no_city_rows = raw_sales[raw_sales["city"].isna()  | (raw_sales["city"].str.strip() == "")]
        st.success(
            f"✅ {len(raw_sales):,} sales records loaded "
            f"({raw_sales['date'].min().date()} → {raw_sales['date'].max().date()}) — "
            f"**{len(city_rows):,} city-tagged**, {len(no_city_rows):,} national/legacy"
        )

    # ── Load saved snapshots ──────────────────────────────────────────────────
    saved_snapshots = _load_snapshots(supabase_client)

    # ── Inventory file uploads ────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📥 Upload Inventory Reports")
    st.caption(
        "Upload a fresh inventory export to refresh a channel. "
        "If no file is uploaded, the last saved snapshot is used automatically."
    )

    f_types = ["csv", "xlsx", "xls"]

    CHANNELS = [
        ("Amazon",     "cp_amz_i", 1),
        ("Blinkit",    "cp_blk_i", 2),
        ("Swiggy",     "cp_swg_i", 0),
        ("Big Basket", "cp_bb_i",  0),
    ]

    cols = st.columns(4)
    uploaders = {}
    for i, (ch_name, key, _) in enumerate(CHANNELS):
        with cols[i]:
            snap_info = saved_snapshots.get(ch_name)
            if snap_info:
                st.success(f"**{ch_name}**")
                st.caption(f"Last upload: {snap_info[0]}")
            else:
                st.info(f"**{ch_name}**")
                st.caption("No snapshot saved yet")
            uploaders[ch_name] = st.file_uploader(
                f"{ch_name} Inventory", type=f_types, key=key
            )

    # ── Parse freshly uploaded files ──────────────────────────────────────────
    freshly_parsed = {}   # channel → parsed DataFrame (from new upload)

    ch_parse_config = {
        "Amazon":     (lambda f: _parse_amazon(
                            _load_file(f, skiprows=1),
                            _channel_sales(raw_sales, "amazon"), n_days, db_mappings,
                            item_map_df)),
        "Blinkit":    (lambda f: _parse_blinkit(
                            _load_file(f, skiprows=2),
                            _channel_sales(raw_sales, "blinkit"), n_days, db_mappings)),
        "Swiggy":     (lambda f: _parse_swiggy(
                            _load_file(f),
                            _channel_sales(raw_sales, "swiggy"), n_days, db_mappings)),
        "Big Basket": (lambda f: _parse_bigbasket(
                            _load_file(f),
                            _channel_sales(raw_sales, "big basket"), n_days, db_mappings)),
    }

    for ch_name, up_file in uploaders.items():
        if up_file:
            try:
                parsed = ch_parse_config[ch_name](up_file)
                parsed["channel"] = ch_name
                freshly_parsed[ch_name] = parsed
                # Save snapshot to Supabase immediately after successful parse
                _save_snapshot(supabase_client, parsed, ch_name)
                st.toast(f"✅ {ch_name} snapshot saved", icon="💾")
            except Exception as e:
                st.error(f"{ch_name} parse error: {e}")

    # ── Combine: fresh uploads + snapshots for channels not uploaded ──────────
    uploaded_data = []
    snapshot_channels_used = []

    for ch_name in [c[0] for c in CHANNELS]:
        if ch_name in freshly_parsed:
            # Fresh upload takes priority
            uploaded_data.append(freshly_parsed[ch_name])
        elif ch_name in saved_snapshots:
            # Use saved snapshot — re-apply current sales data to it
            _, snap_df = saved_snapshots[ch_name]
            snap_df = snap_df.copy()
            snap_df["channel"] = ch_name
            # Re-compute DRR/DOC/STR from current sales window against saved inventory
            snap_df = _reapply_sales(snap_df, raw_sales, ch_name, db_mappings, n_days)
            uploaded_data.append(snap_df)
            snapshot_channels_used.append(ch_name)

    if not uploaded_data:
        st.info(
            "No inventory data available. "
            "Upload at least one inventory file to generate the dashboard."
        )
        return

    # Show which channels are using saved snapshots
    if snapshot_channels_used:
        snap_dates = []
        for ch in snapshot_channels_used:
            snap_dates.append(f"**{ch}** ({saved_snapshots[ch][0]})")
        st.info(
            f"📦 Using saved snapshots for: {', '.join(snap_dates)}. "
            "Upload a new file above to refresh any channel."
        )

    # ── SKU mapping ───────────────────────────────────────────────────────────
    combined              = pd.concat(uploaded_data, ignore_index=True)
    combined["channel_sku"] = combined["channel_sku"].astype(str)
    if not db_mappings.empty:
        db_mappings["channel_sku"] = db_mappings["channel_sku"].astype(str)

    merged   = combined.merge(db_mappings, on=["channel", "channel_sku"], how="left")
    unmapped = merged[merged["master_sku"].isna()][["channel", "channel_sku"]].drop_duplicates()

    if not unmapped.empty:
        st.warning(f"🚨 {len(unmapped)} new channel SKUs found. Map them to master SKUs to continue.")
        with st.form("cp_map_form"):
            new_entries = []
            for _, row in unmapped.iterrows():
                choice = st.selectbox(
                    f"{row['channel']}: `{row['channel_sku']}`",
                    ["Select…"] + master_list,
                    key=f"cp_map_{row['channel']}_{row['channel_sku']}",
                )
                if choice != "Select…":
                    new_entries.append({
                        "channel":     row["channel"],
                        "channel_sku": row["channel_sku"],
                        "master_sku":  choice,
                    })
            if st.form_submit_button("💾 Save & Sync"):
                if new_entries:
                    try:
                        _save_mappings(supabase_client, new_entries)
                        st.success(f"Saved {len(new_entries)} mappings.")
                    except Exception as e:
                        st.error(f"Failed to save mappings: {e}")
                st.rerun()
        return

    _render_dashboard(merged)
