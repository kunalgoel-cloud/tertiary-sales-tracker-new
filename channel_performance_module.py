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

# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL helpers — SKU mapping persistence
# ─────────────────────────────────────────────────────────────────────────────

def _get_pg_engine():
    """Returns a psycopg2 connection URL string, or None if not configured."""
    try:
        url = st.secrets.get("connections", {})
        if not url:
            return None
        url = url.get("postgresql", {})
        if not url:
            return None
        return url.get("url", None)
    except Exception:
        return None


def _init_pg(conn_url):
    import psycopg2
    with psycopg2.connect(conn_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sku_mappings (
                    id SERIAL PRIMARY KEY,
                    channel TEXT NOT NULL,
                    channel_sku TEXT NOT NULL,
                    master_sku TEXT NOT NULL,
                    UNIQUE(channel, channel_sku)
                );
            """)
        conn.commit()


def _load_mappings(conn_url) -> pd.DataFrame:
    import psycopg2
    try:
        with psycopg2.connect(conn_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT channel, channel_sku, master_sku FROM sku_mappings")
                rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["channel", "channel_sku", "master_sku"]).astype(str)
    except Exception:
        return pd.DataFrame(columns=["channel", "channel_sku", "master_sku"])


def _save_mappings(conn_url, new_entries):
    import psycopg2
    with psycopg2.connect(conn_url) as conn:
        with conn.cursor() as cur:
            for entry in new_entries:
                cur.execute("""
                    INSERT INTO sku_mappings (channel, channel_sku, master_sku)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (channel, channel_sku)
                    DO UPDATE SET master_sku = EXCLUDED.master_sku
                """, (entry["channel"], entry["channel_sku"], entry["master_sku"]))
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Supabase sales fetch
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
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

def _parse_amazon(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
    inv_df = inv_df.copy()
    sku_c  = _find_col(inv_df, ["ASIN", "asin", "sku"])
    inv_df["channel_sku"] = inv_df[sku_c].astype(str).str.strip() if sku_c else ""
    inv_df["inventory"]   = pd.to_numeric(inv_df["Sellable On Hand Units"], errors="coerce").fillna(0)
    inv_df["location"]    = "National"

    inv_df["str"] = 0.0
    if "Sell-Through %" in inv_df.columns:
        inv_df["str"] = (
            pd.to_numeric(
                inv_df["Sell-Through %"].astype(str).str.replace("%", ""), errors="coerce"
            ).fillna(0) / 100
        )

    if not sales_df.empty:
        nat = (
            sales_df[sales_df["city"] == "__national__"]
            .groupby("item_name")[["qty_sold"]]
            .sum()
            .reset_index()
            .rename(columns={"qty_sold": "units_sold"})
        )
        inv_df = inv_df.merge(nat, left_on="channel_sku", right_on="item_name", how="left").fillna(0)
        sales_val = pd.to_numeric(inv_df["units_sold"], errors="coerce").fillna(0)
    else:
        sales_val = pd.Series(0.0, index=inv_df.index)

    inv_df["drr"]        = (sales_val / n_days).round(2)
    inv_df["doc"]        = inv_df["inventory"] / inv_df["drr"].replace(0, 0.001)
    inv_df["units_sold"] = sales_val
    inv_df["n_days"]     = n_days
    return inv_df[["channel_sku", "inventory", "str", "doc", "drr", "units_sold", "n_days", "location"]]


def _parse_blinkit(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
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

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "city", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["channel_sku", "_city_key"],
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


def _parse_swiggy(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
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

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        city_sales["_city_upper"] = city_sales["city"].astype(str).str.strip().str.upper()

        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "_city_upper", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["channel_sku", "_city_key"],
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


def _parse_bigbasket(inv_df: pd.DataFrame, sales_df: pd.DataFrame, n_days: int) -> pd.DataFrame:
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

    if not sales_df.empty:
        city_sales = sales_df[sales_df["city"] != "__national__"].copy()
        if not city_sales.empty:
            inv_df = inv_df.merge(
                city_sales[["item_name", "city", "qty_sold"]].rename(columns={"qty_sold": "units_sold"}),
                left_on=["channel_sku", "_city_key"],
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
            .style.format(fmt).applymap(color_doc, subset=["doc"]),
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
            u = d = 0
            for ch, sub in grp.groupby("channel"):
                u += sub["units_sold"].sum()
                d  = max(d, sub["n_days"].max())
            return u / d if d > 0 else 0.0

        agg_df = (
            agg_df
            .join(table_df.groupby(grp_col).apply(_w_doc).rename("doc"), on=grp_col)
            .join(table_df.groupby(grp_col).apply(_w_str).rename("str"), on=grp_col)
            .join(table_df.groupby(grp_col).apply(_g_drr).rename("drr"), on=grp_col)
            .sort_values("inventory", ascending=False).reset_index(drop=True)
        )
        st.dataframe(
            agg_df.style.format({**fmt, "units_sold": "{:,.1f}"})
            .applymap(color_doc, subset=["doc"]),
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
                    .style.format(fmt).applymap(color_doc, subset=["doc"]),
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
    pg_url       = _get_pg_engine()  # returns URL string or None
    pg_available = pg_url is not None

    if not pg_available:
        st.warning(
            "⚠️ PostgreSQL not configured — SKU mappings will not persist across sessions. "
            "Add `[connections.postgresql]\nurl = \"postgresql://...\"` to Streamlit Secrets."
        )
    else:
        try:
            _init_pg(pg_url)
        except Exception as e:
            st.warning(f"Could not initialise mapping table: {e}")
            pg_available = False

    db_mappings = _load_mappings(pg_url) if pg_available else pd.DataFrame(
        columns=["channel", "channel_sku", "master_sku"]
    )

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

    # ── Inventory file uploads ────────────────────────────────────────────────
    st.divider()
    st.markdown("#### 📥 Upload Inventory Reports")
    st.caption("Upload the inventory export for each channel. Sales data is matched automatically.")

    f_types = ["csv", "xlsx", "xls"]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.info("**Amazon**")
        amz_inv = st.file_uploader("Amazon Inventory", type=f_types, key="cp_amz_i")
    with c2:
        st.info("**Blinkit**")
        blk_inv = st.file_uploader("Blinkit Inventory", type=f_types, key="cp_blk_i")
    with c3:
        st.info("**Swiggy**")
        swg_inv = st.file_uploader("Swiggy Inventory", type=f_types, key="cp_swg_i")
    with c4:
        st.info("**Big Basket**")
        bb_inv  = st.file_uploader("Big Basket Inventory", type=f_types, key="cp_bb_i")

    if not any([amz_inv, blk_inv, swg_inv, bb_inv]):
        st.info("Upload at least one inventory file to generate the dashboard.")
        return

    # ── Parse ─────────────────────────────────────────────────────────────────
    uploaded_data = []

    if amz_inv:
        try:
            parsed = _parse_amazon(_load_file(amz_inv, skiprows=1),
                                   _channel_sales(raw_sales, "amazon"), n_days)
            parsed["channel"] = "Amazon"
            uploaded_data.append(parsed)
        except Exception as e:
            st.error(f"Amazon parse error: {e}")

    if blk_inv:
        try:
            parsed = _parse_blinkit(_load_file(blk_inv, skiprows=2),
                                    _channel_sales(raw_sales, "blinkit"), n_days)
            parsed["channel"] = "Blinkit"
            uploaded_data.append(parsed)
        except Exception as e:
            st.error(f"Blinkit parse error: {e}")

    if swg_inv:
        try:
            parsed = _parse_swiggy(_load_file(swg_inv),
                                   _channel_sales(raw_sales, "swiggy"), n_days)
            parsed["channel"] = "Swiggy"
            uploaded_data.append(parsed)
        except Exception as e:
            st.error(f"Swiggy parse error: {e}")

    if bb_inv:
        try:
            parsed = _parse_bigbasket(_load_file(bb_inv),
                                      _channel_sales(raw_sales, "big basket"), n_days)
            parsed["channel"] = "Big Basket"
            uploaded_data.append(parsed)
        except Exception as e:
            st.error(f"Big Basket parse error: {e}")

    if not uploaded_data:
        st.error("No data could be parsed. Check file formats.")
        return

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
                if pg_available and new_entries:
                    try:
                        _save_mappings(pg_url, new_entries)
                        st.success(f"Saved {len(new_entries)} mappings.")
                    except Exception as e:
                        st.error(f"Failed to save: {e}")
                elif new_entries:
                    if "cp_mem_mappings" not in st.session_state:
                        st.session_state["cp_mem_mappings"] = []
                    st.session_state["cp_mem_mappings"].extend(new_entries)
                    st.info("Mappings saved in memory (PostgreSQL not configured).")
                st.rerun()
        return

    _render_dashboard(merged)
