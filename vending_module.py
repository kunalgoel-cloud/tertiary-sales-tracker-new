"""
vending_module.py
─────────────────────────────────────────────────────────────────────────────
Vending Performance Hub — rendered as a tab inside the main Sales Tracker app.

Call:  render_vending_tab(role)

Persistence uses a JSON file on the Streamlit Cloud filesystem.
The file survives across reruns but resets on redeploy (Streamlit Cloud
ephemeral filesystem). For production persistence, migrate load_db / save_db
to a Supabase table.
"""

import json
import os
import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

_DB_PATH = "vending_database.json"

MONTHS = ["Jan","Feb","Mar","Apr","May","Jun",
          "Jul","Aug","Sep","Oct","Nov","Dec"]

CITY_ALIASES = {
    "gurgaon":           "Gurgaon",
    "cochin airport":    "Cochin Airport",
    "goa airport":       "Goa Airport",
    "hyderabad airport": "Hyderabad Airport",
    "mumbai airport":    "Mumbai Airport",
    "new delhi":         "New Delhi",
    "bangalore":         "Bangalore",
    "chandigarh":        "Chandigarh",
    "chennai":           "Chennai",
    "cochin":            "Cochin",
    "dolvi":             "Dolvi",
    "hyderabad":         "Hyderabad",
    "mumbai":            "Mumbai",
    "noida":             "Noida",
    "pune":              "Pune",
}

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE  — all keys namespaced with "vnd_" to avoid conflicts
# ─────────────────────────────────────────────────────────────────────────────

def _init_state():
    defaults = {
        "vnd_analysis_generated": False,
        "vnd_processed_df":       None,
        "vnd_price_map":          {},
        "vnd_db":                 None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE  (JSON file — replace with Supabase for cloud persistence)
# ─────────────────────────────────────────────────────────────────────────────

def _load_db() -> dict:
    if os.path.exists(_DB_PATH):
        try:
            with open(_DB_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_db(db: dict):
    with open(_DB_PATH, "w") as f:
        json.dump(db, f, indent=2)


def _db_key(customer: str, month: str, year: int) -> str:
    return f"{customer}__{month}__{year}"


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_city(city_str: str) -> str:
    return CITY_ALIASES.get(str(city_str).strip().lower(), str(city_str).strip())


def _normalize_product(prod_str: str) -> str:
    """Strip leading 'Airport ' prefix caused by data-entry errors."""
    p = str(prod_str).strip()
    return p[8:] if p.lower().startswith("airport ") else p


def _extract_city(loc_str: str, known_cities: list) -> str:
    """Longest-match city extraction from a warehouse/zone string."""
    loc_l = str(loc_str).lower()
    for city in sorted(known_cities, key=len, reverse=True):
        if city.lower() in loc_l:
            return city
    fallback = str(loc_str).strip().split()[0].title()
    return _normalize_city(fallback)


# ─────────────────────────────────────────────────────────────────────────────
# CORE FILE PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

def _process_master_file(uploaded_file) -> pd.DataFrame | None:
    try:
        all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
        s_map = {k.lower().strip(): k for k in all_sheets}

        req = ["sales summary", "soh", "machine placement"]
        missing = [r for r in req if r not in s_map]
        if missing:
            st.error(f"Missing required sheets: {missing}")
            return None

        # ── Sales Summary ──────────────────────────────────────────────
        sales_raw = all_sheets[s_map["sales summary"]].iloc[1:, 0:3].copy()
        sales_raw.columns = ["City", "Product", "Sales_Qty"]
        sales_raw = sales_raw[
            sales_raw["City"].notna() &
            ~sales_raw["City"].astype(str).str.lower().str.contains("total", na=False)
        ].copy()
        sales_raw["City"]      = sales_raw["City"].apply(_normalize_city)
        sales_raw["Product"]   = sales_raw["Product"].apply(_normalize_product)
        sales_raw["Sales_Qty"] = pd.to_numeric(sales_raw["Sales_Qty"], errors="coerce").fillna(0)
        sales = sales_raw.groupby(["City", "Product"])["Sales_Qty"].sum().reset_index()

        # ── Machine Placement ──────────────────────────────────────────
        mach_raw = all_sheets[s_map["machine placement"]].iloc[1:, 0:3].copy()
        mach_raw.columns = ["City", "Product", "Machine_Count"]
        mach_raw = mach_raw[
            mach_raw["City"].notna() &
            ~mach_raw["City"].astype(str).str.lower().str.contains("total", na=False)
        ].copy()
        mach_raw["City"]          = mach_raw["City"].apply(_normalize_city)
        mach_raw["Product"]       = mach_raw["Product"].apply(_normalize_product)
        mach_raw["Machine_Count"] = pd.to_numeric(mach_raw["Machine_Count"], errors="coerce").fillna(0)
        mach = mach_raw.groupby(["City", "Product"])["Machine_Count"].sum().reset_index()

        # ── SOH ────────────────────────────────────────────────────────
        soh_raw = all_sheets[s_map["soh"]].iloc[1:, [0, 1, 4]].copy()
        soh_raw.columns = ["Loc", "Product", "Total_SOH"]
        soh_raw = soh_raw[
            soh_raw["Loc"].notna() &
            ~soh_raw["Loc"].astype(str).str.lower().str.contains("total", na=False)
        ].copy()
        soh_raw["Product"]   = soh_raw["Product"].apply(_normalize_product)
        soh_raw["Total_SOH"] = pd.to_numeric(soh_raw["Total_SOH"], errors="coerce").fillna(0)

        known_cities = list(set(sales["City"].tolist() + mach["City"].tolist()))
        soh_raw["City"] = soh_raw["Loc"].apply(lambda x: _extract_city(x, known_cities))
        soh_raw["City"] = soh_raw["City"].replace({
            "Goa Zone 1(Mw)": "Goa Airport",
            "Goa Zone 1(MW)": "Goa Airport",
            "Goa":            "Goa Airport",
        })
        soh = soh_raw.groupby(["City", "Product"])["Total_SOH"].sum().reset_index()

        # ── Merge (outer so no rows are silently dropped) ──────────────
        df = pd.merge(mach, sales, on=["City", "Product"], how="outer")
        df = pd.merge(df,   soh,   on=["City", "Product"], how="outer")

        for c in ["Sales_Qty", "Total_SOH", "Machine_Count"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

        days = 30
        df["drr"]      = df["Sales_Qty"] / days
        df["velocity"] = np.where(
            df["Machine_Count"] > 0,
            (df["Sales_Qty"] / df["Machine_Count"]) / days,
            0,
        )
        df["str_pct"]      = np.where(
            (df["Sales_Qty"] + df["Total_SOH"]) > 0,
            df["Sales_Qty"] / (df["Sales_Qty"] + df["Total_SOH"]) * 100,
            0,
        )
        df["days_of_cover"] = np.where(df["drr"] > 0, df["Total_SOH"] / df["drr"], 999)

        df["rank"]      = df["velocity"].rank(pct=True)
        df["abc_class"] = np.where(df["rank"] > 0.8, "A",
                          np.where(df["rank"] > 0.5, "B", "C"))
        df = df.drop(columns=["rank"])
        return df

    except Exception as e:
        st.error(f"Processing Error: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD RENDERER
# ─────────────────────────────────────────────────────────────────────────────

def _render_analysis(df_raw: pd.DataFrame, price_map: dict,
                     target_cust: str, sel_month: str, sel_year: int, db: dict):
    df = df_raw.copy()
    df["unit_price"] = df["Product"].map(price_map).fillna(0)
    df["sales_val"]  = df["Sales_Qty"] * df["unit_price"]
    df["soh_val"]    = df["Total_SOH"] * df["unit_price"]

    # Filters
    st.subheader("🔍 Analysis Filters")
    f1, f2 = st.columns(2)
    sel_cities = f1.multiselect("Filter by City",    sorted(df["City"].unique()),
                                 default=sorted(df["City"].unique()),    key="vnd_f_cities")
    sel_prods  = f2.multiselect("Filter by Product", sorted(df["Product"].unique()),
                                 default=sorted(df["Product"].unique()), key="vnd_f_prods")
    fdf = df[(df["City"].isin(sel_cities)) & (df["Product"].isin(sel_prods))]

    if fdf.empty:
        st.warning("No data found for selected filters.")
        return

    st.subheader(f"📈 Results — {target_cust} ({sel_month} {sel_year})")

    # KPIs
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Sales (Qty)", f"{fdf['Sales_Qty'].sum():,.0f}")
    m1.caption(f"₹{fdf['sales_val'].sum():,.0f}")
    active  = fdf[fdf["Sales_Qty"] > 0]
    avg_vel = active["velocity"].mean() if not active.empty else 0
    m2.metric("Avg Daily Velocity", f"{avg_vel:.2f}")
    m3.metric("Total SOH (Qty)",    f"{fdf['Total_SOH'].sum():,.0f}")
    m3.caption(f"₹{fdf['soh_val'].sum():,.0f}")
    m4.metric("Total Machines",     f"{fdf['Machine_Count'].sum():,.0f}")
    finite_doc = fdf[fdf["days_of_cover"] < 999]["days_of_cover"]
    avg_doc    = finite_doc.mean() if not finite_doc.empty else float("nan")
    m5.metric("Avg Days of Cover",  f"{avg_doc:.1f}" if not np.isnan(avg_doc) else "∞")

    fmt = {
        "drr": "{:.1f}", "days_of_cover": "{:.1f}", "str_pct": "{:.1f}",
        "velocity": "{:.2f}", "Machine_Count": "{:,.0f}",
        "Sales_Qty": "{:,.0f}", "Total_SOH": "{:,.0f}",
    }

    t1, t2, t3, t4 = st.tabs([
        "📦 Inventory Analysis",
        "🤖 Machine Performance",
        "📊 Charts",
        "📈 Trend Lines",
    ])

    with t1:
        st.dataframe(
            fdf[["City","Product","Total_SOH","drr","days_of_cover","str_pct"]]
            .style.format(fmt).background_gradient(subset=["str_pct"], cmap="RdYlGn"),
            use_container_width=True,
        )

    with t2:
        st.dataframe(
            fdf[["City","Product","Machine_Count","Sales_Qty","velocity","abc_class"]]
            .style.format(fmt).background_gradient(subset=["velocity"], cmap="YlGn"),
            use_container_width=True,
        )

    with t3:
        c1, c2 = st.columns(2)
        with c1:
            city_s = fdf.groupby("City")["Sales_Qty"].sum().reset_index().sort_values("Sales_Qty", ascending=False)
            st.plotly_chart(
                px.bar(city_s, x="City", y="Sales_Qty", title="Sales by City",
                       color="Sales_Qty", color_continuous_scale="Blues"),
                use_container_width=True,
            )
        with c2:
            prod_soh = fdf.groupby("Product")["Total_SOH"].sum().reset_index()
            st.plotly_chart(
                px.pie(prod_soh, names="Product", values="Total_SOH",
                       title="SOH Distribution by Product"),
                use_container_width=True,
            )
        c3, c4 = st.columns(2)
        with c3:
            abc_d = fdf["abc_class"].value_counts().reset_index()
            abc_d.columns = ["Class", "Count"]
            st.plotly_chart(
                px.bar(abc_d, x="Class", y="Count", title="ABC Classification",
                       color="Class",
                       color_discrete_map={"A": "#2ecc71", "B": "#f39c12", "C": "#e74c3c"}),
                use_container_width=True,
            )
        with c4:
            vel_top = fdf[fdf["velocity"] > 0].sort_values("velocity", ascending=False).head(15)
            st.plotly_chart(
                px.bar(vel_top, x="velocity", y="Product", orientation="h",
                       title="Top 15 Products by Velocity",
                       color="velocity", color_continuous_scale="Greens"),
                use_container_width=True,
            )

    with t4:
        st.markdown("### 📈 Trend Line Analysis")
        st.info("Select historical data points to plot trends. Save multiple months first.")

        history_records = []
        for key, saved in db.items():
            parts = key.split("__")
            if len(parts) == 3:
                cust, mo, yr = parts
                h = pd.DataFrame(saved["data"])
                h["_customer"], h["_month"], h["_year"] = cust, mo, int(yr)
                history_records.append(h)

        if len(history_records) < 2:
            st.warning("Need at least **2 saved analyses** to plot trends.")
        else:
            all_hist  = pd.concat(history_records, ignore_index=True)
            mo_order  = {m: i for i, m in enumerate(MONTHS)}
            all_hist["_sort"] = all_hist["_year"] * 100 + all_hist["_month"].map(mo_order)

            tf1, tf2, tf3 = st.columns(3)
            t_cities  = tf1.multiselect("Cities",   sorted(all_hist["City"].unique()),
                                         default=sorted(all_hist["City"].unique())[:3],
                                         key="vnd_t_cities")
            t_prods   = tf2.multiselect("Products", sorted(all_hist["Product"].unique()),
                                         default=sorted(all_hist["Product"].unique())[:2],
                                         key="vnd_t_prods")
            t_metric  = tf3.selectbox("Metric",
                                       ["Sales_Qty","Total_SOH","velocity","str_pct","days_of_cover"],
                                       key="vnd_t_metric")

            pts = sorted(
                all_hist[["_year","_month"]].drop_duplicates()
                .apply(lambda r: f"{r['_month']} {r['_year']}", axis=1).tolist(),
                key=lambda x: int(x.split()[1]) * 100 + mo_order.get(x.split()[0], 0),
            )
            sel_pts = st.multiselect("Data points", pts, default=pts, key="vnd_t_pts")

            if sel_pts and t_cities and t_prods:
                trend_df = all_hist[
                    all_hist["City"].isin(t_cities) & all_hist["Product"].isin(t_prods)
                ].copy()
                trend_df["period"] = trend_df["_month"] + " " + trend_df["_year"].astype(str)
                trend_df = trend_df[trend_df["period"].isin(sel_pts)].sort_values("_sort")
                trend_df[t_metric] = pd.to_numeric(trend_df[t_metric], errors="coerce")
                agg = (trend_df.groupby(["period","_sort","City","Product"])[t_metric]
                       .sum().reset_index().sort_values("_sort"))
                st.plotly_chart(
                    px.line(agg, x="period", y=t_metric, color="City",
                            line_dash="Product", title=f"Trend: {t_metric}", markers=True),
                    use_container_width=True,
                )
            else:
                st.info("Select at least one city, product, and data point.")

    # Save button
    st.markdown("---")
    sv1, sv2 = st.columns([3, 1])
    sv1.markdown(f"**Save this analysis:** `{target_cust} | {sel_month} {sel_year}`")
    if sv2.button("💾 Save to Database", type="primary",
                  use_container_width=True, key="vnd_save_btn"):
        key = _db_key(target_cust, sel_month, sel_year)
        db[key] = {
            "customer": target_cust, "month": sel_month, "year": sel_year,
            "saved_at": datetime.datetime.now().isoformat(),
            "price_map": price_map,
            "data": df_raw.to_dict(orient="records"),
        }
        _save_db(db)
        st.session_state["vnd_db"] = db
        st.success(f"✅ Saved: {target_cust} — {sel_month} {sel_year}")
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def render_vending_tab(role: str):
    """
    Renders the full Vending Performance Hub inside a tab.
    Call this from the main app's tab context:
        with tabs[_TAB_VENDING]:
            render_vending_tab(role)
    """
    _init_state()

    st.subheader("📊 Vending Performance Hub")
    st.caption(
        "Upload monthly Excel workbooks (Sales Summary, SOH, Machine Placement sheets) "
        "to analyse vending channel performance. Save analyses to compare trends over time."
    )

    # ── Saved analyses panel ─────────────────────────────────────────────
    db = _load_db()
    st.session_state["vnd_db"] = db

    with st.expander("📁 Saved Analyses", expanded=bool(db)):
        if db:
            keys   = list(db.keys())
            labels = [k.replace("__", " | ") for k in keys]

            sv_col1, sv_col2, sv_col3 = st.columns([3, 1, 1])
            chosen_label = sv_col1.selectbox("Open saved analysis",
                                              ["— select —"] + labels,
                                              key="vnd_open_sel")
            if sv_col2.button("📂 Load", use_container_width=True, key="vnd_load_btn"):
                if chosen_label != "— select —":
                    chosen_key = keys[labels.index(chosen_label)]
                    saved = db[chosen_key]
                    st.session_state["vnd_processed_df"]       = pd.DataFrame(saved["data"])
                    st.session_state["vnd_price_map"]          = saved["price_map"]
                    st.session_state["vnd_analysis_generated"] = True
                    st.rerun()
            if sv_col3.button("🗑 Reset", use_container_width=True, key="vnd_reset_btn"):
                st.session_state["vnd_analysis_generated"] = False
                st.session_state["vnd_processed_df"]       = None
                st.rerun()
        else:
            st.info("No saved analyses yet. Upload and save a report below.")

    st.divider()

    # ── Upload controls ───────────────────────────────────────────────────
    uc1, uc2, uc3 = st.columns([2, 1, 1])
    target_cust = uc1.selectbox("Customer", ["Vendiman", "External Partner"],
                                 key="vnd_cust")
    sel_month   = uc2.selectbox("Month", MONTHS,
                                 index=datetime.datetime.now().month - 1,
                                 key="vnd_month")
    sel_year    = uc3.selectbox("Year", list(range(2024, 2032)),
                                 index=datetime.datetime.now().year - 2024,
                                 key="vnd_year")

    file = st.file_uploader("Upload Excel Workbook (.xlsx)",
                             type="xlsx", key="vnd_uploader")

    if file:
        if st.session_state["vnd_processed_df"] is None:
            with st.spinner("Processing file…"):
                st.session_state["vnd_processed_df"] = _process_master_file(file)

        if st.session_state["vnd_processed_df"] is not None:
            st.markdown("---")
            st.subheader("💰 Step 2: Item-Wise Price Entry")
            st.info("Set the MRP for each product (used for value-based metrics).")

            unique_prods = sorted(st.session_state["vnd_processed_df"]["Product"].unique())
            price_init   = pd.DataFrame({"Product": unique_prods, "Price_per_Unit": 0.0})
            edited       = st.data_editor(price_init, use_container_width=True,
                                           hide_index=True, key="vnd_price_editor")

            if st.button("🚀 Generate Performance Analysis", type="primary",
                          key="vnd_gen_btn"):
                st.session_state["vnd_analysis_generated"] = True
                st.session_state["vnd_price_map"] = dict(
                    zip(edited["Product"], edited["Price_per_Unit"])
                )

    # ── Dashboard ─────────────────────────────────────────────────────────
    if (st.session_state["vnd_analysis_generated"]
            and st.session_state["vnd_processed_df"] is not None):
        st.markdown("---")
        _render_analysis(
            df_raw=st.session_state["vnd_processed_df"],
            price_map=st.session_state["vnd_price_map"],
            target_cust=target_cust,
            sel_month=sel_month,
            sel_year=int(sel_year),
            db=st.session_state["vnd_db"],
        )
    elif not file:
        st.info("Upload your vending report (.xlsx) above to begin.")
