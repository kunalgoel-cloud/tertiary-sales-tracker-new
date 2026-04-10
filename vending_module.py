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

# ── Global Filter Notes ───────────────────────────────────────────────────────
# Vending does NOT consume the global date/channel filter.
# Reason: vending data is uploaded as monthly Excel workbooks (not from the
# shared sales DB) and the user selects month/year per uploaded workbook.
# The customer selector and month/year are dataset identifiers, not view
# filters — globalizing them would conflict with the upload workflow.
# This tab is intentionally self-contained.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

_DB_PATH          = "vending_database.json"
_PRICE_TABLE      = "vending_sku_prices"   # Supabase table for persistent prices

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
# SKU PRICE PERSISTENCE  (Supabase — survives redeployment)
# Table schema:  vending_sku_prices (product TEXT PRIMARY KEY, price FLOAT)
# ─────────────────────────────────────────────────────────────────────────────

def _get_supabase():
    """Return the shared Supabase client from st.session_state, or None."""
    return st.session_state.get("_vnd_supabase", None)


def _attach_supabase(supabase_client):
    """Called from the main app to give the vending module access to Supabase."""
    st.session_state["_vnd_supabase"] = supabase_client


def _load_prices_from_db() -> dict:
    """Load all SKU prices from Supabase.  Returns {} on any failure."""
    sb = _get_supabase()
    if sb is None:
        return {}
    try:
        res = sb.table(_PRICE_TABLE).select("product,price").execute()
        if res.data:
            return {row["product"]: float(row["price"]) for row in res.data}
    except Exception:
        pass
    return {}


def _save_prices_to_db(price_map: dict):
    """Upsert SKU prices into Supabase.  Silently skips on failure."""
    sb = _get_supabase()
    if sb is None:
        return
    try:
        rows = [{"product": k, "price": float(v)} for k, v in price_map.items()]
        if rows:
            sb.table(_PRICE_TABLE).upsert(rows, on_conflict="product").execute()
    except Exception:
        pass


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
        "drr": "{:.2f}", "days_of_cover": "{:.1f}", "str_pct": "{:.1f}",
        "velocity": "{:.2f}", "Machine_Count": "{:,.0f}",
        "Sales_Qty": "{:,.0f}", "Total_SOH": "{:,.0f}",
    }

    t1, t2, t3, t4, t5 = st.tabs([
        "📦 Inventory Analysis",
        "🤖 Machine Performance",
        "🔬 Deep Dive",
        "📈 Trend Lines",
        "⚙️ SKU Config",
    ])

    with t1:
        st.dataframe(
            fdf[["City","Product","Total_SOH","drr","days_of_cover","str_pct"]]
            .style.format(fmt).bar(subset=["str_pct"], color=["#f87171","#4ade80"], vmin=0, vmax=100),
            use_container_width=True,
        )

    with t2:
        st.dataframe(
            fdf[["City","Product","Machine_Count","Sales_Qty","velocity","abc_class"]]
            .style.format(fmt).bar(subset=["velocity"], color="#4ade80"),
            use_container_width=True,
        )

    # ══════════════════════════════════════════════════════════════════════
    # TAB — DEEP DIVE
    # ══════════════════════════════════════════════════════════════════════
    with t3:
        st.markdown("### 🔬 Deep Dive Actionable Intelligence")
        st.caption(
            "Use this section to make concrete decisions with your channel partner: "
            "which cities to scale, which machines to fix, and which SKUs to liquidate or push."
        )

        # ── Thresholds (sidebar-style expander) ─────────────────────────
        with st.expander("⚙️ Adjust Decision Thresholds", expanded=False):
            th1, th2, th3, th4 = st.columns(4)
            DOC_OVER   = th1.number_input("Overstock DOC (days)",   value=60,  min_value=1, key="vnd_doc_over")
            DOC_UNDER  = th2.number_input("Understock DOC (days)",  value=15,  min_value=1, key="vnd_doc_under")
            STR_SCALE  = th3.number_input("Scale STR% threshold",   value=60,  min_value=1, max_value=100, key="vnd_str_scale")
            STR_LIQ    = th4.number_input("Liquidate STR% threshold", value=25, min_value=1, max_value=100, key="vnd_str_liq")
            VEL_LOW    = th1.number_input("Low velocity (<)",        value=0.5, min_value=0.0, step=0.1, key="vnd_vel_low")
            VEL_HIGH   = th2.number_input("High velocity (>)",       value=1.5, min_value=0.0, step=0.1, key="vnd_vel_high")
            DOC_CRIT   = th3.number_input("City CRITICAL DOC (days)",value=10,  min_value=1, key="vnd_doc_crit")
            DOC_OK     = th4.number_input("City OK DOC (days)",      value=30,  min_value=1, key="vnd_doc_ok")

        st.divider()

        # ── 1. CITY HEALTH SCORECARD ────────────────────────────────────
        st.markdown("#### 🏙️ City Health Scorecard")
        st.caption("Overall vending health per city — drive your partner review agenda from this.")

        city_agg = fdf.groupby("City").agg(
            Sales_Qty    = ("Sales_Qty",    "sum"),
            Total_SOH    = ("Total_SOH",    "sum"),
            Machine_Count= ("Machine_Count","sum"),
            velocity_mean= ("velocity",     "mean"),
            str_pct_mean = ("str_pct",      "mean"),
        ).reset_index()
        city_agg["drr"]          = city_agg["Sales_Qty"] / 30
        city_agg["days_of_cover"]= np.where(
            city_agg["drr"] > 0, city_agg["Total_SOH"] / city_agg["drr"], 999
        )
        city_agg["sales_val"] = city_agg["City"].map(
            fdf.groupby("City")["sales_val"].sum()
        )

        def _city_health(row):
            doc = row["days_of_cover"]
            str_p = row["str_pct_mean"]
            if doc < DOC_CRIT or str_p < STR_LIQ:
                return "🔴 Critical"
            if doc < DOC_OK or str_p < STR_SCALE:
                return "🟡 OK"
            return "🟢 Healthy"

        city_agg["Health"] = city_agg.apply(_city_health, axis=1)
        city_agg["DOC_display"] = city_agg["days_of_cover"].apply(
            lambda x: f"{x:.0f}d" if x < 999 else "∞"
        )

        # Health summary pills
        hcols = st.columns(3)
        for i, (label, emoji) in enumerate([("🟢 Healthy","🟢"),("🟡 OK","🟡"),("🔴 Critical","🔴")]):
            cnt = (city_agg["Health"] == label).sum()
            hcols[i].metric(label, f"{cnt} cities")

        # Colour-coded table
        health_disp = city_agg[["City","Health","Sales_Qty","sales_val","Machine_Count",
                                  "velocity_mean","str_pct_mean","DOC_display"]].copy()
        health_disp.columns = ["City","Health","Sales Qty","Sales ₹","Machines",
                                "Avg Velocity","Avg STR%","Avg DOC"]

        def _color_health(val):
            if "Critical" in str(val): return "background-color:#fee2e2; color:#991b1b; font-weight:bold"
            if "OK"       in str(val): return "background-color:#fef9c3; color:#92400e; font-weight:bold"
            if "Healthy"  in str(val): return "background-color:#dcfce7; color:#166534; font-weight:bold"
            return ""

        st.dataframe(
            health_disp.style
            .format({"Sales Qty":"{:,.0f}","Sales ₹":"₹{:,.0f}",
                     "Machines":"{:,.0f}","Avg Velocity":"{:.2f}","Avg STR%":"{:.1f}"})
            .map(_color_health, subset=["Health"]),
            use_container_width=True, hide_index=True,
        )

        # City health bar chart
        ch_order = city_agg.sort_values("str_pct_mean", ascending=True)
        fig_ch = px.bar(
            ch_order, x="str_pct_mean", y="City", orientation="h",
            color="Health",
            color_discrete_map={"🟢 Healthy":"#22c55e","🟡 OK":"#eab308","🔴 Critical":"#ef4444"},
            labels={"str_pct_mean":"Avg STR %"},
            title="City STR% — Health Banding",
            height=max(300, len(ch_order)*40),
        )
        fig_ch.add_vline(x=STR_LIQ,   line_dash="dot", line_color="#ef4444",
                         annotation_text=f"Liquidate <{STR_LIQ}%", annotation_position="top right")
        fig_ch.add_vline(x=STR_SCALE, line_dash="dot", line_color="#22c55e",
                         annotation_text=f"Scale >{STR_SCALE}%", annotation_position="top right")
        st.plotly_chart(fig_ch, use_container_width=True)

        st.divider()

        # ── 2. MACHINE OVER/UNDER STOCK ─────────────────────────────────
        st.markdown("#### 🏧 Machine Stock Alerts — Which Machines to Check")
        st.caption("Pinpoint city×product combinations that are overstocked (cash tied up) or understocked (lost sales).")

        mach_df = fdf.copy()
        mach_df["stock_status"] = np.where(
            mach_df["days_of_cover"] > DOC_OVER,  "⬆️ Overstock",
            np.where(mach_df["days_of_cover"] < DOC_UNDER, "⬇️ Understock", "✅ Normal")
        )
        # Only show problem rows
        alerts = mach_df[mach_df["stock_status"] != "✅ Normal"].copy()

        mc1, mc2 = st.columns(2)
        mc1.metric("⬆️ Overstock alerts",  (alerts["stock_status"]=="⬆️ Overstock").sum(),
                   help=f"DOC > {DOC_OVER} days")
        mc2.metric("⬇️ Understock alerts", (alerts["stock_status"]=="⬇️ Understock").sum(),
                   help=f"DOC < {DOC_UNDER} days")

        if not alerts.empty:
            def _color_stock(val):
                if "Overstock"  in str(val): return "background-color:#dbeafe; color:#1e40af; font-weight:bold"
                if "Understock" in str(val): return "background-color:#fee2e2; color:#991b1b; font-weight:bold"
                return ""

            alerts_disp = alerts[["City","Product","Machine_Count","Total_SOH",
                                   "drr","days_of_cover","stock_status"]].copy()
            alerts_disp.columns = ["City","Product","Machines","SOH","DRR","DOC","Status"]
            st.dataframe(
                alerts_disp.style
                .format({"Machines":"{:,.0f}","SOH":"{:,.0f}","DRR":"{:.2f}","DOC":"{:.1f}"})
                .map(_color_stock, subset=["Status"]),
                use_container_width=True, hide_index=True,
            )

            # Bubble chart: DOC vs Velocity, sized by SOH
            bubble_df = mach_df[mach_df["days_of_cover"] < 500].copy()
            fig_bub = px.scatter(
                bubble_df, x="days_of_cover", y="velocity",
                size="Total_SOH", color="stock_status", hover_data=["City","Product"],
                color_discrete_map={
                    "⬆️ Overstock":"#3b82f6","⬇️ Understock":"#ef4444","✅ Normal":"#22c55e"
                },
                labels={"days_of_cover":"Days of Cover","velocity":"Daily Velocity / Machine"},
                title="Stock Health Map — Bubble Size = SOH Qty",
                height=450,
            )
            fig_bub.add_vline(x=DOC_UNDER, line_dash="dot", line_color="#ef4444")
            fig_bub.add_vline(x=DOC_OVER,  line_dash="dot", line_color="#3b82f6")
            fig_bub.add_hline(y=VEL_LOW,   line_dash="dot", line_color="#f59e0b")
            st.plotly_chart(fig_bub, use_container_width=True)
        else:
            st.success("✅ All city×product combinations are within normal stock range.")

        st.divider()

        # ── 3. PRODUCT × LOCATION DRILL-DOWN ────────────────────────────
        st.markdown("#### 📦 Product × Location Stock Drill-Down")
        st.caption("Select a product to see its stock health across every city.")

        prod_list = sorted(fdf["Product"].unique())
        sel_prod_dd = st.selectbox("Select Product", prod_list, key="vnd_dd_prod")
        prod_city = fdf[fdf["Product"] == sel_prod_dd].copy()
        prod_city["stock_status"] = np.where(
            prod_city["days_of_cover"] > DOC_OVER,  "⬆️ Overstock",
            np.where(prod_city["days_of_cover"] < DOC_UNDER, "⬇️ Understock", "✅ Normal")
        )

        pcc1, pcc2 = st.columns([3, 2])
        with pcc1:
            pc_disp = prod_city[["City","Machines" if "Machines" in prod_city.columns else "Machine_Count",
                                  "Total_SOH","Sales_Qty","drr","days_of_cover","str_pct","stock_status"]].copy()
            pc_disp.columns = ["City","Machines","SOH","Sales","DRR","DOC","STR%","Status"]

            def _color_pc(val):
                if "Overstock"  in str(val): return "background-color:#dbeafe;color:#1e40af;font-weight:bold"
                if "Understock" in str(val): return "background-color:#fee2e2;color:#991b1b;font-weight:bold"
                if "Normal"     in str(val): return "background-color:#dcfce7;color:#166534;font-weight:bold"
                return ""

            st.dataframe(
                pc_disp.style
                .format({"Machines":"{:,.0f}","SOH":"{:,.0f}","Sales":"{:,.0f}",
                         "DRR":"{:.2f}","DOC":"{:.1f}","STR%":"{:.1f}"})
                .map(_color_pc, subset=["Status"]),
                use_container_width=True, hide_index=True,
            )

        with pcc2:
            fig_pc = px.bar(
                prod_city.sort_values("days_of_cover"),
                x="days_of_cover", y="City", orientation="h",
                color="stock_status",
                color_discrete_map={
                    "⬆️ Overstock":"#3b82f6","⬇️ Understock":"#ef4444","✅ Normal":"#22c55e"
                },
                title=f"DOC by City — {sel_prod_dd[:30]}",
                labels={"days_of_cover":"Days of Cover"},
                height=max(300, len(prod_city)*45),
            )
            fig_pc.add_vline(x=DOC_UNDER, line_dash="dot", line_color="#ef4444")
            fig_pc.add_vline(x=DOC_OVER,  line_dash="dot", line_color="#3b82f6")
            st.plotly_chart(fig_pc, use_container_width=True)

        st.divider()

        # ── 4. SCALE / MAINTAIN / LIQUIDATE MATRIX ──────────────────────
        st.markdown("#### 🎯 SKU × City Action Matrix — Scale / Maintain / Liquidate")
        st.caption(
            f"**Scale** = STR% > {STR_SCALE}% and DOC < {DOC_OVER}d  |  "
            f"**Liquidate** = STR% < {STR_LIQ}% or DOC > {DOC_OVER}d  |  "
            f"**Maintain** = everything else"
        )

        action_df = fdf.copy()

        def _action(row):
            s, d = row["str_pct"], row["days_of_cover"]
            if s >= STR_SCALE and d < DOC_OVER:
                return "🚀 Scale"
            if s < STR_LIQ or d > DOC_OVER:
                return "🔻 Liquidate"
            return "🔄 Maintain"

        action_df["Action"] = action_df.apply(_action, axis=1)

        ac1, ac2, ac3 = st.columns(3)
        ac1.metric("🚀 Scale",     (action_df["Action"]=="🚀 Scale").sum(),     delta="opportunities")
        ac2.metric("🔄 Maintain",  (action_df["Action"]=="🔄 Maintain").sum())
        ac3.metric("🔻 Liquidate", (action_df["Action"]=="🔻 Liquidate").sum(), delta="⚠️ at risk", delta_color="inverse")

        # Pivot: City vs Product coloured by action
        pivot_data = action_df[["City","Product","Action","str_pct","days_of_cover","Sales_Qty"]].copy()

        # Action filter
        act_filter = st.multiselect(
            "Filter Actions", ["🚀 Scale","🔄 Maintain","🔻 Liquidate"],
            default=["🚀 Scale","🔻 Liquidate"], key="vnd_act_filter"
        )
        filtered_act = pivot_data[pivot_data["Action"].isin(act_filter)] if act_filter else pivot_data

        def _color_action(val):
            if "Scale"     in str(val): return "background-color:#dcfce7;color:#166534;font-weight:bold"
            if "Liquidate" in str(val): return "background-color:#fee2e2;color:#991b1b;font-weight:bold"
            if "Maintain"  in str(val): return "background-color:#fef9c3;color:#92400e"
            return ""

        act_disp = filtered_act[["City","Product","Action","str_pct","days_of_cover","Sales_Qty"]].copy()
        act_disp.columns = ["City","Product","Action","STR%","DOC","Sales Qty"]
        st.dataframe(
            act_disp.sort_values(["Action","City"])
            .style.format({"STR%":"{:.1f}","DOC":"{:.1f}","Sales Qty":"{:,.0f}"})
            .map(_color_action, subset=["Action"]),
            use_container_width=True, hide_index=True,
        )

        # Scatter: STR% vs DOC quadrant chart
        quad_df = action_df[action_df["days_of_cover"] < 500].copy()
        fig_quad = px.scatter(
            quad_df, x="str_pct", y="days_of_cover",
            color="Action", size="Sales_Qty",
            hover_data=["City","Product","str_pct","days_of_cover"],
            color_discrete_map={
                "🚀 Scale":"#22c55e","🔄 Maintain":"#eab308","🔻 Liquidate":"#ef4444"
            },
            labels={"str_pct":"STR %","days_of_cover":"Days of Cover"},
            title="Action Quadrant — STR% vs Days of Cover (size = Sales Qty)",
            height=500,
        )
        fig_quad.add_vline(x=STR_LIQ,   line_dash="dash", line_color="#ef4444",
                           annotation_text=f"STR {STR_LIQ}%")
        fig_quad.add_vline(x=STR_SCALE, line_dash="dash", line_color="#22c55e",
                           annotation_text=f"STR {STR_SCALE}%")
        fig_quad.add_hline(y=DOC_UNDER, line_dash="dash", line_color="#ef4444",
                           annotation_text=f"DOC {DOC_UNDER}d")
        fig_quad.add_hline(y=DOC_OVER,  line_dash="dash", line_color="#3b82f6",
                           annotation_text=f"DOC {DOC_OVER}d")
        st.plotly_chart(fig_quad, use_container_width=True)

        st.divider()

        # ── 5. PARTNER REVIEW SUMMARY ────────────────────────────────────
        st.markdown("#### 📋 Partner Review Summary")
        st.caption("Ready-to-use talking points for your channel partner meeting.")

        critical_cities = city_agg[city_agg["Health"]=="🔴 Critical"]["City"].tolist()
        scale_items     = action_df[action_df["Action"]=="🚀 Scale"][["City","Product","str_pct","days_of_cover"]]
        liq_items       = action_df[action_df["Action"]=="🔻 Liquidate"][["City","Product","str_pct","days_of_cover"]]
        over_items      = alerts[alerts["stock_status"]=="⬆️ Overstock"][["City","Product","days_of_cover"]] if not alerts.empty else pd.DataFrame()
        under_items     = alerts[alerts["stock_status"]=="⬇️ Understock"][["City","Product","days_of_cover"]] if not alerts.empty else pd.DataFrame()

        rv1, rv2 = st.columns(2)
        with rv1:
            st.markdown("**🔴 Immediate Attention Required**")
            if critical_cities:
                for c in critical_cities:
                    st.error(f"• **{c}** — Critical health, review stock and sales urgently")
            if not under_items.empty:
                for _, r in under_items.head(5).iterrows():
                    st.error(f"• **{r['City']}** / {r['Product'][:25]}… — only {r['days_of_cover']:.0f}d cover (risk of stockout)")
            if not critical_cities and under_items.empty:
                st.success("No critical issues found.")

        with rv2:
            st.markdown("**🟢 Growth Opportunities**")
            if not scale_items.empty:
                for _, r in scale_items.head(5).iterrows():
                    st.success(f"• **{r['City']}** / {r['Product'][:25]}… — STR {r['str_pct']:.0f}%, DOC {r['days_of_cover']:.0f}d → increase fill qty")
            else:
                st.info("No scale opportunities in current filters.")

        if not liq_items.empty:
            st.markdown("**⚠️ Liquidation Candidates**")
            liq_disp = liq_items.copy()
            liq_disp.columns = ["City","Product","STR%","DOC"]
            st.dataframe(
                liq_disp.style.format({"STR%":"{:.1f}","DOC":"{:.1f}"}),
                use_container_width=True, hide_index=True,
            )

    # ══════════════════════════════════════════════════════════════════════
    # TAB — TREND LINES (unchanged)
    # ══════════════════════════════════════════════════════════════════════
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

    # ══════════════════════════════════════════════════════════════════════
    # TAB — SKU CONFIG  (persistent prices via Supabase)
    # ══════════════════════════════════════════════════════════════════════
    with t5:
        st.markdown("### ⚙️ SKU Price Configuration")
        st.caption(
            "Set MRP for each product once — prices are saved to the database and "
            "auto-loaded next time. You can update them at any time."
        )

        # Load persisted prices and merge with current session prices
        persisted = _load_prices_from_db()
        merged_prices = {**persisted, **price_map}   # session prices take precedence

        all_prods_cfg = sorted(fdf["Product"].unique())
        cfg_init = pd.DataFrame({
            "Product":        all_prods_cfg,
            "Price_per_Unit": [merged_prices.get(p, 0.0) for p in all_prods_cfg],
        })

        cfg_edited = st.data_editor(
            cfg_init,
            use_container_width=True,
            hide_index=True,
            key="vnd_cfg_price_editor",
            column_config={
                "Product":        st.column_config.TextColumn("Product", disabled=True),
                "Price_per_Unit": st.column_config.NumberColumn("MRP (₹)", min_value=0.0, step=0.5, format="₹%.2f"),
            },
        )

        cfg1, cfg2 = st.columns(2)
        if cfg1.button("💾 Save Prices Permanently", type="primary",
                       use_container_width=True, key="vnd_cfg_save"):
            new_map = dict(zip(cfg_edited["Product"], cfg_edited["Price_per_Unit"]))
            _save_prices_to_db(new_map)
            st.session_state["vnd_price_map"] = new_map
            st.success("✅ Prices saved to database — they will auto-load on every future session.")
            st.rerun()

        if cfg2.button("🔄 Reload Saved Prices", use_container_width=True, key="vnd_cfg_reload"):
            reloaded = _load_prices_from_db()
            if reloaded:
                st.session_state["vnd_price_map"] = reloaded
                st.success(f"Loaded {len(reloaded)} saved prices.")
                st.rerun()
            else:
                st.info("No prices found in database yet.")

        if persisted:
            st.divider()
            st.caption(f"ℹ️ {len(persisted)} SKU price(s) currently saved in database.")

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
        # Also persist prices to Supabase so they survive redeployment
        _save_prices_to_db(price_map)
        st.session_state["vnd_db"] = db
        st.success(f"✅ Saved: {target_cust} — {sel_month} {sel_year}")
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def render_vending_tab(role: str, supabase_client=None):
    """
    Renders the full Vending Performance Hub inside a tab.
    Call this from the main app's tab context:
        with tabs[_TAB_VENDING]:
            render_vending_tab(role, supabase_client=supabase)

    supabase_client: the Supabase client from the main app (for price persistence).
    """
    _init_state()

    # Attach Supabase client so price helpers can use it
    if supabase_client is not None:
        _attach_supabase(supabase_client)

    # Auto-load persisted prices once per session
    if not st.session_state.get("vnd_prices_loaded", False):
        persisted = _load_prices_from_db()
        if persisted:
            # Merge: don't overwrite prices already set this session
            current = st.session_state.get("vnd_price_map", {})
            st.session_state["vnd_price_map"] = {**persisted, **current}
        st.session_state["vnd_prices_loaded"] = True

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
                    # Merge saved prices with persisted DB prices (DB wins for any not in saved)
                    persisted_now = _load_prices_from_db()
                    st.session_state["vnd_price_map"] = {**persisted_now, **saved.get("price_map", {})}
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

            # Pre-fill from persisted prices
            current_prices = st.session_state.get("vnd_price_map", {})
            persisted_now  = _load_prices_from_db()
            prefill        = {**persisted_now, **current_prices}

            unique_prods = sorted(st.session_state["vnd_processed_df"]["Product"].unique())
            prefilled_prices = [prefill.get(p, 0.0) for p in unique_prods]
            any_prefilled = any(v > 0 for v in prefilled_prices)

            if any_prefilled:
                st.success(
                    f"✅ Prices auto-loaded from database for "
                    f"{sum(1 for v in prefilled_prices if v > 0)}/{len(unique_prods)} SKUs. "
                    "Update below if needed."
                )
            else:
                st.info("Set the MRP for each product (saved permanently after first entry).")

            price_init = pd.DataFrame({
                "Product":        unique_prods,
                "Price_per_Unit": prefilled_prices,
            })
            edited = st.data_editor(
                price_init, use_container_width=True, hide_index=True,
                key="vnd_price_editor",
                column_config={
                    "Product":        st.column_config.TextColumn("Product", disabled=True),
                    "Price_per_Unit": st.column_config.NumberColumn("MRP (₹)", min_value=0.0,
                                                                     step=0.5, format="₹%.2f"),
                },
            )

            if st.button("🚀 Generate Performance Analysis", type="primary",
                          key="vnd_gen_btn"):
                new_map = dict(zip(edited["Product"], edited["Price_per_Unit"]))
                st.session_state["vnd_analysis_generated"] = True
                st.session_state["vnd_price_map"]          = new_map
                # Auto-save prices to DB whenever analysis is generated
                _save_prices_to_db(new_map)

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
