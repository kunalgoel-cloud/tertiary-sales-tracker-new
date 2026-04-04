"""
marketing_module.py
───────────────────
Self-contained performance-marketing logic for Mamanourish dashboard.
Import render_marketing_tab(role) and call it inside the
📣 Performance Marketing tab of the main app.

Uses its OWN Supabase connection via:
  MARKETING_SUPABASE_URL  — st.secrets key  (marketing DB)
  MARKETING_SUPABASE_KEY  — st.secrets key

For ACOS/TACOS cross-DB views it also reads the sales DB via:
  SUPABASE_URL / SUPABASE_KEY  (same keys as main app)

Edit this file independently; the main app never needs to know the internals.
"""

import io
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from datetime import datetime
from supabase import create_client, Client


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENTS
# ─────────────────────────────────────────────────────────────

@st.cache_resource
def _get_marketing_supabase() -> Client:
    try:
        return create_client(st.secrets["MARKETING_SUPABASE_URL"], st.secrets["MARKETING_SUPABASE_KEY"])
    except KeyError:
        st.error("Add MARKETING_SUPABASE_URL and MARKETING_SUPABASE_KEY to Streamlit Secrets.")
        st.stop()


@st.cache_resource
def _get_sales_supabase() -> Client:
    try:
        return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
    except KeyError:
        st.error("Add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
        st.stop()


# ─────────────────────────────────────────────────────────────
# DATABASE HELPERS — MARKETING DB
# ─────────────────────────────────────────────────────────────

def _get_products(sb) -> list[str]:
    try:
        r = sb.table("products").select("name").order("name").execute()
        return [x["name"] for x in r.data]
    except Exception:
        return []


def _get_channels(sb) -> list[str]:
    try:
        r = sb.table("channels").select("name").order("name").execute()
        return [x["name"] for x in r.data]
    except Exception:
        return []


def _add_product(sb, name: str) -> bool:
    try:
        sb.table("products").insert({"name": name}).execute()
        return True
    except Exception as e:
        st.error(f"Error adding product: {e}")
        return False


def _add_channel(sb, name: str) -> bool:
    try:
        sb.table("channels").insert({"name": name}).execute()
        return True
    except Exception as e:
        st.error(f"Error adding channel: {e}")
        return False


def _get_mappings(sb) -> pd.DataFrame:
    try:
        r = sb.table("mappings").select("campaign, product_name").order("campaign").execute()
        return pd.DataFrame(r.data)
    except Exception:
        return pd.DataFrame(columns=["campaign", "product_name"])


def _add_mapping(sb, campaign: str, product_name: str) -> bool:
    try:
        sb.table("mappings").insert({"campaign": campaign, "product_name": product_name}).execute()
        return True
    except Exception:
        return False


def _delete_mapping(sb, campaign: str, product_name: str) -> bool:
    try:
        sb.table("mappings").delete().eq("campaign", campaign).eq("product_name", product_name).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting mapping: {e}")
        return False


def _get_performance(sb) -> pd.DataFrame:
    try:
        all_rows = []
        page, PAGE_SIZE = 0, 1000
        while True:
            r = sb.table("performance").select("*").order("date", desc=True)\
                .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1).execute()
            if not r.data:
                break
            all_rows.extend(r.data)
            if len(r.data) < PAGE_SIZE:
                break
            page += 1
        df = pd.DataFrame(all_rows)
        if not df.empty:
            for col in ["spend", "sales"]:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df
    except Exception as e:
        st.error(f"Error fetching performance data: {e}")
        return pd.DataFrame()


def _add_performance_record(sb, date, channel, campaign, product, spend, sales) -> bool:
    record = {
        "date": date, "channel": channel, "campaign": campaign,
        "product": product, "spend": float(spend), "sales": float(sales),
        "clicks": 0, "orders": 0,
    }
    try:
        sb.table("performance").upsert(record, on_conflict="date,channel,campaign,product").execute()
        return True
    except Exception:
        try:
            ex = sb.table("performance").select("*")\
                .eq("date", date).eq("channel", channel)\
                .eq("campaign", campaign).eq("product", product).execute()
            if ex.data:
                sb.table("performance").update({"spend": float(spend), "sales": float(sales)})\
                    .eq("date", date).eq("channel", channel)\
                    .eq("campaign", campaign).eq("product", product).execute()
            else:
                sb.table("performance").insert(record).execute()
            return True
        except Exception as e2:
            st.error(f"Error saving record: {e2}")
            return False


def _delete_performance(sb, channel: str, date: str) -> int:
    try:
        r = sb.table("performance").delete().eq("channel", channel).eq("date", date).execute()
        return len(r.data) if r.data else 0
    except Exception as e:
        st.error(f"Error deleting: {e}")
        return 0


# ─────────────────────────────────────────────────────────────
# CHANNEL MAP HELPERS  (marketing channel -> sales channels)
# ─────────────────────────────────────────────────────────────

def _get_channel_map(sb) -> dict[str, list[str]]:
    """
    Returns {marketing_channel: [sales_channel, ...]} from the channel_map table.
    Example: {"Amazon": ["Amazon RKW", "Amazon Seller"], "Blinkit": ["Blinkit"]}
    Falls back to identity mapping (channel maps to itself) if table missing or empty.
    """
    try:
        r = sb.table("channel_map").select("mkt_channel,sales_channel").execute()
        result: dict[str, list[str]] = {}
        for row in r.data:
            result.setdefault(row["mkt_channel"], []).append(row["sales_channel"])
        return result
    except Exception:
        return {}


def _save_channel_map_entry(sb, mkt_channel: str, sales_channel: str) -> bool:
    try:
        sb.table("channel_map").upsert(
            {"mkt_channel": mkt_channel, "sales_channel": sales_channel},
            on_conflict="mkt_channel,sales_channel",
        ).execute()
        return True
    except Exception as e:
        st.error(f"Error saving channel map: {e}")
        return False


def _delete_channel_map_entry(sb, mkt_channel: str, sales_channel: str) -> bool:
    try:
        sb.table("channel_map").delete()\
            .eq("mkt_channel", mkt_channel)\
            .eq("sales_channel", sales_channel).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting channel map: {e}")
        return False


def _apply_channel_map(sales_df: pd.DataFrame, channel_map: dict[str, list[str]]) -> pd.DataFrame:
    """Adds mkt_channel column; unmapped channels map to themselves."""
    if sales_df.empty:
        return sales_df
    reverse = {sc: mkt for mkt, scs in channel_map.items() for sc in scs}
    df = sales_df.copy()
    df["mkt_channel"] = df["channel"].map(reverse).fillna(df["channel"])
    return df


# ─────────────────────────────────────────────────────────────
# PRODUCT MAP HELPERS  (marketing product -> sales item_names)
# ─────────────────────────────────────────────────────────────

def _get_product_map(sb) -> dict[str, list[str]]:
    """Returns {mkt_product: [sales_item, ...]} from product_map table."""
    try:
        r = sb.table("product_map").select("mkt_product,sales_item").execute()
        result: dict[str, list[str]] = {}
        for row in r.data:
            result.setdefault(row["mkt_product"], []).append(row["sales_item"])
        return result
    except Exception:
        return {}


def _save_product_map_entry(sb, mkt_product: str, sales_item: str) -> bool:
    try:
        sb.table("product_map").upsert(
            {"mkt_product": mkt_product, "sales_item": sales_item},
            on_conflict="mkt_product,sales_item",
        ).execute()
        return True
    except Exception as e:
        st.error(f"Error saving product map: {e}")
        return False


def _delete_product_map_entry(sb, mkt_product: str, sales_item: str) -> bool:
    try:
        sb.table("product_map").delete()            .eq("mkt_product", mkt_product)            .eq("sales_item", sales_item).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting product map: {e}")
        return False


def _apply_product_map(sales_df: pd.DataFrame, product_map: dict[str, list[str]]) -> pd.DataFrame:
    """
    Adds mkt_product column to sales_df by reversing product_map.
    Combo/unmapped items get NaN mkt_product — excluded from per-product TACOS.
    This prevents double-counting combos across multiple products.
    """
    if sales_df.empty:
        return sales_df
    reverse = {si: mkt for mkt, items in product_map.items() for si in items}
    df = sales_df.copy()
    df["mkt_product"] = df["item_name"].map(reverse)  # NaN for unmapped — intentional
    return df


# ─────────────────────────────────────────────────────────────
# DATABASE HELPERS — SALES DB  (for TACOS cross-join)
# ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def _get_sales_data() -> pd.DataFrame:
    try:
        sb = _get_sales_supabase()
        all_rows = []
        page, PAGE_SIZE = 0, 1000
        while True:
            r = sb.table("sales").select("date,channel,item_name,qty_sold,revenue")\
                .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1).execute()
            if not r.data:
                break
            all_rows.extend(r.data)
            if len(r.data) < PAGE_SIZE:
                break
            page += 1
        if not all_rows:
            return pd.DataFrame()
        df = pd.DataFrame(all_rows)
        df["revenue"]  = pd.to_numeric(df["revenue"],  errors="coerce").fillna(0.0)
        df["qty_sold"] = pd.to_numeric(df["qty_sold"], errors="coerce").fillna(0.0)
        df["date"]     = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        st.warning(f"Could not load sales data for TACOS: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────
# FILE PROCESSING
# ─────────────────────────────────────────────────────────────

def _read_file(file) -> pd.DataFrame | None:
    name = file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(file)
        except Exception as e:
            st.error(f"Error reading Excel: {e}")
            return None
    bytes_data = file.read()
    for enc in ["utf-8", "ISO-8859-1", "cp1252", "utf-16"]:
        try:
            check = pd.read_csv(io.BytesIO(bytes_data), encoding=enc, nrows=10)
            if "METRICS_DATE" not in check.columns and any(
                "Selected Filters" in str(c) for c in check.columns
            ):
                return pd.read_csv(io.BytesIO(bytes_data), encoding=enc, skiprows=6)
            return pd.read_csv(io.BytesIO(bytes_data), encoding=enc)
        except Exception:
            continue
    st.error("Could not read file with any supported encoding.")
    return None


def _standardize(df: pd.DataFrame, manual_date=None) -> pd.DataFrame:
    col_map = {
        "METRICS_DATE": "date", "CAMPAIGN_NAME": "campaign",
        "TOTAL_BUDGET_BURNT": "spend", "TOTAL_SPEND": "spend",
        "TOTAL_GMV": "sales", "Campaign name": "campaign",
        "Total cost": "spend", "Sales": "sales",
        "Date": "date", "Ad Spend": "spend", "Ad Revenue": "sales",
    }
    df = df.rename(columns=col_map)
    if "campaign" not in df.columns:
        df["campaign"] = "CHANNEL_TOTAL"
    for col in ["spend", "sales"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(r"[₹,]", "", regex=True), errors="coerce"
        ).fillna(0)
    df = df[df["spend"] > 0].copy()
    if manual_date:
        df["date"] = manual_date.strftime("%Y-%m-%d")
    else:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df = df.dropna(subset=["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df[["date", "campaign", "spend", "sales"]]


# ─────────────────────────────────────────────────────────────
# SHARED FILTER HELPER
# ─────────────────────────────────────────────────────────────

def _apply_filters(df: pd.DataFrame, key_prefix: str, show_product: bool = True):
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        dr = st.date_input(
            "Date Range", value=(min_date, max_date),
            min_value=min_date, max_value=max_date,
            key=f"{key_prefix}_dr",
        )
    with fc2:
        all_channels = sorted(df["channel"].unique())
        ch_f = st.multiselect("Channels", all_channels, default=all_channels, key=f"{key_prefix}_ch")
    with fc3:
        if show_product and "product" in df.columns:
            all_products = sorted(df["product"].unique())
            pr_f = st.multiselect("Products", all_products, default=all_products, key=f"{key_prefix}_pr")
        else:
            pr_f = []

    f = df.copy()
    if len(dr) == 2:
        f = f[(f["date"] >= pd.to_datetime(dr[0])) & (f["date"] <= pd.to_datetime(dr[1]))]
    if ch_f:
        f = f[f["channel"].isin(ch_f)]
    if pr_f and "product" in df.columns:
        f = f[f["product"].isin(pr_f)]

    start = dr[0] if len(dr) == 2 else min_date
    end   = dr[1] if len(dr) == 2 else max_date
    return f, start, end


# ─────────────────────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────────────────────

def _render_dashboard(sb):
    st.subheader("📊 Performance Dashboard")

    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No marketing data yet. Upload data in the Upload tab.")
        return

    df_p = df_p.drop(columns=["id", "created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])

    f_df, _, _ = _apply_filters(df_p, "dash")
    if f_df.empty:
        st.warning("No data matches the selected filters.")
        return

    t_spend = f_df["spend"].sum()
    t_sales = f_df["sales"].sum()
    roas    = t_sales / t_spend if t_spend > 0 else 0
    acos    = (t_spend / t_sales * 100) if t_sales > 0 else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Ad Spend",   f"₹{t_spend:,.0f}")
    k2.metric("Total Ad Revenue", f"₹{t_sales:,.0f}")
    k3.metric("Overall ROAS",     f"{roas:.2f}x")
    k4.metric("ACOS",             f"{acos:.1f}%", help="Ad Spend ÷ Ad-Attributed Revenue × 100")

    st.divider()
    st.markdown("#### 📈 Spend vs ROAS by Channel")

    ch_trend    = f_df.groupby(["date","channel"]).agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
    ch_trend["ROAS"] = ch_trend["sales"] / ch_trend["spend"]
    total_trend = f_df.groupby("date").agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
    total_trend["ROAS"] = total_trend["sales"] / total_trend["spend"]

    fig = go.Figure()
    for ch in sorted(ch_trend["channel"].unique()):
        d = ch_trend[ch_trend["channel"] == ch]
        fig.add_trace(go.Bar(x=d["date"], y=d["spend"], name=f"{ch} Spend"))
    for ch in sorted(ch_trend["channel"].unique()):
        d = ch_trend[ch_trend["channel"] == ch]
        fig.add_trace(go.Scatter(x=d["date"], y=d["ROAS"], name=f"{ch} ROAS",
                                 yaxis="y2", mode="lines+markers"))
    fig.add_trace(go.Scatter(
        x=total_trend["date"], y=total_trend["ROAS"], name="Total ROAS",
        yaxis="y2", line=dict(color="black", width=4, dash="dot"),
    ))
    fig.update_layout(
        barmode="stack",
        yaxis=dict(title="Spend (₹)"),
        yaxis2=dict(title="ROAS", overlaying="y", side="right",
                    range=[0, ch_trend["ROAS"].max() * 1.2 if not ch_trend.empty else 10]),
        legend=dict(orientation="h", y=1.2), hovermode="x unified", height=480,
    )
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown("**By Channel**")
        ch_sum = (
            f_df.groupby("channel").agg(spend=("spend","sum"), sales=("sales","sum"))
            .assign(ROAS=lambda x: x.sales / x.spend,
                    ACOS=lambda x: (x.spend / x.sales * 100).round(1))
            .sort_values("spend", ascending=False)
        )
        st.dataframe(ch_sum.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}",
                                          "ROAS":"{:.2f}x", "ACOS":"{:.1f}%"}),
                     use_container_width=True)
    with tc2:
        st.markdown("**By Product**")
        pr_sum = (
            f_df.groupby("product").agg(spend=("spend","sum"), sales=("sales","sum"))
            .assign(ROAS=lambda x: x.sales / x.spend,
                    ACOS=lambda x: (x.spend / x.sales * 100).round(1))
            .sort_values("spend", ascending=False)
        )
        st.dataframe(pr_sum.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}",
                                          "ROAS":"{:.2f}x", "ACOS":"{:.1f}%"}),
                     use_container_width=True)

    st.divider()
    st.markdown("**Campaign Performance**")
    cp_tab = (
        f_df.groupby(["channel","campaign"]).agg(spend=("spend","sum"), sales=("sales","sum"))
        .assign(ROAS=lambda x: x.sales / x.spend,
                ACOS=lambda x: (x.spend / x.sales * 100).round(1))
        .reset_index().sort_values("spend", ascending=False)
    )
    st.dataframe(
        cp_tab.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}",
                             "ROAS":"{:.2f}x", "ACOS":"{:.1f}%"}),
        hide_index=True, use_container_width=True, height=300,
    )

    st.divider()
    st.markdown("#### 📅 Date-wise Detail")
    detail = f_df[["date","channel","product","campaign","spend","sales"]].copy()
    detail["ROAS"] = (detail["sales"] / detail["spend"]).round(2)
    detail["ACOS"] = (detail["spend"] / detail["sales"] * 100).round(1)
    detail["date"] = detail["date"].dt.strftime("%Y-%m-%d")
    detail = detail.sort_values("date", ascending=False)
    detail.columns = ["Date","Channel","Product","Campaign","Spend (₹)","Ad Revenue (₹)","ROAS","ACOS %"]
    st.dataframe(
        detail.style.format({"Spend (₹)":"₹{:,.2f}", "Ad Revenue (₹)":"₹{:,.2f}",
                             "ROAS":"{:.2f}x", "ACOS %":"{:.1f}%"}),
        hide_index=True, use_container_width=True, height=380,
    )
    st.caption(f"Total Records: {len(detail):,}")

    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button("📥 CSV", detail.to_csv(index=False),
                           file_name=f"mkt_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)
    with dl2:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            detail.to_excel(w, index=False, sheet_name="Performance")
            pd.DataFrame({
                "Metric": ["Spend","Ad Revenue","ROAS","ACOS","Records"],
                "Value":  [f"₹{t_spend:,.2f}", f"₹{t_sales:,.2f}",
                           f"{roas:.2f}x", f"{acos:.1f}%", len(detail)],
            }).to_excel(w, index=False, sheet_name="Summary")
        st.download_button("📥 Excel", buf.getvalue(),
                           file_name=f"mkt_{datetime.now().strftime('%Y%m%d')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    with dl3:
        summary_csv = pd.concat([
            ch_sum.reset_index().assign(Group="Channel"),
            pr_sum.reset_index().assign(Group="Product"),
        ])
        st.download_button("📥 Summary", summary_csv.to_csv(index=False),
                           file_name=f"mkt_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)


# ─────────────────────────────────────────────────────────────
# DEEP DIVE
# ─────────────────────────────────────────────────────────────

def _render_deep_dive(sb):
    st.subheader("🔬 Marketing Deep Dive")

    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No marketing data yet.")
        return

    df_p = df_p.drop(columns=["id","created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])

    f_df, dd_start, dd_end = _apply_filters(df_p, "dd")
    if f_df.empty:
        st.warning("No data matches filters.")
        return

    dd_days = max((dd_end - dd_start).days + 1, 1)
    f_df["week"] = f_df["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))

    views = st.tabs([
        "📦 ROAS by Product",
        "🎯 Spend Efficiency Quadrant",
        "📊 Budget Concentration",
        "📈 WoW ROAS",
        "⏳ Campaign Lifecycle",
    ])

    # ── ROAS by Product ──────────────────────────────────────
    with views[0]:
        st.markdown("#### 📦 ROAS Trend by Product")
        st.caption("Which SKUs are efficient to advertise? Declining ROAS over time = potential ad fatigue.")

        prod_daily = (
            f_df.groupby(["date","product"])
            .agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
        )
        prod_daily["ROAS"] = prod_daily["sales"] / prod_daily["spend"]

        fig1 = px.line(prod_daily, x="date", y="ROAS", color="product",
                       markers=True, height=420,
                       color_discrete_sequence=px.colors.qualitative.Bold)
        fig1.add_hline(y=1, line_dash="dash", line_color="red", annotation_text="Break-even (1x)")
        fig1.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig1, use_container_width=True)

        prod_sum = (
            f_df.groupby("product").agg(spend=("spend","sum"), ad_revenue=("sales","sum")).reset_index()
        )
        prod_sum["ROAS"]      = (prod_sum["ad_revenue"] / prod_sum["spend"]).round(2)
        prod_sum["ACOS%"]     = (prod_sum["spend"] / prod_sum["ad_revenue"] * 100).round(1)
        prod_sum["daily_spend"] = (prod_sum["spend"] / dd_days).round(0)
        prod_sum["status"]    = prod_sum["ROAS"].apply(
            lambda r: "🟢 Efficient" if r >= 3 else ("🟡 Marginal" if r >= 1 else "🔴 Loss-making")
        )
        prod_sum = prod_sum.sort_values("ROAS", ascending=False)
        st.dataframe(
            prod_sum.rename(columns={
                "product":"Product","spend":"Spend (₹)","ad_revenue":"Ad Revenue (₹)",
                "ROAS":"ROAS","ACOS%":"ACOS %","daily_spend":"Daily Spend (₹)","status":"Status"
            }).style.format({
                "Spend (₹)":"₹{:,.0f}","Ad Revenue (₹)":"₹{:,.0f}",
                "ROAS":"{:.2f}x","ACOS %":"{:.1f}%","Daily Spend (₹)":"₹{:,.0f}",
            }),
            hide_index=True, use_container_width=True,
        )

    # ── Spend Efficiency Quadrant ─────────────────────────────
    with views[1]:
        st.markdown("#### 🎯 Spend Efficiency Quadrant")
        st.caption(
            "Each bubble = one campaign. Bubble size = spend amount. "
            "Vertical axis = ROAS. Horizontal axis = total spend."
        )

        camp_sum = (
            f_df.groupby(["campaign","channel","product"])
            .agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
        )
        camp_sum["ROAS"] = camp_sum["sales"] / camp_sum["spend"]
        camp_sum = camp_sum[camp_sum["spend"] > 0]
        med_spend = camp_sum["spend"].median()
        med_roas  = camp_sum["ROAS"].median()

        fig2 = px.scatter(
            camp_sum, x="spend", y="ROAS",
            size="spend", color="channel",
            hover_name="campaign",
            hover_data={"product":True,"spend":":.0f","ROAS":":.2f","sales":":.0f"},
            size_max=60, height=500,
            labels={"spend":"Total Spend (₹)","ROAS":"ROAS"},
            color_discrete_sequence=px.colors.qualitative.Bold,
        )
        fig2.add_vline(x=med_spend, line_dash="dot", line_color="grey",
                       annotation_text="Median Spend")
        fig2.add_hline(y=med_roas, line_dash="dot", line_color="grey",
                       annotation_text="Median ROAS")
        fig2.add_hline(y=1, line_dash="dash", line_color="red",
                       annotation_text="Break-even")
        st.plotly_chart(fig2, use_container_width=True)

        q1, q2, q3, q4 = st.columns(4)
        q1.success("⭐ **Stars**\nHigh spend + High ROAS → Scale up")
        q2.info("💎 **Gems**\nLow spend + High ROAS → Invest more")
        q3.error("🚨 **Money Pits**\nHigh spend + Low ROAS → Pause/optimise")
        q4.warning("🗑️ **Cut**\nLow spend + Low ROAS → Switch off")

    # ── Budget Concentration ──────────────────────────────────
    with views[2]:
        st.markdown("#### 📊 Budget Concentration Risk")
        st.caption("High concentration = fragile. If your top campaign pauses, the whole funnel stalls.")

        camp_spend = (
            f_df.groupby("campaign")["spend"].sum()
            .sort_values(ascending=False).reset_index()
        )
        camp_spend["cumulative_%"] = (camp_spend["spend"].cumsum() / camp_spend["spend"].sum() * 100).round(1)
        camp_spend["spend_%"]      = (camp_spend["spend"] / camp_spend["spend"].sum() * 100).round(1)

        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=camp_spend["campaign"], y=camp_spend["spend_%"],
            name="Spend Share %",
            text=camp_spend["spend_%"].apply(lambda x: f"{x:.1f}%"),
            textposition="outside",
        ))
        fig3.add_trace(go.Scatter(
            x=camp_spend["campaign"], y=camp_spend["cumulative_%"],
            name="Cumulative %", yaxis="y2",
            mode="lines+markers", line=dict(color="black", width=2),
        ))
        fig3.update_layout(
            height=420,
            yaxis=dict(title="Spend Share %"),
            yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 110]),
            xaxis_tickangle=-30,
            legend=dict(orientation="h", y=1.15),
        )
        st.plotly_chart(fig3, use_container_width=True)

        n = len(camp_spend)
        top1 = camp_spend.iloc[0]["cumulative_%"]          if n >= 1 else 0
        top3 = camp_spend.iloc[min(2, n-1)]["cumulative_%"] if n >= 1 else 0
        top5 = camp_spend.iloc[min(4, n-1)]["cumulative_%"] if n >= 1 else 0
        cc1, cc2, cc3 = st.columns(3)
        cc1.metric("Top 1 campaign", f"{top1:.1f}% of spend",
                   delta="⚠️ High risk" if top1 > 60 else "✅ Healthy",
                   delta_color="inverse" if top1 > 60 else "normal")
        cc2.metric("Top 3 campaigns", f"{top3:.1f}% of spend",
                   delta="⚠️ Concentrated" if top3 > 80 else "✅ Diversified",
                   delta_color="inverse" if top3 > 80 else "normal")
        cc3.metric("Top 5 campaigns", f"{top5:.1f}% of spend")

    # ── WoW ROAS ─────────────────────────────────────────────
    with views[3]:
        st.markdown("#### 📈 Week-over-Week ROAS & ACOS by Channel")
        st.caption("Downward ROAS trend = declining efficiency. Review bidding strategy or creative.")

        weekly = (
            f_df.groupby(["week","channel"])
            .agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
        )
        weekly["ROAS"] = weekly["sales"] / weekly["spend"]
        weekly["ACOS"] = (weekly["spend"] / weekly["sales"] * 100).round(1)

        wow_metric = st.radio("Metric:", ["ROAS", "ACOS %"], horizontal=True, key="dd_wow_metric")
        y_col = "ROAS" if wow_metric == "ROAS" else "ACOS"

        fig4 = px.line(weekly, x="week", y=y_col, color="channel",
                       markers=True, height=400,
                       color_discrete_sequence=px.colors.qualitative.Bold)
        if y_col == "ROAS":
            fig4.add_hline(y=1, line_dash="dash", line_color="red", annotation_text="Break-even")
        fig4.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig4, use_container_width=True)

        wow_pivot = weekly.pivot_table(index="channel", columns="week", values=y_col).fillna(0)
        weeks = sorted(wow_pivot.columns)
        wow_pivot = wow_pivot[weeks]
        if len(weeks) >= 2:
            wow_pivot["WoW Change"] = wow_pivot[weeks[-1]] - wow_pivot[weeks[-2]]
            wow_pivot["WoW %"] = (
                wow_pivot["WoW Change"] / wow_pivot[weeks[-2]].where(wow_pivot[weeks[-2]] != 0) * 100
            ).round(1)
        fmt = {w: "{:.2f}x" if y_col == "ROAS" else "{:.1f}%" for w in weeks}
        if len(weeks) >= 2:
            fmt["WoW Change"] = "{:+.2f}" if y_col == "ROAS" else "{:+.1f}%"
            fmt["WoW %"] = "{:+.1f}%"
        st.dataframe(
            wow_pivot.style.format(fmt).map(
                lambda v: "color: green" if isinstance(v, float) and v > 0
                else ("color: red" if isinstance(v, float) and v < 0 else ""),
                subset=["WoW %","WoW Change"] if len(weeks) >= 2 else [],
            ),
            use_container_width=True,
        )

    # ── Campaign Lifecycle ────────────────────────────────────
    with views[4]:
        st.markdown("#### ⏳ Campaign Lifecycle & Fatigue")
        st.caption("ROAS declining despite consistent spend = ad fatigue. Refresh creative or reset audience.")

        camp_weekly = (
            f_df.groupby(["week","campaign","channel"])
            .agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
        )
        camp_weekly["ROAS"] = camp_weekly["sales"] / camp_weekly["spend"]
        active_camps = camp_weekly.groupby("campaign")["week"].nunique()
        multi_week   = active_camps[active_camps >= 2].index.tolist()

        if not multi_week:
            st.info("Need at least 2 weeks of data per campaign to show lifecycle trends.")
        else:
            sel_camps = st.multiselect(
                "Select campaigns:",
                sorted(multi_week),
                default=sorted(multi_week)[:min(5, len(multi_week))],
                key="dd_lifecycle_camps",
            )
            if sel_camps:
                lc_df = camp_weekly[camp_weekly["campaign"].isin(sel_camps)]
                fig5 = px.line(lc_df, x="week", y="ROAS", color="campaign",
                               markers=True, height=420)
                fig5.add_hline(y=1, line_dash="dash", line_color="red", annotation_text="Break-even")
                fig5.update_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.25))
                st.plotly_chart(fig5, use_container_width=True)

                st.markdown("**Fatigue Assessment**")
                for camp in sel_camps:
                    cd = lc_df[lc_df["campaign"] == camp].sort_values("week")
                    if len(cd) >= 2:
                        first_r = cd.iloc[0]["ROAS"]
                        last_r  = cd.iloc[-1]["ROAS"]
                        chg     = (last_r - first_r) / first_r * 100 if first_r > 0 else 0
                        if chg < -20:
                            st.error(f"🔴 **{camp}** — ROAS dropped {chg:.1f}% since launch → Refresh creative")
                        elif chg > 10:
                            st.success(f"🟢 **{camp}** — ROAS improved {chg:+.1f}% → Scaling well")
                        else:
                            st.info(f"🟡 **{camp}** — ROAS stable ({chg:+.1f}%)")


# ─────────────────────────────────────────────────────────────
# ACOS / TACOS
# ─────────────────────────────────────────────────────────────

def _render_acos_tacos(sb):
    st.subheader("📐 ACOS & TACOS Analysis")
    st.caption(
        "**ACOS** = Ad Spend ÷ Ad-Attributed Revenue × 100 &nbsp;*(pure ad efficiency)*  \n"
        "**TACOS** = Ad Spend ÷ Total GMV × 100 &nbsp;*(true business cost of advertising)*  \n"
        "**Gap (ACOS − TACOS)** = organic revenue doing work. A widening gap means healthy organic growth."
    )

    df_mkt = _get_performance(sb)
    df_mkt = df_mkt.drop(columns=["id","created_at"], errors="ignore")

    if df_mkt.empty:
        st.info("No marketing data yet.")
        return

    df_mkt["date"] = pd.to_datetime(df_mkt["date"])

    # Load channel map, product map, and sales data
    channel_map = _get_channel_map(sb)
    product_map = _get_product_map(sb)

    with st.spinner("Loading total sales data…"):
        df_sales = _get_sales_data()

    has_sales = not df_sales.empty
    if not has_sales:
        st.warning(
            "⚠️ Could not load sales data from the sales DB. "
            "TACOS and Organic Revenue columns will not be shown — displaying ACOS only."
        )

    # Show channel mapping status banner
    if has_sales and channel_map:
        mapped_lines = [f"**{mk}** → {', '.join(sv)}" for mk, sv in channel_map.items()]
        with st.expander("🔗 Active channel mapping (edit in ⚙️ Settings → Channel Map)", expanded=False):
            for line in mapped_lines:
                st.markdown(f"• {line}")
    elif has_sales and not channel_map:
        st.info(
            "ℹ️ No channel mapping configured. Each marketing channel is matched directly "
            "to the same-named sales channel. Go to **⚙️ Settings → Channel Map** to set up "
            "multi-channel mappings (e.g. Amazon → Amazon RKW + Amazon Seller)."
        )

    # Filters
    min_date = df_mkt["date"].min().date()
    max_date = df_mkt["date"].max().date()
    tc1, tc2 = st.columns([3, 2])
    with tc1:
        dr = st.date_input("Date Range", value=(min_date, max_date),
                           min_value=min_date, max_value=max_date, key="at_dr")
    with tc2:
        all_ch = sorted(df_mkt["channel"].unique())
        ch_sel = st.multiselect("Channels", all_ch, default=all_ch, key="at_ch")

    mkt_f = df_mkt.copy()
    if len(dr) == 2:
        mkt_f = mkt_f[(mkt_f["date"] >= pd.to_datetime(dr[0])) & (mkt_f["date"] <= pd.to_datetime(dr[1]))]
    if ch_sel:
        mkt_f = mkt_f[mkt_f["channel"].isin(ch_sel)]

    if mkt_f.empty:
        st.warning("No marketing data for selected filters.")
        return

    # Apply channel map to sales data: adds mkt_channel column, filters to selected mkt channels
    if has_sales:
        sales_mapped = _apply_channel_map(df_sales, channel_map)
        # Date-filtered ALL sales (for company-level TACOS — includes channels with no ad spend)
        sales_all = sales_mapped.copy()
        if len(dr) == 2:
            sales_all = sales_all[
                (sales_all["date"] >= pd.to_datetime(dr[0])) &
                (sales_all["date"] <= pd.to_datetime(dr[1]))
            ]
        # Channel-filtered sales (for per-channel TACOS — only channels where we advertise)
        sales_f = sales_all.copy()
        if ch_sel:
            sales_f = sales_f[sales_f["mkt_channel"].isin(ch_sel)]

    # Grand KPIs
    total_spend  = mkt_f["spend"].sum()
    total_ad_rev = mkt_f["sales"].sum()
    acos_overall = (total_spend / total_ad_rev * 100) if total_ad_rev > 0 else 0
    roas_overall = (total_ad_rev / total_spend) if total_spend > 0 else 0

    if has_sales:
        # Channel TACOS: spend vs GMV only on channels where we advertise
        ch_gmv        = sales_f["revenue"].sum()
        ch_tacos      = (total_spend / ch_gmv * 100) if ch_gmv > 0 else 0
        # Company TACOS: spend vs ALL company GMV (every channel, organic included)
        co_gmv        = sales_all["revenue"].sum()
        co_tacos      = (total_spend / co_gmv * 100) if co_gmv > 0 else 0
        organic_rev   = max(co_gmv - total_ad_rev, 0)
        organic_pct   = (organic_rev / co_gmv * 100) if co_gmv > 0 else 0

        st.markdown("##### 📊 Key Metrics")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Ad Spend",        f"₹{total_spend:,.0f}")
        k2.metric("Ad Revenue",      f"₹{total_ad_rev:,.0f}")
        k3.metric("ACOS",            f"{acos_overall:.1f}%",
                  help="Ad Spend ÷ Ad-Attributed Revenue × 100")
        k4.metric("ROAS",            f"{roas_overall:.2f}x")

        st.markdown("##### 🏢 TACOS View")
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Channel GMV",     f"₹{ch_gmv:,.0f}",
                  help="Total GMV on channels where marketing spend exists")
        t2.metric("Channel TACOS",   f"{ch_tacos:.1f}%",
                  help="Ad Spend ÷ GMV on advertised channels only")
        t3.metric("Company GMV",     f"₹{co_gmv:,.0f}",
                  help="Total GMV across ALL channels (full business view)")
        t4.metric("Company TACOS",   f"{co_tacos:.1f}%",
                  help="Ad Spend ÷ Total Company GMV — true cost of advertising to the business")

        st.caption(
            f"🌱 Organic Revenue: ₹{organic_rev:,.0f} ({organic_pct:.1f}% of company GMV) "
            f"— revenue not attributed to ad spend"
        )
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Ad Spend",   f"₹{total_spend:,.0f}")
        k2.metric("Ad Revenue", f"₹{total_ad_rev:,.0f}")
        k3.metric("ACOS",       f"{acos_overall:.1f}%")
        k4.metric("ROAS",       f"{roas_overall:.2f}x")

    st.divider()

    # Weekly trend
    st.markdown("#### 📈 Weekly ACOS vs TACOS Trend")
    mkt_f["week"] = mkt_f["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
    mkt_weekly = mkt_f.groupby("week").agg(spend=("spend","sum"), ad_rev=("sales","sum")).reset_index()
    mkt_weekly["ACOS"] = (mkt_weekly["spend"] / mkt_weekly["ad_rev"] * 100).round(1)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Bar(
        x=mkt_weekly["week"], y=mkt_weekly["spend"],
        name="Ad Spend (₹)", marker_color="#636EFA", opacity=0.7,
    ))

    if has_sales:
        # Weekly trend uses company-wide GMV (sales_all) for true TACOS picture
        sales_all["week"]  = sales_all["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
        sales_weekly       = sales_all.groupby("week")["revenue"].sum().reset_index()
        combined           = mkt_weekly.merge(sales_weekly, on="week", how="left").fillna(0)
        combined["TACOS"]    = (combined["spend"] / combined["revenue"].where(combined["revenue"] > 0) * 100).round(1)
        combined["organic"]  = (combined["revenue"] - combined["ad_rev"]).clip(lower=0)
        combined["organic%"] = (combined["organic"] / combined["revenue"].where(combined["revenue"] > 0) * 100).round(1)

        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["ACOS"],
            name="ACOS %", yaxis="y2", mode="lines+markers",
            line=dict(color="orange", width=2.5),
        ))
        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["TACOS"],
            name="TACOS %", yaxis="y2", mode="lines+markers",
            line=dict(color="red", width=2.5, dash="dot"),
        ))
        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["organic%"],
            name="Organic Rev %", yaxis="y2", mode="lines+markers",
            line=dict(color="green", width=2, dash="dash"),
        ))
    else:
        fig_trend.add_trace(go.Scatter(
            x=mkt_weekly["week"], y=mkt_weekly["ACOS"],
            name="ACOS %", yaxis="y2", mode="lines+markers",
            line=dict(color="orange", width=2.5),
        ))

    fig_trend.update_layout(
        height=460,
        yaxis=dict(title="Ad Spend (₹)"),
        yaxis2=dict(title="% Metric", overlaying="y", side="right", range=[0, 100]),
        legend=dict(orientation="h", y=1.15),
        hovermode="x unified",
    )
    st.plotly_chart(fig_trend, use_container_width=True)
    st.divider()

    # By Channel — join on mkt_channel so "Amazon" picks up both RKW + Seller
    st.markdown("#### 🏢 By Channel")
    ch_mkt = mkt_f.groupby("channel").agg(spend=("spend","sum"), ad_rev=("sales","sum")).reset_index()
    ch_mkt["ACOS"] = (ch_mkt["spend"] / ch_mkt["ad_rev"] * 100).round(1)
    ch_mkt["ROAS"] = (ch_mkt["ad_rev"] / ch_mkt["spend"]).round(2)

    if has_sales:
        # Group sales by mkt_channel — this correctly sums Amazon RKW + Amazon Seller → Amazon
        ch_sales = sales_f.groupby("mkt_channel")["revenue"].sum().reset_index()\
                          .rename(columns={"mkt_channel":"channel","revenue":"total_gmv"})
        ch_comb = ch_mkt.merge(ch_sales, on="channel", how="left").fillna(0)
        ch_comb["TACOS"]    = (ch_comb["spend"] / ch_comb["total_gmv"] * 100).round(1)
        ch_comb["Organic%"] = ((ch_comb["total_gmv"] - ch_comb["ad_rev"]).clip(lower=0)
                               / ch_comb["total_gmv"] * 100).round(1)
        disp = ch_comb.rename(columns={
            "channel":"Channel","spend":"Ad Spend (₹)","ad_rev":"Ad Revenue (₹)",
            "total_gmv":"Total GMV (₹)","ACOS":"ACOS %","TACOS":"TACOS %",
            "ROAS":"ROAS","Organic%":"Organic %",
        })
        fmt = {"Ad Spend (₹)":"₹{:,.0f}","Ad Revenue (₹)":"₹{:,.0f}","Total GMV (₹)":"₹{:,.0f}",
               "ACOS %":"{:.1f}%","TACOS %":"{:.1f}%","ROAS":"{:.2f}x","Organic %":"{:.1f}%"}
    else:
        disp = ch_mkt.rename(columns={
            "channel":"Channel","spend":"Ad Spend (₹)","ad_rev":"Ad Revenue (₹)",
            "ACOS":"ACOS %","ROAS":"ROAS",
        })
        fmt = {"Ad Spend (₹)":"₹{:,.0f}","Ad Revenue (₹)":"₹{:,.0f}",
               "ACOS %":"{:.1f}%","ROAS":"{:.2f}x"}

    st.dataframe(disp.style.format(fmt), hide_index=True, use_container_width=True)
    st.divider()

    # By Product — join via product_map (mkt short name -> sales item_names)
    st.markdown("#### 📦 By Product")
    pr_mkt = mkt_f.groupby("product").agg(spend=("spend","sum"), ad_rev=("sales","sum")).reset_index()
    pr_mkt["ACOS"] = (pr_mkt["spend"] / pr_mkt["ad_rev"] * 100).round(1)
    pr_mkt["ROAS"] = (pr_mkt["ad_rev"] / pr_mkt["spend"]).round(2)

    if has_sales:
        if product_map:
            sales_prod = _apply_product_map(sales_f, product_map)
            sales_prod = sales_prod.dropna(subset=["mkt_product"])
            pr_sales = sales_prod.groupby("mkt_product")["revenue"].sum().reset_index()                                 .rename(columns={"mkt_product":"product","revenue":"total_gmv"})
        else:
            st.warning(
                "Product map not configured. Go to **Settings → Product Map** to map "
                "marketing product names to sales SKUs for per-product TACOS."
            )
            pr_sales = pd.DataFrame(columns=["product","total_gmv"])

        pr_comb = pr_mkt.merge(pr_sales, on="product", how="left").fillna(0)
        pr_comb["TACOS"]    = (pr_comb["spend"] / pr_comb["total_gmv"].where(pr_comb["total_gmv"] > 0) * 100).round(1)
        pr_comb["Organic%"] = ((pr_comb["total_gmv"] - pr_comb["ad_rev"]).clip(lower=0)
                               / pr_comb["total_gmv"].where(pr_comb["total_gmv"] > 0) * 100).round(1)
        pr_disp = pr_comb.rename(columns={
            "product":"Product","spend":"Ad Spend (₹)","ad_rev":"Ad Revenue (₹)",
            "total_gmv":"Total GMV (₹)","ACOS":"ACOS %","TACOS":"TACOS %",
            "ROAS":"ROAS","Organic%":"Organic %",
        }).sort_values("Ad Spend (₹)", ascending=False)
        pr_fmt = {"Ad Spend (₹)":"₹{:,.0f}","Ad Revenue (₹)":"₹{:,.0f}","Total GMV (₹)":"₹{:,.0f}",
                  "ACOS %":"{:.1f}%","TACOS %":"{:.1f}%","ROAS":"{:.2f}x","Organic %":"{:.1f}%"}
    else:
        pr_disp = pr_mkt.rename(columns={
            "product":"Product","spend":"Ad Spend (₹)","ad_rev":"Ad Revenue (₹)",
            "ACOS":"ACOS %","ROAS":"ROAS",
        }).sort_values("Ad Spend (₹)", ascending=False)
        pr_fmt = {"Ad Spend (₹)":"₹{:,.0f}","Ad Revenue (₹)":"₹{:,.0f}",
                  "ACOS %":"{:.1f}%","ROAS":"{:.2f}x"}

    st.dataframe(pr_disp.style.format(pr_fmt), hide_index=True, use_container_width=True)

    # Organic vs Paid stacked chart
    if has_sales:
        st.divider()
        st.markdown("#### 🌱 Organic vs Ad-Attributed Revenue Split (Weekly)")
        st.caption("A growing green band = organic is strengthening. Heavy blue = ad-dependent.")

        org_chart = combined[["week","ad_rev","organic"]].copy()
        org_chart.columns = ["Week","Ad-Attributed Revenue","Organic Revenue"]
        org_long = org_chart.melt(id_vars="Week", var_name="Type", value_name="Revenue")

        fig_org = px.bar(
            org_long, x="Week", y="Revenue", color="Type",
            barmode="stack", height=400,
            color_discrete_map={
                "Ad-Attributed Revenue": "#636EFA",
                "Organic Revenue":       "#00CC96",
            },
            text_auto=".2s",
        )
        fig_org.update_traces(textposition="inside")
        fig_org.update_layout(hovermode="x unified", legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig_org, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# UPLOAD / HISTORY / SETTINGS
# ─────────────────────────────────────────────────────────────

def _render_upload(sb):
    st.subheader("📥 Upload Marketing Reports")

    channels = _get_channels(sb)
    products = _get_products(sb)

    if not channels or not products:
        st.warning("Configure Channels and Products in the Settings tab first.")
        return

    uc1, uc2 = st.columns(2)
    with uc1:
        manual_date = st.date_input("Date Override (leave blank to use file date)",
                                    value=None, key="mkt_manual_date")
    with uc2:
        sel_ch = st.selectbox("Channel", channels, key="mkt_upload_chan")

    file = st.file_uploader("Upload CSV or Excel", type=["csv","xlsx"], key="mkt_file")

    if file:
        with st.spinner("Processing…"):
            raw_df = _read_file(file)
        if raw_df is None:
            return

        df = _standardize(raw_df, manual_date=manual_date)
        st.success(f"Processed {len(df)} rows")
        st.dataframe(df.head(10), use_container_width=True)

        df_map   = _get_mappings(sb)
        mappings: dict[str, list[str]] = {}
        if not df_map.empty:
            for _, row in df_map.iterrows():
                mappings.setdefault(row["campaign"], []).append(row["product_name"])

        unmapped = [c for c in df["campaign"].unique() if c not in mappings]

        if unmapped:
            st.warning(f"{len(unmapped)} campaigns need product mapping")
            prods = products + ["Brand/Global"]
            with st.form("mkt_map_form"):
                st.markdown("#### Map campaigns → products")
                new_maps: dict[str, list] = {}
                for cp in unmapped:
                    new_maps[cp] = st.multiselect(f"**{cp}**", prods, key=f"mkt_map_{cp}")
                if st.form_submit_button("💾 Save Mappings", type="primary"):
                    saved = sum(
                        _add_mapping(sb, cp, pn)
                        for cp, pl in new_maps.items()
                        for pn in pl
                    )
                    if saved:
                        st.success(f"{saved} mappings saved!")
                        st.rerun()
        else:
            st.success("All campaigns mapped ✅")
            if st.button("🚀 Push to Dashboard", type="primary", key="mkt_push"):
                inserted = errors = 0
                with st.spinner("Uploading…"):
                    for _, row in df.iterrows():
                        targets = mappings.get(row["campaign"], ["Unmapped"])
                        n = len(targets)
                        for p_name in targets:
                            if _add_performance_record(
                                sb, row["date"], sel_ch, row["campaign"], p_name,
                                float(row["spend"] / n), float(row["sales"] / n),
                            ):
                                inserted += 1
                            else:
                                errors += 1
                if inserted:
                    st.success(f"✅ {inserted} records uploaded!")
                    if errors:
                        st.warning(f"{errors} errors during upload.")
                    st.balloons()
                else:
                    st.error("No records uploaded.")


def _render_history(sb):
    st.subheader("📚 Data History")
    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No data yet.")
        return

    df_p = df_p.drop(columns=["id","created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])

    hc1, hc2, hc3, hc4 = st.columns(4)
    hc1.metric("Total Records", f"{len(df_p):,}")
    hc2.metric("Date Range", f"{df_p['date'].min().date()} → {df_p['date'].max().date()}")
    hc3.metric("Channels",  df_p["channel"].nunique())
    hc4.metric("Products",  df_p["product"].nunique())

    st.divider()
    history = (
        df_p.groupby(["date","channel"])
        .agg(Records=("campaign","count"), Spend=("spend","sum"), Sales=("sales","sum"))
        .reset_index().sort_values("date", ascending=False)
    )
    history["date"] = history["date"].dt.strftime("%Y-%m-%d")
    st.dataframe(
        history.style.format({"Spend":"₹{:,.2f}", "Sales":"₹{:,.2f}"}),
        hide_index=True, use_container_width=True,
    )


def _render_settings(sb):
    st.subheader("⚙️ Marketing Settings")
    s1, s2, s3, s4, s5 = st.tabs(["Master Data", "Mapping Manager", "Channel Map", "Product Map", "Data Cleanup"])

    with s1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 📢 Ad Channels")
            new_ch = st.text_input("New Channel Name", key="mkt_new_ch")
            if st.button("Add Channel", key="mkt_add_ch") and new_ch:
                if _add_channel(sb, new_ch):
                    st.success(f"'{new_ch}' added!")
                    st.rerun()
            channels = _get_channels(sb)
            if channels:
                st.dataframe(pd.DataFrame({"name": channels}), hide_index=True, use_container_width=True)
        with col2:
            st.markdown("##### 📦 Products")
            new_pr = st.text_input("New Product Name", key="mkt_new_pr")
            if st.button("Add Product", key="mkt_add_pr") and new_pr:
                if _add_product(sb, new_pr):
                    st.success(f"'{new_pr}' added!")
                    st.rerun()
            products = _get_products(sb)
            if products:
                st.dataframe(pd.DataFrame({"name": products}), hide_index=True, use_container_width=True)

    with s2:
        st.markdown("##### 🔗 Campaign → Product Mappings")
        df_map = _get_mappings(sb)
        if df_map.empty:
            st.info("No mappings yet.")
        else:
            search = st.text_input("Search campaigns", key="mkt_map_search")
            if search:
                df_map = df_map[df_map["campaign"].str.contains(search, case=False, na=False)]
            for idx, row in df_map.iterrows():
                mc1, mc2 = st.columns([4, 1])
                mc1.write(f"**{row['campaign']}** → {row['product_name']}")
                if mc2.button("Delete", key=f"mkt_del_map_{idx}"):
                    if _delete_mapping(sb, row["campaign"], row["product_name"]):
                        st.success("Deleted!")
                        st.rerun()

    with s3:
        st.markdown("##### 🔗 Channel Map — Marketing → Sales Channels")
        st.caption(
            "Map each **marketing channel** (as it appears in ad reports) to one or more "
            "**sales channels** (as they appear in the sales dashboard).  \n"
            "Example: **Amazon** → Amazon RKW + Amazon Seller"
        )

        # Fetch current map and available channels from both sides
        channel_map = _get_channel_map(sb)
        mkt_channels  = _get_channels(sb)
        sales_sb      = _get_sales_supabase()

        # Fetch sales channel names from sales DB
        try:
            sc_r = sales_sb.table("master_channels").select("name").execute()
            sales_channels = sorted([x["name"] for x in sc_r.data])
        except Exception:
            sales_channels = []
            st.warning("Could not load sales channels from sales DB.")

        # Display current mappings
        if channel_map:
            st.markdown("**Current mappings:**")
            for mkt_ch, sales_chs in sorted(channel_map.items()):
                for sc in sales_chs:
                    row_c1, row_c2 = st.columns([4, 1])
                    row_c1.write(f"**{mkt_ch}** → {sc}")
                    if row_c2.button("Remove", key=f"cmap_del_{mkt_ch}_{sc}"):
                        if _delete_channel_map_entry(sb, mkt_ch, sc):
                            st.success(f"Removed: {mkt_ch} → {sc}")
                            st.rerun()
        else:
            st.info("No channel mappings yet. Add one below.")

        st.divider()
        st.markdown("**Add a mapping:**")
        if not mkt_channels:
            st.warning("No marketing channels found. Add them in the Master Data tab first.")
        elif not sales_channels:
            st.warning("No sales channels found in the sales DB.")
        else:
            cm1, cm2 = st.columns(2)
            with cm1:
                new_mkt_ch = st.selectbox(
                    "Marketing channel (ad side)",
                    mkt_channels, key="cmap_mkt_ch",
                )
            with cm2:
                new_sales_ch = st.selectbox(
                    "Sales channel (revenue side)",
                    sales_channels, key="cmap_sales_ch",
                )
            if st.button("➕ Add Mapping", key="cmap_add"):
                if _save_channel_map_entry(sb, new_mkt_ch, new_sales_ch):
                    st.success(f"Added: **{new_mkt_ch}** → {new_sales_ch}")
                    st.rerun()

        # Preview what current map does to sales data
        if channel_map and sales_channels:
            with st.expander("🔍 Preview: how sales channels roll up"):
                preview_rows = []
                for mkt_ch, sales_chs in sorted(channel_map.items()):
                    for sc in sales_chs:
                        preview_rows.append({"Sales Channel": sc, "Rolls up to → ": mkt_ch})
                # Any sales channel not in map → maps to itself
                mapped_sales = {sc for scs in channel_map.values() for sc in scs}
                for sc in sales_channels:
                    if sc not in mapped_sales:
                        preview_rows.append({"Sales Channel": sc, "Rolls up to → ": sc + " (identity)"})
                st.dataframe(pd.DataFrame(preview_rows), hide_index=True, use_container_width=True)

    with s4:
        st.markdown("##### 🗺️ Product Map — Marketing Product → Sales SKUs")
        st.caption(
            "Map each **marketing product** (short name from ad reports) to all its **sales SKUs** "
            "(as they appear in the sales dashboard). Multi-pack and combo items are handled automatically.  \n"
            "Example: **Patal** → Patal Poha 100g, Patal Poha 37g, Patal Poha 100g x 2 …"
        )

        product_map_data = _get_product_map(sb)
        mkt_products = _get_products(sb)

        # Fetch sales item names from sales DB
        sales_sb2 = _get_sales_supabase()
        try:
            si_r = sales_sb2.table("sales").select("item_name").execute()
            all_sales_items = sorted(set(x["item_name"] for x in si_r.data))
        except Exception:
            all_sales_items = []
            st.warning("Could not load sales items from sales DB.")

        # Show current mappings
        if product_map_data:
            st.markdown("**Current mappings:**")
            for mkt_prod, sales_items in sorted(product_map_data.items()):
                st.markdown(f"**{mkt_prod}** ({len(sales_items)} SKUs)")
                for si in sorted(sales_items):
                    pc1, pc2 = st.columns([5, 1])
                    pc1.write(f"  → {si}")
                    if pc2.button("✕", key=f"pmap_del_{mkt_prod}_{si}"):
                        if _delete_product_map_entry(sb, mkt_prod, si):
                            st.success(f"Removed: {mkt_prod} → {si}")
                            st.rerun()
        else:
            st.info("No product mappings yet. Add them below or run the SQL setup script.")

        st.divider()
        st.markdown("**Add a mapping:**")
        if not mkt_products:
            st.warning("No marketing products configured. Add them in Master Data first.")
        elif not all_sales_items:
            st.warning("No sales items found in the sales DB.")
        else:
            pm1, pm2 = st.columns(2)
            with pm1:
                new_mkt_prod = st.selectbox(
                    "Marketing product", mkt_products, key="pmap_mkt_prod"
                )
            with pm2:
                new_sales_item = st.selectbox(
                    "Sales SKU", all_sales_items, key="pmap_sales_item"
                )
            if st.button("➕ Add Product Mapping", key="pmap_add"):
                if _save_product_map_entry(sb, new_mkt_prod, new_sales_item):
                    st.success(f"Added: **{new_mkt_prod}** → {new_sales_item}")
                    st.rerun()

        # Coverage summary
        if product_map_data and all_sales_items:
            with st.expander("📊 Coverage — which sales SKUs are mapped vs unmapped"):
                mapped_items = {si for items in product_map_data.values() for si in items}
                unmapped_items = [x for x in all_sales_items if x not in mapped_items]
                st.markdown(f"**Mapped:** {len(mapped_items)} SKUs | **Unmapped:** {len(unmapped_items)} SKUs")
                if unmapped_items:
                    st.markdown("*Unmapped SKUs (excluded from per-product TACOS — counted in total GMV only):*")
                    for u in unmapped_items:
                        st.write(f"  • {u}")

    with s5:
        st.markdown("##### 🗑️ Delete Performance Records")
        st.warning("This permanently removes data. Use with caution.")
        dc1, dc2 = st.columns(2)
        with dc1:
            channels  = _get_channels(sb)
            target_ch = st.selectbox("Channel", ["Select…"] + channels, key="mkt_del_ch")
        with dc2:
            target_dt = st.date_input("Date", value=None, key="mkt_del_date")
        if st.button("Delete Records", type="primary", key="mkt_del_btn"):
            if target_ch != "Select…" and target_dt:
                deleted = _delete_performance(sb, target_ch, target_dt.strftime("%Y-%m-%d"))
                if deleted:
                    st.success(f"Deleted {deleted} records.")
                    st.rerun()
                else:
                    st.info("No records found for that channel + date.")
            else:
                st.error("Select both a channel and a date.")


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────

def render_marketing_tab(role: str):
    """
    Renders the full Performance Marketing section.
    - Marketing data: MARKETING_SUPABASE_URL / MARKETING_SUPABASE_KEY
    - Sales data (TACOS): SUPABASE_URL / SUPABASE_KEY
    """
    sb = _get_marketing_supabase()

    if role == "admin":
        sub_tabs = st.tabs([
            "📊 Dashboard",
            "🔬 Deep Dive",
            "📐 ACOS & TACOS",
            "📥 Upload",
            "📚 History",
            "⚙️ Settings",
        ])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_deep_dive(sb)
        with sub_tabs[2]: _render_acos_tacos(sb)
        with sub_tabs[3]: _render_upload(sb)
        with sub_tabs[4]: _render_history(sb)
        with sub_tabs[5]: _render_settings(sb)
    else:
        sub_tabs = st.tabs([
            "📊 Dashboard",
            "🔬 Deep Dive",
            "📐 ACOS & TACOS",
            "📚 History",
        ])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_deep_dive(sb)
        with sub_tabs[2]: _render_acos_tacos(sb)
        with sub_tabs[3]: _render_history(sb)
