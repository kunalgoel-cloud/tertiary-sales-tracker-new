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

# ── Global Filter Integration ─────────────────────────────────────────────────
# Marketing module reads date + channel from the global filter state when
# available. Campaign/product filters remain local (marketing-specific).
# Import is guarded so the module still works standalone (e.g. in tests).
try:
    from global_filters import get_global_filters as _get_global_filters
    _GLOBAL_FILTERS_AVAILABLE = True
except ImportError:
    _GLOBAL_FILTERS_AVAILABLE = False

try:
    from ui_theme import apply_chart_theme, brand_color_sequence, section_header, empty_state
    _UI_THEME_AVAILABLE = True
except ImportError:
    def apply_chart_theme(f, **kw): return f
    def brand_color_sequence(): return None
    def section_header(i, t, s=""): import streamlit as st; st.markdown(f"### {i} {t}")
    def empty_state(i, t, b): import streamlit as st; st.info(f"{i} {t}: {b}")
    _UI_THEME_AVAILABLE = False


# ─────────────────────────────────────────────────────────────
# SUPABASE CLIENTS
# ─────────────────────────────────────────────────────────────

def _fmt_err(e: Exception) -> str:
    """Return a short, readable error message — strips HTML 502 bodies etc."""
    msg = str(e)
    if len(msg) > 300 or "<html" in msg.lower() or "<!doctype" in msg.lower():
        # Supabase returned an HTML error page (502, 503 etc.)
        code = "502" if "502" in msg else ("503" if "503" in msg else "server error")
        return f"Supabase {code} — server temporarily unavailable. Try again in a moment."
    return msg[:300]


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


@st.cache_data(ttl=120, show_spinner=False)
def _get_performance(_sb) -> pd.DataFrame:
    """
    Fetch marketing performance data. Cached for 120 s so the 5 sub-tabs
    that call this function share a single Supabase round-trip per session.
    The leading underscore on _sb tells Streamlit not to hash the client object.
    """
    try:
        all_rows = []
        page, PAGE_SIZE = 0, 1000
        while True:
            r = _sb.table("performance").select("*").order("date", desc=True)\
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
        st.error(f"Error fetching performance data: {_fmt_err(e)}")
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
# BRAND SPEND DB HELPERS
# ─────────────────────────────────────────────────────────────

def _get_branding_channels(sb) -> list[str]:
    """Fetch configured branding channel names."""
    try:
        r = sb.table("branding_channels").select("name").order("name").execute()
        return [x["name"] for x in r.data]
    except Exception:
        return []

def _add_branding_channel(sb, name: str) -> bool:
    try:
        sb.table("branding_channels").insert({"name": name}).execute()
        return True
    except Exception as e:
        st.error(f"Error adding branding channel: {e}")
        return False

def _delete_branding_channel(sb, name: str) -> bool:
    try:
        sb.table("branding_channels").delete().eq("name", name).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting branding channel: {e}")
        return False

def _get_brand_spends(sb) -> pd.DataFrame:
    """Fetch all brand spend records."""
    try:
        all_rows, page, PAGE_SIZE = [], 0, 1000
        while True:
            r = sb.table("brand_spends").select("*").order("year", desc=True)                .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1).execute()
            if not r.data: break
            all_rows.extend(r.data)
            if len(r.data) < PAGE_SIZE: break
            page += 1
        if not all_rows:
            return pd.DataFrame(columns=["id","year","month","channel","product","amount"])
        df = pd.DataFrame(all_rows)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        return df
    except Exception as e:
        st.warning(f"Could not load brand spends: {_fmt_err(e)}")
        return pd.DataFrame(columns=["id","year","month","channel","product","amount"])

def _save_brand_spend(sb, year: int, month: int, channel: str,
                      product: str | None, amount: float) -> bool:
    try:
        sb.table("brand_spends").insert({
            "year": year, "month": month,
            "channel": channel,
            "product": product if product else None,
            "amount": float(amount),
        }).execute()
        return True
    except Exception as e:
        st.error(f"Error saving brand spend: {e}")
        return False

def _delete_brand_spend(sb, record_id: int) -> bool:
    try:
        sb.table("brand_spends").delete().eq("id", record_id).execute()
        return True
    except Exception as e:
        st.error(f"Error deleting brand spend: {e}")
        return False

def _expand_brand_spends_daily(brand_df: pd.DataFrame,
                                start_date, end_date,
                                product_map: dict) -> pd.DataFrame:
    """
    Given brand_spends records, expand into a daily series so they can be
    added to perf spend in the ACOS/TACOS view.

    Rules:
    - Each record covers the full calendar month (year, month).
    - Daily brand spend = monthly amount / days_in_month.
    - If product is None → split proportionally across products using product_map
      keys (equal split as proxy; actual revenue-weighted split done at display time).
    - Returns: date, channel, product (nullable), brand_spend_daily
    """
    import calendar
    rows = []
    for _, rec in brand_df.iterrows():
        y, m, ch = int(rec["year"]), int(rec["month"]), rec["channel"]
        amount    = float(rec["amount"])
        product   = rec.get("product")
        if pd.isna(product) or str(product).strip() in ("", "None"):
            product = None

        days_in_month = calendar.monthrange(y, m)[1]
        daily_amt     = amount / days_in_month

        # Generate daily rows for days that fall within the requested date range
        month_start = pd.Timestamp(y, m, 1)
        month_end   = pd.Timestamp(y, m, days_in_month)
        window_start = pd.Timestamp(start_date)
        window_end   = pd.Timestamp(end_date)

        effective_start = max(month_start, window_start)
        effective_end   = min(month_end, window_end)

        if effective_start > effective_end:
            continue

        dates = pd.date_range(effective_start, effective_end, freq="D")
        for d in dates:
            rows.append({
                "date":              d,
                "channel":           ch,
                "product":           product,
                "brand_spend_daily": daily_amt,
            })

    if not rows:
        return pd.DataFrame(columns=["date","channel","product","brand_spend_daily"])
    return pd.DataFrame(rows)


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
        st.warning(f"Could not load sales data for TACOS: {_fmt_err(e)}")
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
    """
    Apply filters to a marketing performance DataFrame.

    GLOBAL FILTER INTEGRATION:
    - Date range and channel filter are read from the global filter state
      (st.session_state["gf_*"]) when the global filter system is active.
    - This keeps marketing date/channel in sync with the rest of the app.
    - The campaign/product filter remains LOCAL (marketing-specific concept,
      not shared with sales tabs).

    Fallback: if global_filters module is unavailable, renders its own
    date + channel widgets (backward-compatible).
    """
    min_date = df["date"].min().date()
    max_date = df["date"].max().date()

    # ── Read date + channel from global filter state ──────────────────────────
    if _GLOBAL_FILTERS_AVAILABLE:
        _gf    = _get_global_filters()
        start  = _gf["start"] or min_date
        end    = _gf["end"]   or max_date
        # Clamp to available data range
        start  = max(start, min_date)
        end    = min(end,   max_date)
        # Channel: use global selection if set; otherwise all marketing channels
        all_channels = sorted(df["channel"].unique())
        gf_chans     = _gf["channels"]
        # Only apply global channels that exist in marketing data
        ch_f = [c for c in (gf_chans or []) if c in all_channels] or all_channels
        # Show info label so user knows filters are from global bar
        st.caption(
            f"📅 Period: **{start}** → **{end}** | "
            f"Channels: **{', '.join(ch_f) if ch_f != all_channels else 'All'}** "
            f"_(from Global Filter Bar above tabs)_"
        )
    else:
        # ── Fallback: render local widgets (no global filter system) ──────────
        fc1, fc2 = st.columns([2, 2])
        with fc1:
            dr = st.date_input(
                "Date Range", value=(min_date, max_date),
                min_value=min_date, max_value=max_date,
                key=f"{key_prefix}_dr",
            )
        with fc2:
            all_channels = sorted(df["channel"].unique())
            ch_f = st.multiselect("Channels", all_channels, default=all_channels, key=f"{key_prefix}_ch")
        start = dr[0] if len(dr) == 2 else min_date
        end   = dr[1] if len(dr) == 2 else max_date

    # ── LOCAL filter: Product (marketing-specific, not in global bar) ─────────
    if show_product and "product" in df.columns:
        all_products = sorted(df["product"].unique())
        pr_f = st.multiselect(
            "Products (local filter)",
            all_products, default=all_products,
            key=f"{key_prefix}_pr",
            help="This product filter is local to the Marketing tab only.",
        )
    else:
        pr_f = []

    # ── Apply all filters ─────────────────────────────────────────────────────
    f = df.copy()
    f = f[(f["date"] >= pd.to_datetime(start)) & (f["date"] <= pd.to_datetime(end))]
    if ch_f:
        f = f[f["channel"].isin(ch_f)]
    if pr_f and "product" in df.columns:
        f = f[f["product"].isin(pr_f)]

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
    fig = apply_chart_theme(fig)
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown("**By Channel**")
        ch_sum = (
            f_df.groupby("channel").agg(spend=("spend","sum"), sales=("sales","sum"))
            .assign(ROAS=lambda x: x.sales / x.spend,
                    ACOS=lambda x: pd.to_numeric(x.spend / x.sales.where(x.sales > 0) * 100, errors="coerce").round(1))
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
                    ACOS=lambda x: pd.to_numeric(x.spend / x.sales.where(x.sales > 0) * 100, errors="coerce").round(1))
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
                ACOS=lambda x: pd.to_numeric(x.spend / x.sales.where(x.sales > 0) * 100, errors="coerce").round(1))
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
    detail["ROAS"] = pd.to_numeric(detail["sales"] / detail["spend"].where(detail["spend"] > 0), errors="coerce").round(2)
    detail["ACOS"] = pd.to_numeric(detail["spend"] / detail["sales"].where(detail["sales"] > 0) * 100, errors="coerce").round(1)
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
# DEEP DIVE  — Investment Decision Engine
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# DEEP DIVE  — Investment Decision Engine
# ─────────────────────────────────────────────────────────────

def _score_campaign(grp: pd.DataFrame, window_days: int) -> dict:
    """
    Score a campaign's performance over a given window.
    Returns metrics used for investment decisions.
    """
    import numpy as np
    grp = grp.sort_values("date")
    daily = grp.groupby("date").agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
    daily["roas"] = daily["sales"] / daily["spend"].where(daily["spend"] > 0)

    total_spend  = daily["spend"].sum()
    total_sales  = daily["sales"].sum()
    avg_roas     = total_sales / total_spend if total_spend > 0 else 0
    active_days  = len(daily)

    # Volatility: coefficient of variation of daily ROAS (lower = more consistent)
    roas_vals = daily["roas"].dropna()
    if len(roas_vals) >= 2 and roas_vals.mean() > 0:
        volatility = roas_vals.std() / roas_vals.mean()
    else:
        volatility = float("nan")

    # Trend: slope of ROAS over time (positive = improving)
    if len(roas_vals) >= 3:
        x = np.arange(len(roas_vals))
        slope = np.polyfit(x, roas_vals.values, 1)[0]
        trend_pct = (slope * len(roas_vals) / max(roas_vals.mean(), 0.01)) * 100
    else:
        trend_pct = 0.0

    # Consistency: % of active days with ROAS > 1
    consistency = (roas_vals > 1).mean() * 100 if len(roas_vals) > 0 else 0

    # Investment verdict
    if avg_roas >= 2.0 and (pd.isna(volatility) or volatility < 0.5):
        verdict = "SCALE"
    elif avg_roas >= 1.5 and (pd.isna(volatility) or volatility < 0.7):
        verdict = "SCALE"
    elif avg_roas >= 1.0 and (pd.isna(volatility) or volatility < 0.8):
        verdict = "MAINTAIN"
    elif avg_roas >= 1.0 and not pd.isna(volatility) and volatility >= 0.8:
        verdict = "MONITOR"
    elif avg_roas < 1.0 and trend_pct > 15:
        verdict = "WATCH"
    elif avg_roas < 1.0 and trend_pct <= 15 and avg_roas >= 0.7:
        verdict = "PAUSE"
    else:
        verdict = "CUT"

    return {
        "total_spend": total_spend,
        "total_sales": total_sales,
        "avg_roas": avg_roas,
        "volatility": volatility,
        "trend_pct": trend_pct,
        "active_days": active_days,
        "consistency": consistency,
        "verdict": verdict,
        "daily": daily,
    }


VERDICT_COLOR = {
    "SCALE":    "#16a34a",  # green
    "MAINTAIN": "#2563eb",  # blue
    "MONITOR":  "#d97706",  # amber
    "WATCH":    "#7c3aed",  # purple
    "PAUSE":    "#dc2626",  # red
    "CUT":      "#6b7280",  # grey
}
VERDICT_BG = {
    "SCALE":    "#f0fdf4",
    "MAINTAIN": "#eff6ff",
    "MONITOR":  "#fffbeb",
    "WATCH":    "#f5f3ff",
    "PAUSE":    "#fef2f2",
    "CUT":      "#f9fafb",
}
VERDICT_DESC = {
    "SCALE":    "High ROAS + consistent → Increase budget",
    "MAINTAIN": "Profitable + stable → Keep current spend",
    "MONITOR":  "Profitable but volatile → Watch closely, don't increase",
    "WATCH":    "Below break-even but improving → Give it 1 more week",
    "PAUSE":    "Below break-even, not improving → Pause spend now",
    "CUT":      "Consistently loss-making → Stop immediately",
}


def _render_deep_dive(sb):
    st.subheader("🔬 Campaign Investment Intelligence")
    st.caption(
        "Every campaign is scored like a stock: **return** (ROAS), **risk** (volatility), "
        "**trend** (direction) and **consistency**. The verdict tells you exactly what to do with the budget."
    )

    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No marketing data yet.")
        return

    df_p = df_p.drop(columns=["id","created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])
    df_p["date_only"] = df_p["date"].dt.date

    # ── Global controls ──────────────────────────────────────
    ctrl1, ctrl2, ctrl3 = st.columns([2, 2, 1])
    with ctrl1:
        all_channels = sorted(df_p["channel"].unique())
        sel_channels = st.multiselect("Channels", all_channels, default=all_channels, key="dd_ch")
    with ctrl2:
        window = st.radio(
            "Performance Window",
            ["2 Weeks", "4 Weeks", "All Time"],
            horizontal=True, index=1, key="dd_window",
        )
    with ctrl3:
        min_spend = st.number_input("Min spend filter (₹)", value=0, step=100, key="dd_min_spend")

    if sel_channels:
        df_p = df_p[df_p["channel"].isin(sel_channels)]

    # Apply window
    max_date = df_p["date"].max()
    if window == "2 Weeks":
        cutoff = max_date - pd.Timedelta(days=13)
    elif window == "4 Weeks":
        cutoff = max_date - pd.Timedelta(days=27)
    else:
        cutoff = df_p["date"].min()
    win_df = df_p[df_p["date"] >= cutoff].copy()

    if win_df.empty:
        st.warning("No data in the selected window.")
        return

    # Score every campaign
    import numpy as np
    records = []
    for camp, grp in win_df.groupby("campaign"):
        sc = _score_campaign(grp, (max_date - cutoff).days + 1)
        if sc["total_spend"] < min_spend:
            continue
        records.append({
            "Campaign":      camp,
            "Channel":       grp["channel"].iloc[0],
            "Spend (₹)":     sc["total_spend"],
            "Ad Revenue (₹)":sc["total_sales"],
            "ROAS":          round(sc["avg_roas"], 2),
            "Volatility":    round(sc["volatility"], 2) if not pd.isna(sc["volatility"]) else None,
            "Trend":         round(sc["trend_pct"], 1),
            "Active Days":   sc["active_days"],
            "Consistency %": round(sc["consistency"], 0),
            "Verdict":       sc["verdict"],
            "_daily":        sc["daily"],
        })

    if not records:
        st.warning("No campaigns meet the filter criteria.")
        return

    score_df = pd.DataFrame(records)

    # ── TAB NAVIGATION ───────────────────────────────────────
    tabs = st.tabs([
        "📋 Investment Decisions",
        "📦 Portfolio Buckets",
        "📈 Campaign Scorecards",
        "📊 Volatility vs Return",
    ])

    # ════════════════════════════════════════════════════════
    # TAB 1 — INVESTMENT DECISION TABLE
    # ════════════════════════════════════════════════════════
    with tabs[0]:
        st.markdown("#### 📋 Campaign Investment Decisions")
        st.caption(
            "Every campaign ranked by verdict. "
            "**Trend** = ROAS direction over the window (positive = improving). "
            "**Volatility** = coefficient of variation of daily ROAS (lower = more reliable, like a low-beta stock). "
            "**Consistency** = % of days with ROAS > 1."
        )

        verdict_order = ["SCALE","MAINTAIN","MONITOR","WATCH","PAUSE","CUT"]
        display_df = score_df.drop(columns=["_daily"]).copy()
        display_df["Verdict_sort"] = display_df["Verdict"].map({v:i for i,v in enumerate(verdict_order)})
        display_df = display_df.sort_values(["Verdict_sort","Spend (₹)"], ascending=[True, False])\
                               .drop(columns=["Verdict_sort"])

        # Colour verdict column
        def colour_verdict(val):
            bg = VERDICT_BG.get(val, "#ffffff")
            fg = VERDICT_COLOR.get(val, "#000000")
            return f"background-color: {bg}; color: {fg}; font-weight: bold"

        def colour_roas(val):
            if not isinstance(val, (int, float)) or pd.isna(val):
                return ""
            if val >= 2:   return "color: #16a34a; font-weight: bold"
            if val >= 1:   return "color: #2563eb"
            return "color: #dc2626"

        def colour_trend(val):
            if not isinstance(val, (int, float)) or pd.isna(val):
                return ""
            return "color: #16a34a" if val > 5 else ("color: #dc2626" if val < -5 else "color: #6b7280")

        styled = display_df.style\
            .format({
                "Spend (₹)":"₹{:,.0f}", "Ad Revenue (₹)":"₹{:,.0f}",
                "ROAS":"{:.2f}x", "Trend":"{:+.1f}%",
                "Consistency %":"{:.0f}%",
                "Volatility": lambda v: f"{v:.2f}" if v is not None and not (isinstance(v, float) and pd.isna(v)) else "—",
            })\
            .map(colour_verdict, subset=["Verdict"])\
            .map(colour_roas,    subset=["ROAS"])\
            .map(colour_trend,   subset=["Trend"])

        st.dataframe(styled, hide_index=True, use_container_width=True, height=500)

        # Legend
        st.markdown("**Verdict guide:**")
        leg_cols = st.columns(6)
        for i, v in enumerate(verdict_order):
            leg_cols[i].markdown(
                f"<div style='background:{VERDICT_BG[v]};color:{VERDICT_COLOR[v]};"
                f"border-radius:6px;padding:6px 8px;font-size:12px;font-weight:bold'>"
                f"{v}<br><span style='font-weight:normal;font-size:11px'>{VERDICT_DESC[v]}</span></div>",
                unsafe_allow_html=True,
            )

        st.divider()
        # Download
        dl_df = display_df.drop(columns=[], errors="ignore")
        st.download_button(
            "📥 Download Decision Table",
            dl_df.to_csv(index=False),
            file_name=f"campaign_decisions_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    # ════════════════════════════════════════════════════════
    # TAB 2 — PORTFOLIO BUCKETS
    # ════════════════════════════════════════════════════════
    with tabs[1]:
        st.markdown("#### 📦 Portfolio Buckets")
        st.caption(
            "Campaigns grouped by investment verdict. "
            "Use this to decide total budget per bucket — not per campaign."
        )

        for verdict in ["SCALE","MAINTAIN","MONITOR","WATCH","PAUSE","CUT"]:
            bucket = score_df[score_df["Verdict"] == verdict]
            if bucket.empty:
                continue

            b_spend  = bucket["Spend (₹)"].sum()
            b_rev    = bucket["Ad Revenue (₹)"].sum()
            b_roas   = b_rev / b_spend if b_spend > 0 else 0
            b_n      = len(bucket)

            with st.expander(
                f"{verdict} — {b_n} campaigns | ₹{b_spend:,.0f} spend | "
                f"{b_roas:.2f}x ROAS avg",
                expanded=(verdict in ["SCALE","MAINTAIN","PAUSE","CUT"]),
            ):
                st.markdown(
                    f"<div style='background:{VERDICT_BG[verdict]};color:{VERDICT_COLOR[verdict]};"
                    f"border-left:4px solid {VERDICT_COLOR[verdict]};padding:10px;border-radius:4px;"
                    f"margin-bottom:12px;font-size:13px'>{VERDICT_DESC[verdict]}</div>",
                    unsafe_allow_html=True,
                )

                b1, b2, b3, b4 = st.columns(4)
                b1.metric("Campaigns", b_n)
                b2.metric("Total Spend", f"₹{b_spend:,.0f}")
                b3.metric("Total Revenue", f"₹{b_rev:,.0f}")
                b4.metric("Bucket ROAS", f"{b_roas:.2f}x")

                disp = bucket[["Campaign","Channel","Spend (₹)","ROAS","Volatility","Trend","Active Days"]]\
                       .sort_values("Spend (₹)", ascending=False)
                st.dataframe(
                    disp.style.format({
                        "Spend (₹)":"₹{:,.0f}", "ROAS":"{:.2f}x", "Trend":"{:+.1f}%",
                        "Volatility": lambda v: f"{v:.2f}" if v is not None and not (isinstance(v, float) and pd.isna(v)) else "—",
                    }),
                    hide_index=True, use_container_width=True,
                )

    # ════════════════════════════════════════════════════════
    # TAB 3 — CAMPAIGN SCORECARDS  (stock-market style)
    # ════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown("#### 📈 Campaign Scorecards")
        st.caption(
            "Each card = one campaign. The sparkline is daily ROAS — "
            "read it like a stock chart. Flat or rising = good. Spikey = volatile."
        )

        # Filter to top N by spend for readability
        top_n = st.slider("Show top N campaigns by spend", 5, 50,
                          min(20, len(score_df)), key="dd_topn")
        shown = score_df.nlargest(top_n, "Spend (₹)")

        # 3-per-row layout
        cols_per_row = 3
        rows = [shown.iloc[i:i+cols_per_row] for i in range(0, len(shown), cols_per_row)]

        for row in rows:
            cols = st.columns(cols_per_row)
            for j, (_, camp_row) in enumerate(row.iterrows()):
                with cols[j]:
                    verdict = camp_row["Verdict"]
                    daily   = camp_row["_daily"]
                    camp    = camp_row["Campaign"]
                    roas    = camp_row["ROAS"]
                    vol     = camp_row["Volatility"]
                    trend   = camp_row["Trend"]

                    # Truncate campaign name
                    short_name = camp[:28] + "…" if len(camp) > 28 else camp

                    # Sparkline
                    fig_spark = go.Figure()
                    fig_spark.add_trace(go.Scatter(
                        x=daily["date"], y=daily["roas"],
                        mode="lines", fill="tozeroy",
                        line=dict(
                            color=VERDICT_COLOR[verdict], width=2
                        ),
                        fillcolor=VERDICT_BG[verdict],
                    ))
                    fig_spark.add_hline(y=1, line_dash="dot", line_color="#dc2626", line_width=1)
                    fig_spark.update_layout(
                        height=100, margin=dict(l=0,r=0,t=0,b=0),
                        showlegend=False,
                        xaxis=dict(visible=False),
                        yaxis=dict(visible=False, rangemode="tozero"),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                    )

                    trend_arrow = "↗" if trend > 5 else ("↘" if trend < -5 else "→")
                    vol_str = f"{vol:.2f}" if vol is not None and not (isinstance(vol, float) and pd.isna(vol)) else "—"

                    st.markdown(
                        f"<div style='border:1px solid {VERDICT_COLOR[verdict]};border-radius:8px;"
                        f"padding:10px;background:{VERDICT_BG[verdict]};margin-bottom:4px'>"
                        f"<div style='font-size:11px;color:#6b7280;white-space:nowrap;overflow:hidden;"
                        f"text-overflow:ellipsis'>{short_name}</div>"
                        f"<div style='font-size:18px;font-weight:bold;color:{VERDICT_COLOR[verdict]}'>{verdict}</div>"
                        f"<div style='font-size:13px'>ROAS: <b>{roas:.2f}x</b> &nbsp; {trend_arrow} {trend:+.1f}%</div>"
                        f"<div style='font-size:12px;color:#6b7280'>Vol: {vol_str} | ₹{camp_row['Spend (₹)']:,.0f}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                    st.plotly_chart(fig_spark, use_container_width=True, config={"displayModeBar": False})

    # ════════════════════════════════════════════════════════
    # TAB 4 — VOLATILITY vs RETURN (Risk/Return scatter)
    # ════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown("#### 📊 Risk vs Return — Campaign Portfolio Map")
        st.caption(
            "**X-axis = Volatility** (like beta in stocks — lower is more predictable). "
            "**Y-axis = ROAS** (return). "
            "**Bubble size = spend**. "
            "Top-left quadrant = high return, low risk = best campaigns to scale."
        )

        plot_df = score_df.dropna(subset=["Volatility"]).copy()
        if plot_df.empty:
            st.info("Need at least 2 active days per campaign to calculate volatility.")
        else:
            fig_rv = px.scatter(
                plot_df,
                x="Volatility", y="ROAS",
                size="Spend (₹)", color="Verdict",
                hover_name="Campaign",
                hover_data={"Channel":True,"Spend (₹)":":.0f",
                            "ROAS":":.2f","Volatility":":.2f","Trend":True},
                color_discrete_map=VERDICT_COLOR,
                size_max=50, height=520,
                labels={"Volatility":"Volatility (lower = more consistent)",
                        "ROAS":"ROAS (higher = better return)"},
            )

            # Quadrant lines at median
            med_vol  = plot_df["Volatility"].median()
            med_roas = max(plot_df["ROAS"].median(), 0)

            fig_rv.add_vline(x=med_vol,  line_dash="dot", line_color="#9ca3af",
                             annotation_text="Median Volatility", annotation_position="top")
            fig_rv.add_hline(y=med_roas, line_dash="dot", line_color="#9ca3af",
                             annotation_text="Median ROAS", annotation_position="right")
            fig_rv.add_hline(y=1, line_dash="dash", line_color="#dc2626",
                             annotation_text="Break-even", annotation_position="right")

            # Quadrant labels
            x_max = plot_df["Volatility"].max() * 1.1
            y_max = plot_df["ROAS"].max() * 1.1
            for txt, x, y, color in [
                ("⭐ Scale",          med_vol*0.3,  y_max*0.95,  "#16a34a"),
                ("💎 Gem — Invest",   med_vol*0.3,  med_roas*0.5,"#2563eb"),
                ("⚠️ Risky High",      x_max*0.8,   y_max*0.95,  "#d97706"),
                ("🗑️ Cut",             x_max*0.8,   med_roas*0.5,"#6b7280"),
            ]:
                fig_rv.add_annotation(x=x, y=y, text=f"<b>{txt}</b>", showarrow=False,
                                      font=dict(color=color, size=12))

            fig_rv.update_layout(hovermode="closest")
            fig_rv = apply_chart_theme(fig_rv)
            st.plotly_chart(fig_rv, use_container_width=True)

            # Summary table sorted by ROAS desc, volatility asc
            summary = plot_df[["Campaign","Spend (₹)","ROAS","Volatility","Trend","Verdict"]]\
                      .sort_values(["ROAS","Volatility"], ascending=[False, True])
            st.dataframe(
                summary.style.format({
                    "Spend (₹)":"₹{:,.0f}", "ROAS":"{:.2f}x",
                    "Volatility":"{:.2f}", "Trend":"{:+.1f}%",
                }).map(colour_verdict, subset=["Verdict"]),
                hide_index=True, use_container_width=True,
            )


# ─────────────────────────────────────────────────────────────
# FORECAST & BUDGET OPTIMIZER
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# FORECAST & BUDGET OPTIMIZER
# ─────────────────────────────────────────────────────────────

def _render_forecast(sb):
    st.subheader("🎯 Budget Optimizer & Forecast")
    st.caption(
        "Enter your total available budget. Set constraints. "
        "The optimizer allocates budget across campaigns to maximise expected revenue, "
        "weighted by each campaign's historical ROAS and consistency."
    )

    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No marketing data yet.")
        return

    df_p = df_p.drop(columns=["id","created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])
    import numpy as np

    # ── Inputs ───────────────────────────────────────────────
    st.markdown("### ⚙️ Constraints")
    ic1, ic2, ic3 = st.columns(3)
    with ic1:
        total_budget   = st.number_input("Total Budget (₹)", value=50000, step=1000,
                                          min_value=1000, key="fc_budget")
        perf_window    = st.radio("Base forecasts on:", ["Last 2 Weeks","Last 4 Weeks"],
                                   horizontal=True, key="fc_window")
    with ic2:
        min_roas_thresh = st.number_input("Min ROAS to invest in campaign",
                                           value=0.8, step=0.1, min_value=0.0, key="fc_min_roas")
        max_camp_pct   = st.slider("Max % of budget to any single campaign",
                                    5, 80, 40, key="fc_max_pct")
    with ic3:
        min_camp_spend = st.number_input("Min spend per campaign (₹) if included",
                                          value=500, step=100, key="fc_min_camp")
        exclude_verdicts = st.multiselect(
            "Exclude these verdicts from allocation",
            ["SCALE","MAINTAIN","MONITOR","WATCH","PAUSE","CUT"],
            default=["PAUSE","CUT"], key="fc_exclude",
        )

    st.divider()

    # ── Score campaigns ──────────────────────────────────────
    max_date = df_p["date"].max()
    days     = 13 if perf_window == "Last 2 Weeks" else 27
    win_df   = df_p[df_p["date"] >= max_date - pd.Timedelta(days=days)].copy()

    if win_df.empty:
        st.warning("No data in the selected window.")
        return

    camp_scores = []
    for camp, grp in win_df.groupby("campaign"):
        sc = _score_campaign(grp, days + 1)
        if sc["avg_roas"] < min_roas_thresh:
            continue
        if sc["verdict"] in exclude_verdicts:
            continue
        if sc["total_spend"] == 0:
            continue

        # Risk-adjusted ROAS: penalise volatility
        # risk_adj_roas = avg_roas / (1 + volatility)
        vol = sc["volatility"] if not pd.isna(sc["volatility"]) else 1.0
        risk_adj_roas = sc["avg_roas"] / (1 + vol)

        camp_scores.append({
            "campaign":       camp,
            "channel":        grp["channel"].iloc[0],
            "hist_spend":     sc["total_spend"],
            "avg_roas":       sc["avg_roas"],
            "volatility":     vol,
            "risk_adj_roas":  risk_adj_roas,
            "verdict":        sc["verdict"],
            "active_days":    sc["active_days"],
            "daily_spend_avg": sc["total_spend"] / max(sc["active_days"], 1),
        })

    if not camp_scores:
        st.warning(
            "No campaigns pass the current constraints. "
            "Try lowering the Min ROAS threshold or removing verdict exclusions."
        )
        return

    scores_df = pd.DataFrame(camp_scores).sort_values("risk_adj_roas", ascending=False)

    # ── Budget allocation (proportional to risk-adjusted ROAS) ─
    max_per_camp = total_budget * max_camp_pct / 100
    total_weight = scores_df["risk_adj_roas"].sum()

    scores_df["raw_alloc"]   = (scores_df["risk_adj_roas"] / total_weight * total_budget)
    scores_df["alloc"]       = scores_df["raw_alloc"].clip(upper=max_per_camp)

    # Redistribute surplus from capped campaigns
    surplus = total_budget - scores_df["alloc"].sum()
    if surplus > 1:
        uncapped = scores_df[scores_df["alloc"] < max_per_camp].copy()
        if not uncapped.empty:
            uncap_weight = uncapped["risk_adj_roas"].sum()
            scores_df.loc[uncapped.index, "alloc"] += (
                uncapped["risk_adj_roas"] / uncap_weight * surplus
            ).clip(upper=max_per_camp - uncapped["alloc"])

    # Apply minimum spend — remove campaigns below min
    scores_df = scores_df[scores_df["alloc"] >= min_camp_spend].copy()
    if scores_df.empty:
        st.warning("All campaigns allocated below minimum spend. Lower Min spend per campaign.")
        return

    # Re-normalise to total_budget after removing sub-minimum
    total_alloc = scores_df["alloc"].sum()
    scores_df["alloc"] = (scores_df["alloc"] / total_alloc * total_budget).round(0)

    # Forecast expected revenue
    scores_df["expected_revenue"] = (scores_df["alloc"] * scores_df["avg_roas"]).round(0)
    scores_df["expected_roas"]    = (scores_df["expected_revenue"] / scores_df["alloc"]).round(2)

    # ── Summary KPIs ─────────────────────────────────────────
    total_expected = scores_df["expected_revenue"].sum()
    total_roas_est = total_expected / total_budget if total_budget > 0 else 0
    n_camps        = len(scores_df)

    st.markdown("### 📊 Allocation Summary")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Budget",        f"₹{total_budget:,.0f}")
    k2.metric("Campaigns Funded",    n_camps)
    k3.metric("Expected Revenue",    f"₹{total_expected:,.0f}")
    k4.metric("Blended ROAS",        f"{total_roas_est:.2f}x")

    st.divider()

    # ── Allocation table ─────────────────────────────────────
    st.markdown("### 💰 Recommended Budget Allocation")

    disp = scores_df[[
        "campaign","channel","verdict","avg_roas","volatility",
        "alloc","expected_revenue","expected_roas"
    ]].copy()
    disp.columns = [
        "Campaign","Channel","Verdict","Hist ROAS","Volatility",
        "Allocated (₹)","Expected Revenue (₹)","Expected ROAS"
    ]

    def colour_verdict_fc(val):
        bg = VERDICT_BG.get(val, "#ffffff")
        fg = VERDICT_COLOR.get(val, "#000000")
        return f"background-color: {bg}; color: {fg}; font-weight: bold"

    st.dataframe(
        disp.style.format({
            "Hist ROAS":"{:.2f}x",
            "Volatility":"{:.2f}",
            "Allocated (₹)":"₹{:,.0f}",
            "Expected Revenue (₹)":"₹{:,.0f}",
            "Expected ROAS":"{:.2f}x",
        }).map(colour_verdict_fc, subset=["Verdict"]),
        hide_index=True, use_container_width=True, height=450,
    )

    # ── Allocation bar chart ─────────────────────────────────
    st.markdown("### 📊 Budget Split Visualisation")
    fig_alloc = go.Figure()
    fig_alloc.add_trace(go.Bar(
        x=scores_df["campaign"],
        y=scores_df["alloc"],
        name="Allocated Budget (₹)",
        marker_color=[VERDICT_COLOR.get(v,"#6b7280") for v in scores_df["verdict"]],
        text=scores_df["alloc"].apply(lambda x: f"₹{x:,.0f}"),
        textposition="outside",
    ))
    fig_alloc.add_trace(go.Scatter(
        x=scores_df["campaign"],
        y=scores_df["expected_revenue"],
        name="Expected Revenue (₹)",
        yaxis="y2", mode="markers",
        marker=dict(symbol="diamond", size=10, color="#1d4ed8"),
    ))
    fig_alloc.update_layout(
        height=430,
        xaxis_tickangle=-40,
        yaxis=dict(title="Budget Allocated (₹)"),
        yaxis2=dict(title="Expected Revenue (₹)", overlaying="y", side="right"),
        barmode="group",
        legend=dict(orientation="h", y=1.1),
        hovermode="x unified",
    )
    fig_alloc = apply_chart_theme(fig_alloc)
    st.plotly_chart(fig_alloc, use_container_width=True)

    # ── What-if simulator ────────────────────────────────────
    st.divider()
    st.markdown("### 🔮 What-If: Budget Scenarios")
    st.caption("See how expected revenue changes across different total budgets.")

    scenario_budgets = [
        total_budget * 0.5,
        total_budget * 0.75,
        total_budget,
        total_budget * 1.25,
        total_budget * 1.5,
        total_budget * 2.0,
    ]
    scenario_rows = []
    for sb_val in scenario_budgets:
        # Simple linear scale of allocations
        scale = sb_val / total_budget
        exp_rev = (scores_df["expected_revenue"] * scale).sum()
        exp_roas = exp_rev / sb_val if sb_val > 0 else 0
        scenario_rows.append({
            "Budget (₹)": round(sb_val, 0),
            "Expected Revenue (₹)": round(exp_rev, 0),
            "Expected ROAS": round(exp_roas, 2),
            "vs Current": f"{(scale-1)*100:+.0f}%",
        })

    sc_df = pd.DataFrame(scenario_rows)
    sc_df["highlight"] = sc_df["Budget (₹)"] == total_budget

    fig_sc = px.line(
        sc_df, x="Budget (₹)", y="Expected Revenue (₹)",
        markers=True, height=320,
        labels={"Budget (₹)":"Budget (₹)","Expected Revenue (₹)":"Expected Revenue (₹)"},
    )
    # Mark current budget
    cur_row = sc_df[sc_df["highlight"]]
    fig_sc.add_scatter(
        x=cur_row["Budget (₹)"], y=cur_row["Expected Revenue (₹)"],
        mode="markers", marker=dict(size=14, color="#dc2626", symbol="star"),
        name="Current Budget",
    )
    fig_sc.update_layout(hovermode="x unified")
    fig_sc = apply_chart_theme(fig_sc)
    st.plotly_chart(fig_sc, use_container_width=True)

    st.dataframe(
        sc_df.drop(columns=["highlight"]).style.format({
            "Budget (₹)":"₹{:,.0f}",
            "Expected Revenue (₹)":"₹{:,.0f}",
            "Expected ROAS":"{:.2f}x",
        }),
        hide_index=True, use_container_width=True,
    )

    # ── Download plan ────────────────────────────────────────
    st.download_button(
        "📥 Download Allocation Plan",
        disp.to_csv(index=False),
        file_name=f"budget_plan_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )



# ─────────────────────────────────────────────────────────────
# BRAND SPENDS TAB
# ─────────────────────────────────────────────────────────────

def _render_brand_spends(sb):
    st.subheader("🏷️ Brand Spend Tracker")
    st.caption(
        "Record monthly brand / awareness ad investments. These are not campaign-level "
        "attributable spends — they apply to the whole company or a specific channel. "
        "Brand spend is distributed equally across each day of the month for TACOS reporting."
    )

    products       = _get_products(sb)
    brand_channels = _get_branding_channels(sb)

    if not brand_channels:
        st.warning(
            "⚠️ No branding channels configured. "
            "Add branding channels in **⚙️ Settings → Branding Channels** first."
        )

    # ── Record new spend ─────────────────────────────────────
    st.markdown("#### ➕ Record Brand Spend")
    with st.form("brand_spend_form"):
        bc1, bc2, bc3 = st.columns(3)
        with bc1:
            sel_year  = st.selectbox("Year", list(range(2023, 2030)),
                                      index=list(range(2023, 2030)).index(
                                          pd.Timestamp.now().year), key="bs_year")
            sel_month = st.selectbox("Month", list(range(1, 13)),
                                      index=pd.Timestamp.now().month - 1,
                                      format_func=lambda m: pd.Timestamp(2024, m, 1).strftime("%B"),
                                      key="bs_month")
        with bc2:
            ch_options = brand_channels if brand_channels else ["(no channels yet)"]
            sel_ch     = st.selectbox("Branding Channel", ch_options, key="bs_ch")
            amount     = st.number_input("Monthly Spend (₹)", min_value=0.0,
                                          step=1000.0, value=0.0, key="bs_amount")
        with bc3:
            prod_options = ["(All Products — proportional split)"] + products
            sel_prod     = st.selectbox(
                "Product Attribution (optional)",
                prod_options, key="bs_prod",
                help="Leave as 'All Products' if this brand spend is not attributable "
                     "to any single product. The spend will be split proportionally "
                     "across all products based on their sales share."
            )
            st.markdown("")
            st.markdown("")
            submitted = st.form_submit_button("💾 Save Brand Spend", type="primary",
                                               use_container_width=True)

        if submitted:
            if not brand_channels:
                st.error("Add branding channels in Settings first.")
            elif amount <= 0:
                st.error("Amount must be greater than 0.")
            else:
                prod_val = None if sel_prod.startswith("(All") else sel_prod
                if _save_brand_spend(sb, sel_year, sel_month, sel_ch, prod_val, amount):
                    st.success(
                        f"✅ Saved ₹{amount:,.0f} brand spend for **{sel_ch}** "
                        f"({pd.Timestamp(sel_year, sel_month, 1).strftime('%B %Y')})"
                        + (f" → {prod_val}" if prod_val else " → All Products (proportional)")
                    )
                    st.rerun()

    st.divider()

    # ── Existing records ─────────────────────────────────────
    st.markdown("#### 📋 Recorded Brand Spends")
    brand_df = _get_brand_spends(sb)

    if brand_df.empty:
        st.info("No brand spend records yet.")
        return

    # Format for display
    disp = brand_df.copy()
    disp["Month-Year"] = disp.apply(
        lambda r: pd.Timestamp(int(r["year"]), int(r["month"]), 1).strftime("%B %Y"), axis=1
    )
    disp["Product"]    = disp["product"].fillna("All Products (proportional)")
    disp["Daily Rate"] = (disp["amount"] / disp.apply(
        lambda r: __import__("calendar").monthrange(int(r["year"]), int(r["month"]))[1], axis=1
    )).round(0)

    disp_show = disp[["Month-Year","channel","Product","amount","Daily Rate"]].rename(columns={
        "channel": "Branding Channel", "amount": "Monthly Spend (₹)",
        "Daily Rate": "Daily Rate (₹)",
    })

    # Summary KPIs
    sk1, sk2, sk3 = st.columns(3)
    sk1.metric("Total Records", len(brand_df))
    sk2.metric("Total Brand Spend", f"₹{brand_df['amount'].sum():,.0f}")
    sk3.metric("Channels", brand_df["channel"].nunique())

    st.dataframe(
        disp_show.style.format({"Monthly Spend (₹)": "₹{:,.0f}", "Daily Rate (₹)": "₹{:,.0f}"}),
        hide_index=True, use_container_width=True,
    )

    # ── Delete a record ───────────────────────────────────────
    st.divider()
    st.markdown("#### 🗑️ Delete a Record")
    st.caption("Select a record to permanently remove it.")

    if "id" in brand_df.columns:
        del_options = {
            f"ID {row['id']} — {pd.Timestamp(int(row['year']), int(row['month']), 1).strftime('%B %Y')} "
            f"| {row['channel']} | ₹{row['amount']:,.0f}": row["id"]
            for _, row in brand_df.iterrows()
        }
        del_label = st.selectbox("Select record to delete", ["— select —"] + list(del_options.keys()),
                                  key="bs_del_select")
        if st.button("🗑️ Delete Selected", type="primary", key="bs_del_btn"):
            if del_label != "— select —":
                rec_id = del_options[del_label]
                if _delete_brand_spend(sb, rec_id):
                    st.success("Record deleted.")
                    st.rerun()
            else:
                st.error("Please select a record to delete.")


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

    # Load channel map, product map, sales data, and brand spends
    channel_map = _get_channel_map(sb)
    product_map = _get_product_map(sb)
    brand_df    = _get_brand_spends(sb)

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

    # ── Brand spend for the selected date range ──────────────────────────────
    # Expand monthly brand spends into daily rows, then sum for the window
    bs_daily = pd.DataFrame(columns=["date","channel","product","brand_spend_daily"])
    if not brand_df.empty and len(dr) == 2:
        bs_daily = _expand_brand_spends_daily(brand_df, dr[0], dr[1], product_map)
    total_brand_spend = bs_daily["brand_spend_daily"].sum() if not bs_daily.empty else 0.0

    # Grand KPIs
    total_perf_spend = mkt_f["spend"].sum()
    total_spend      = total_perf_spend + total_brand_spend  # combined for TACOS
    total_ad_rev     = mkt_f["sales"].sum()
    acos_overall     = (total_perf_spend / total_ad_rev * 100) if total_ad_rev > 0 else 0
    roas_overall     = (total_ad_rev / total_perf_spend) if total_perf_spend > 0 else 0

    if has_sales:
        # Channel TACOS: spend vs GMV only on channels where we advertise
        ch_gmv        = sales_f["revenue"].sum()
        ch_tacos_perf  = (total_perf_spend / ch_gmv * 100) if ch_gmv > 0 else 0
        ch_tacos_total = (total_spend / ch_gmv * 100) if ch_gmv > 0 else 0
        # Company TACOS: spend vs ALL company GMV (every channel, organic included)
        co_gmv        = sales_all["revenue"].sum()
        co_tacos_perf  = (total_perf_spend / co_gmv * 100) if co_gmv > 0 else 0
        co_tacos_total = (total_spend / co_gmv * 100) if co_gmv > 0 else 0
        organic_rev    = max(co_gmv - total_ad_rev, 0)
        organic_pct    = (organic_rev / co_gmv * 100) if co_gmv > 0 else 0

        st.markdown("##### 📊 Ad Spend Breakdown")
        ks1, ks2, ks3, ks4 = st.columns(4)
        ks1.metric("Perf Ad Spend",   f"₹{total_perf_spend:,.0f}",
                   help="Performance marketing spend — campaign-level, daily attribution")
        ks2.metric("Brand Ad Spend",  f"₹{total_brand_spend:,.0f}",
                   help="Brand/awareness spend — monthly, distributed equally across days")
        ks3.metric("Total Ad Spend",  f"₹{total_spend:,.0f}",
                   help="Perf Spend + Brand Spend combined")
        ks4.metric("Ad Revenue",      f"₹{total_ad_rev:,.0f}",
                   help="Ad-attributed revenue from performance campaigns only")

        st.markdown("##### 📊 Efficiency Metrics")
        ke1, ke2, ke3, ke4 = st.columns(4)
        ke1.metric("ACOS",            f"{acos_overall:.1f}%",
                   help="Perf Ad Spend ÷ Ad-Attributed Revenue × 100")
        ke2.metric("ROAS",            f"{roas_overall:.2f}x",
                   help="Ad Revenue ÷ Perf Ad Spend")
        ke3.metric("Perf-only TACOS", f"{co_tacos_perf:.1f}%",
                   help="Perf Ad Spend ÷ Total Company GMV")
        ke4.metric("Total TACOS",     f"{co_tacos_total:.1f}%",
                   help="(Perf + Brand Spend) ÷ Total Company GMV — true total marketing cost")

        st.markdown("##### 🏢 TACOS View")
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("Channel GMV",      f"₹{ch_gmv:,.0f}",
                  help="Total GMV on channels where marketing spend exists")
        t2.metric("Channel TACOS",    f"{ch_tacos_total:.1f}%",
                  help="(Perf + Brand Spend) ÷ GMV on advertised channels only")
        t3.metric("Company GMV",      f"₹{co_gmv:,.0f}",
                  help="Total GMV across ALL channels (full business view)")
        t4.metric("Company TACOS",    f"{co_tacos_total:.1f}%",
                  help="(Perf + Brand Spend) ÷ Total Company GMV")

        st.caption(
            f"🌱 Organic Revenue: ₹{organic_rev:,.0f} ({organic_pct:.1f}% of company GMV) "
            f"— revenue not attributed to ad spend"
        )
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Perf Ad Spend",  f"₹{total_perf_spend:,.0f}")
        k2.metric("Brand Ad Spend", f"₹{total_brand_spend:,.0f}")
        k3.metric("ACOS",           f"{acos_overall:.1f}%")
        k4.metric("ROAS",           f"{roas_overall:.2f}x")

    st.divider()

    # Weekly trend — add brand spend weekly rollup
    st.markdown("#### 📈 Weekly Spend & TACOS Trend")
    mkt_f["week"] = mkt_f["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
    mkt_weekly = mkt_f.groupby("week").agg(spend=("spend","sum"), ad_rev=("sales","sum")).reset_index()
    mkt_weekly["ACOS"] = pd.to_numeric(mkt_weekly["spend"] / mkt_weekly["ad_rev"].where(mkt_weekly["ad_rev"] > 0) * 100, errors="coerce").round(1)

    # Brand spend weekly aggregation
    if not bs_daily.empty:
        bs_daily["week"] = bs_daily["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
        brand_weekly     = bs_daily.groupby("week")["brand_spend_daily"].sum().reset_index()                                   .rename(columns={"brand_spend_daily":"brand_spend"})
        mkt_weekly = mkt_weekly.merge(brand_weekly, on="week", how="left").fillna(0)
    else:
        mkt_weekly["brand_spend"] = 0.0
    mkt_weekly["total_spend"] = mkt_weekly["spend"] + mkt_weekly["brand_spend"]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Bar(
        x=mkt_weekly["week"], y=mkt_weekly["spend"],
        name="Perf Ad Spend (₹)", marker_color="#636EFA", opacity=0.85,
    ))
    fig_trend.add_trace(go.Bar(
        x=mkt_weekly["week"], y=mkt_weekly["brand_spend"],
        name="Brand Ad Spend (₹)", marker_color="#f59e0b", opacity=0.85,
    ))

    if has_sales:
        # Weekly trend uses company-wide GMV (sales_all) for true TACOS picture
        sales_all["week"]  = sales_all["date"].dt.to_period("W").apply(lambda p: str(p.start_time.date()))
        sales_weekly       = sales_all.groupby("week")["revenue"].sum().reset_index()
        combined           = mkt_weekly.merge(sales_weekly, on="week", how="left").fillna(0)
        # Perf-only TACOS and Total TACOS (perf + brand)
        combined["TACOS_perf"]  = pd.to_numeric(combined["spend"] / combined["revenue"].where(combined["revenue"] > 0) * 100, errors="coerce").round(1)
        combined["TACOS_total"] = pd.to_numeric(combined["total_spend"] / combined["revenue"].where(combined["revenue"] > 0) * 100, errors="coerce").round(1)
        combined["organic"]     = (combined["revenue"] - combined["ad_rev"]).clip(lower=0)
        combined["organic%"]    = pd.to_numeric(combined["organic"] / combined["revenue"].where(combined["revenue"] > 0) * 100, errors="coerce").round(1)
        # Keep backward-compat alias
        combined["TACOS"] = combined["TACOS_total"]

        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["ACOS"],
            name="ACOS %", yaxis="y2", mode="lines+markers",
            line=dict(color="orange", width=2.5),
        ))
        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["TACOS_perf"],
            name="TACOS % (Perf only)", yaxis="y2", mode="lines+markers",
            line=dict(color="red", width=2, dash="dot"),
        ))
        fig_trend.add_trace(go.Scatter(
            x=combined["week"], y=combined["TACOS_total"],
            name="TACOS % (Perf+Brand)", yaxis="y2", mode="lines+markers",
            line=dict(color="#7c3aed", width=2.5, dash="solid"),
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
        barmode="stack",
        height=480,
        yaxis=dict(title="Ad Spend (₹)"),
        yaxis2=dict(title="% Metric", overlaying="y", side="right", range=[0, 100]),
        legend=dict(orientation="h", y=1.15),
        hovermode="x unified",
    )
    fig_trend = apply_chart_theme(fig_trend)
    st.plotly_chart(fig_trend, use_container_width=True)
    st.divider()

    # By Channel — join on mkt_channel so "Amazon" picks up both RKW + Seller
    st.markdown("#### 🏢 By Channel")
    ch_mkt = mkt_f.groupby("channel").agg(spend=("spend","sum"), ad_rev=("sales","sum")).reset_index()
    ch_mkt["ACOS"] = pd.to_numeric(ch_mkt["spend"] / ch_mkt["ad_rev"].where(ch_mkt["ad_rev"] > 0) * 100, errors="coerce").round(1)
    ch_mkt["ROAS"] = pd.to_numeric(ch_mkt["ad_rev"] / ch_mkt["spend"].where(ch_mkt["spend"] > 0), errors="coerce").round(2)

    if has_sales:
        # Group sales by mkt_channel — this correctly sums Amazon RKW + Amazon Seller → Amazon
        ch_sales = sales_f.groupby("mkt_channel")["revenue"].sum().reset_index()\
                          .rename(columns={"mkt_channel":"channel","revenue":"total_gmv"})
        ch_comb = ch_mkt.merge(ch_sales, on="channel", how="left").fillna(0)
        ch_comb["TACOS"]    = pd.to_numeric(ch_comb["spend"] / ch_comb["total_gmv"].where(ch_comb["total_gmv"] > 0) * 100, errors="coerce").round(1)
        ch_comb["Organic%"] = pd.to_numeric(((ch_comb["total_gmv"] - ch_comb["ad_rev"]).clip(lower=0)
                               / ch_comb["total_gmv"].where(ch_comb["total_gmv"] > 0) * 100), errors="coerce").round(1)
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
    pr_mkt["ACOS"] = pd.to_numeric(pr_mkt["spend"] / pr_mkt["ad_rev"].where(pr_mkt["ad_rev"] > 0) * 100, errors="coerce").round(1)
    pr_mkt["ROAS"] = pd.to_numeric(pr_mkt["ad_rev"] / pr_mkt["spend"].where(pr_mkt["spend"] > 0), errors="coerce").round(2)

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
        pr_comb["TACOS"]    = pd.to_numeric(pr_comb["spend"] / pr_comb["total_gmv"].where(pr_comb["total_gmv"] > 0) * 100, errors="coerce").round(1)
        pr_comb["Organic%"] = pd.to_numeric(((pr_comb["total_gmv"] - pr_comb["ad_rev"]).clip(lower=0)
                               / pr_comb["total_gmv"].where(pr_comb["total_gmv"] > 0) * 100), errors="coerce").round(1)
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
    fig_org = apply_chart_theme(fig_org)
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
    s1, s2, s3, s4, s5, s6 = st.tabs(["Master Data", "Mapping Manager", "Channel Map", "Product Map", "Branding Channels", "Data Cleanup"])

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
        st.markdown("##### 📺 Branding Channels")
        st.caption(
            "Define channels where brand/awareness ads are placed. "
            "These are used in the **Brand Spends** tab to record monthly investments."
        )
        existing_bc = _get_branding_channels(sb)
        if existing_bc:
            st.markdown("**Configured branding channels:**")
            for bc in existing_bc:
                bcc1, bcc2 = st.columns([4, 1])
                bcc1.write(f"• {bc}")
                if bcc2.button("Remove", key=f"bs_rmv_{bc}"):
                    if _delete_branding_channel(sb, bc):
                        st.success(f"Removed: {bc}")
                        st.rerun()
        else:
            st.info("No branding channels configured yet.")

        st.divider()
        new_bc = st.text_input("New Branding Channel Name", key="bs_new_ch",
                               help="e.g. Instagram, YouTube, LinkedIn, Offline Events")
        if st.button("Add Branding Channel", key="bs_add_ch") and new_bc.strip():
            if _add_branding_channel(sb, new_bc.strip()):
                st.success(f"Added: {new_bc.strip()}")
                st.rerun()

    with s6:
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
            "🎯 Budget Optimizer",
            "📐 ACOS & TACOS",
            "🏷️ Brand Spends",
            "📥 Upload",
            "📚 History",
            "⚙️ Settings",
        ])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_deep_dive(sb)
        with sub_tabs[2]: _render_forecast(sb)
        with sub_tabs[3]: _render_acos_tacos(sb)
        with sub_tabs[4]: _render_brand_spends(sb)
        with sub_tabs[5]: _render_upload(sb)
        with sub_tabs[6]: _render_history(sb)
        with sub_tabs[7]: _render_settings(sb)
    else:
        sub_tabs = st.tabs([
            "📊 Dashboard",
            "🔬 Deep Dive",
            "🎯 Budget Optimizer",
            "📐 ACOS & TACOS",
            "📚 History",
        ])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_deep_dive(sb)
        with sub_tabs[2]: _render_forecast(sb)
        with sub_tabs[3]: _render_acos_tacos(sb)
        with sub_tabs[4]: _render_history(sb)
