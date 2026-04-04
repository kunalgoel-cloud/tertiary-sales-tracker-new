"""
marketing_module.py
───────────────────
Self-contained performance-marketing logic for Mamanourish dashboard.
Import render_marketing_tab(role) and call it inside the
📣 Performance Marketing tab of the main app.

Uses its OWN Supabase connection via:
  MARKETING_SUPABASE_URL  — st.secrets key
  MARKETING_SUPABASE_KEY  — st.secrets key

This keeps the marketing DB fully separate from the sales DB.
Edit this file independently; the main app never needs to know the internals.
"""

import io
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime
from supabase import create_client, Client


# ─────────────────────────────────────────────────────────────
# OWN SUPABASE CLIENT  (separate from main app sales DB)
# ─────────────────────────────────────────────────────────────

@st.cache_resource
def _get_marketing_supabase() -> Client:
    """
    Returns a cached Supabase client pointed at the MARKETING database.
    Secrets required in .streamlit/secrets.toml:
        MARKETING_SUPABASE_URL = "https://xxxx.supabase.co"
        MARKETING_SUPABASE_KEY = "your-anon-or-service-key"
    """
    try:
        url = st.secrets["MARKETING_SUPABASE_URL"]
        key = st.secrets["MARKETING_SUPABASE_KEY"]
        return create_client(url, key)
    except KeyError:
        st.error(
            "⚠️ Marketing DB not configured. "
            "Add MARKETING_SUPABASE_URL and MARKETING_SUPABASE_KEY to Streamlit Secrets."
        )
        st.stop()


# ─────────────────────────────────────────────────────────────
# DATABASE HELPERS
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
    """Fetch all performance records — always fresh (no cache at module level)."""
    try:
        r = sb.table("performance").select("*").order("date", desc=True).execute()
        df = pd.DataFrame(r.data)
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
        sb.table("performance").upsert(
            record, on_conflict="date,channel,campaign,product"
        ).execute()
        return True
    except Exception:
        # Fallback: explicit update or insert
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
# SUB-VIEWS  (each renders into whatever container is active)
# ─────────────────────────────────────────────────────────────

def _render_dashboard(sb):
    st.subheader("📊 Performance Dashboard")

    df_p = _get_performance(sb)
    if df_p.empty:
        st.info("No marketing data yet. Upload data in the Upload tab.")
        return

    df_p = df_p.drop(columns=["id", "created_at"], errors="ignore")
    df_p["date"] = pd.to_datetime(df_p["date"])

    # ── Filters ─────────────────────────────────────────────
    min_date = df_p["date"].min().date()
    max_date = df_p["date"].max().date()

    fc1, fc2, fc3 = st.columns([2, 2, 1])
    with fc1:
        dr = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date, max_value=max_date,
            key="mkt_daterange",
        )
    with fc2:
        all_channels = sorted(df_p["channel"].unique())
        ch_f = st.multiselect("Channels", all_channels, default=all_channels, key="mkt_chan")
    with fc3:
        all_products = sorted(df_p["product"].unique())
        pr_f = st.multiselect("Products", all_products, default=all_products, key="mkt_prod")

    # Apply filters
    f_df = df_p.copy()
    if len(dr) == 2:
        f_df = f_df[(f_df["date"] >= pd.to_datetime(dr[0])) & (f_df["date"] <= pd.to_datetime(dr[1]))]
    if ch_f:
        f_df = f_df[f_df["channel"].isin(ch_f)]
    if pr_f:
        f_df = f_df[f_df["product"].isin(pr_f)]

    if f_df.empty:
        st.warning("No data matches the selected filters.")
        return

    # ── KPI metrics ─────────────────────────────────────────
    t_spend = f_df["spend"].sum()
    t_sales = f_df["sales"].sum()
    roas    = t_sales / t_spend if t_spend > 0 else 0

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Ad Spend",   f"₹{t_spend:,.0f}")
    k2.metric("Total Ad Revenue", f"₹{t_sales:,.0f}")
    k3.metric("Overall ROAS",     f"{roas:.2f}x")

    st.divider()

    # ── Spend + ROAS chart ───────────────────────────────────
    st.markdown("#### 📈 Spend vs ROAS by Channel")
    ch_trend    = f_df.groupby(["date", "channel"]).agg(spend=("spend","sum"), sales=("sales","sum")).reset_index()
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
        legend=dict(orientation="h", y=1.2),
        hovermode="x unified", height=480,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Summary tables ───────────────────────────────────────
    tc1, tc2 = st.columns(2)
    with tc1:
        st.markdown("**By Channel**")
        ch_sum = (
            f_df.groupby("channel").agg(spend=("spend","sum"), sales=("sales","sum"))
            .assign(ROAS=lambda x: x.sales / x.spend)
            .sort_values("spend", ascending=False)
        )
        st.dataframe(ch_sum.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}", "ROAS":"{:.2f}x"}),
                     use_container_width=True)

    with tc2:
        st.markdown("**By Product**")
        pr_sum = (
            f_df.groupby("product").agg(spend=("spend","sum"), sales=("sales","sum"))
            .assign(ROAS=lambda x: x.sales / x.spend)
            .sort_values("spend", ascending=False)
        )
        st.dataframe(pr_sum.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}", "ROAS":"{:.2f}x"}),
                     use_container_width=True)

    st.divider()
    st.markdown("**Campaign Performance**")
    cp_tab = (
        f_df.groupby(["channel","campaign"]).agg(spend=("spend","sum"), sales=("sales","sum"))
        .assign(ROAS=lambda x: x.sales / x.spend)
        .reset_index()
        .sort_values("spend", ascending=False)
    )
    st.dataframe(
        cp_tab.style.format({"spend":"₹{:,.2f}", "sales":"₹{:,.2f}", "ROAS":"{:.2f}x"}),
        hide_index=True, use_container_width=True, height=300,
    )

    st.divider()
    st.markdown("#### 📅 Date-wise Detail")
    detail = f_df[["date","channel","product","campaign","spend","sales"]].copy()
    detail["ROAS"]  = detail["sales"] / detail["spend"]
    detail["date"]  = detail["date"].dt.strftime("%Y-%m-%d")
    detail = detail.sort_values("date", ascending=False)
    detail.columns = ["Date","Channel","Product","Campaign","Spend (₹)","Revenue (₹)","ROAS"]
    st.dataframe(
        detail.style.format({"Spend (₹)":"₹{:,.2f}", "Revenue (₹)":"₹{:,.2f}", "ROAS":"{:.2f}x"}),
        hide_index=True, use_container_width=True, height=380,
    )
    st.caption(f"Total Records: {len(detail):,}")

    # ── Downloads ────────────────────────────────────────────
    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button("📥 Download CSV", detail.to_csv(index=False),
                           file_name=f"mkt_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)
    with dl2:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            detail.to_excel(w, index=False, sheet_name="Performance")
            pd.DataFrame({
                "Metric": ["Spend","Revenue","ROAS","Records"],
                "Value":  [f"₹{t_spend:,.2f}", f"₹{t_sales:,.2f}", f"{roas:.2f}x", len(detail)],
            }).to_excel(w, index=False, sheet_name="Summary")
        st.download_button("📥 Download Excel", buf.getvalue(),
                           file_name=f"mkt_{datetime.now().strftime('%Y%m%d')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)
    with dl3:
        summary_csv = pd.concat([
            ch_sum.reset_index().assign(Group="Channel"),
            pr_sum.reset_index().assign(Group="Product"),
        ])
        st.download_button("📥 Download Summary", summary_csv.to_csv(index=False),
                           file_name=f"mkt_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv", use_container_width=True)


def _render_upload(sb):
    st.subheader("📥 Upload Marketing Reports")

    channels = _get_channels(sb)
    products = _get_products(sb)

    if not channels or not products:
        st.warning("Configure Channels and Products in the Settings tab first.")
        return

    uc1, uc2 = st.columns(2)
    with uc1:
        manual_date = st.date_input("Date Override (leave blank to use file date)", value=None, key="mkt_manual_date")
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
        .reset_index()
        .sort_values("date", ascending=False)
    )
    history["date"] = history["date"].dt.strftime("%Y-%m-%d")
    st.dataframe(
        history.style.format({"Spend":"₹{:,.2f}", "Sales":"₹{:,.2f}"}),
        hide_index=True, use_container_width=True,
    )


def _render_settings(sb):
    st.subheader("⚙️ Marketing Settings")
    s1, s2, s3 = st.tabs(["Master Data", "Mapping Manager", "Data Cleanup"])

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
        st.markdown("##### 🗑️ Delete Performance Records")
        st.warning("This permanently removes data. Use with caution.")
        dc1, dc2 = st.columns(2)
        with dc1:
            channels   = _get_channels(sb)
            target_ch  = st.selectbox("Channel", ["Select…"] + channels, key="mkt_del_ch")
        with dc2:
            target_dt  = st.date_input("Date", value=None, key="mkt_del_date")
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
# PUBLIC ENTRY POINT — called by the main app
# ─────────────────────────────────────────────────────────────

def render_marketing_tab(role: str):
    """
    Renders the full Performance Marketing section inside the calling tab.
    Internally creates/reuses its own Supabase client pointed at the
    marketing DB (MARKETING_SUPABASE_URL / MARKETING_SUPABASE_KEY in secrets).

    Parameters
    ----------
    role : str
        "admin" or "viewer" — controls which sub-tabs are shown.
    """
    sb = _get_marketing_supabase()

    if role == "admin":
        sub_tabs = st.tabs(["📊 Dashboard", "📥 Upload", "📚 History", "⚙️ Settings"])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_upload(sb)
        with sub_tabs[2]: _render_history(sb)
        with sub_tabs[3]: _render_settings(sb)
    else:
        sub_tabs = st.tabs(["📊 Dashboard", "📚 History"])
        with sub_tabs[0]: _render_dashboard(sb)
        with sub_tabs[1]: _render_history(sb)
