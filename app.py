import streamlit as st
import pandas as pd
from supabase import create_client, Client
import re
import plotly.express as px
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# 1. CONFIG & DB CONNECTION
# ─────────────────────────────────────────────
st.set_page_config(page_title="Mamanourish Executive Tracker", layout="wide")

@st.cache_resource
def get_supabase() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

try:
    supabase = get_supabase()
except Exception as e:
    st.error("Missing Supabase Secrets! Add SUPABASE_URL and SUPABASE_KEY to Streamlit Secrets.")
    st.stop()

# ─────────────────────────────────────────────
# 2. HELPERS
# ─────────────────────────────────────────────
def clean_num(val) -> float:
    """Convert messy numeric strings (commas, parentheses) to float."""
    if pd.isna(val) or str(val).strip() == "":
        return 0.0
    s = str(val).strip().replace(",", "")
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    res = re.sub(r"[^-0-9.]", "", s)
    try:
        return round(float(res), 2) if res else 0.0
    except ValueError:
        return 0.0

def sanitize(text: str) -> str:
    """Strip dangerous characters from user-supplied names."""
    return re.sub(r"[<>\"'%;()&+]", "", str(text)).strip()[:200]

def get_table(table: str, default_cols: list) -> pd.DataFrame:
    """Fetch a Supabase table; return empty DataFrame on failure."""
    try:
        res = supabase.table(table).select("*").execute()
        df = pd.DataFrame(res.data)
        if df.empty:
            return pd.DataFrame(columns=default_cols)
        # Cast numeric columns that Supabase may return as strings/objects
        for col in ["qty_sold", "revenue"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df
    except Exception as e:
        st.warning(f"Could not load '{table}': {e}")
        return pd.DataFrame(columns=default_cols)

# ─────────────────────────────────────────────
# 3. AUTHENTICATION  (passwords via secrets only)
# ─────────────────────────────────────────────
def check_auth() -> bool:
    """
    Passwords are read from st.secrets to avoid hardcoding credentials.
    Expected secrets keys: ADMIN_PASSWORD, VIEWER_PASSWORD
    """
    if "authenticated" not in st.session_state:
        st.title("🔐 Mamanourish Sales Portal")
        role_choice = st.selectbox("I am a…", ["Select Role", "Admin (Full Access)", "Viewer (View Only)"])
        pw = st.text_input("Enter Password", type="password")

        if st.button("Login"):
            try:
                admin_pw = st.secrets["ADMIN_PASSWORD"]
                viewer_pw = st.secrets["VIEWER_PASSWORD"]
            except KeyError:
                st.error("ADMIN_PASSWORD / VIEWER_PASSWORD not set in Streamlit Secrets.")
                return False

            if role_choice == "Admin (Full Access)" and pw == admin_pw:
                st.session_state["authenticated"] = True
                st.session_state["role"] = "admin"
                st.rerun()
            elif role_choice == "Viewer (View Only)" and pw == viewer_pw:
                st.session_state["authenticated"] = True
                st.session_state["role"] = "viewer"
                st.rerun()
            else:
                st.error("Incorrect password or role.")
        return False
    return True

# ─────────────────────────────────────────────
# 4. MAIN APP
# ─────────────────────────────────────────────
if not check_auth():
    st.stop()

role: str = st.session_state["role"]

history_df   = get_table("sales",           ["id", "date", "channel", "item_name", "qty_sold", "revenue"])
master_skus  = get_table("master_skus",     ["name"])
master_chans = get_table("master_channels", ["name"])
item_map_df  = get_table("item_map",        ["raw_name", "master_name"])

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.header(f"👤 {role.upper()}")

    if role == "admin":
        st.divider()
        st.subheader("🛠 Data Correction")

        with st.expander("Delete Specific Entry"):
            del_date = st.date_input("Select Date to Clear", value=datetime.now().date())
            chan_options = ["Select…"] + master_chans["name"].tolist()
            del_chan = st.selectbox("Select Channel to Clear", chan_options)
            if st.button("🗑️ Delete Selection"):
                if del_chan != "Select…":
                    try:
                        supabase.table("sales").delete()\
                            .eq("date", str(del_date))\
                            .eq("channel", del_chan)\
                            .execute()
                        st.success(f"Deleted {del_chan} data for {del_date}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Delete failed: {e}")
                else:
                    st.error("Please select a channel.")

        st.divider()
        if st.checkbox("Unlock Global Danger Zone"):
            if st.button("💥 Flush Entire History"):
                try:
                    supabase.table("sales").delete().neq("id", -1).execute()
                    st.success("All history flushed.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Flush failed: {e}")
            if st.button("🔄 Reset All Mappings"):
                try:
                    supabase.table("item_map").delete().neq("raw_name", "dummy").execute()
                    st.success("Mappings reset.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Reset failed: {e}")

    st.divider()
    if st.button("Logout"):
        del st.session_state["authenticated"]
        del st.session_state["role"]
        st.rerun()

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
if role == "admin":
    tabs = st.tabs(["📊 Trend Analytics", "📤 Smart Upload", "🛠 Configuration"])
else:
    tabs = st.tabs(["📊 Analytics"])

# ══════════════════════════════════════════════
# TAB 1 – ANALYTICS
# ══════════════════════════════════════════════
with tabs[0]:
    if history_df.empty:
        st.info("No data found. Admin must upload sales data first.")
    else:
        history_df["date_dt"] = pd.to_datetime(history_df["date"], errors="coerce")
        history_df = history_df.dropna(subset=["date_dt"])

        v1, v2 = st.columns([2, 1])
        with v1:
            view_metric = st.radio(
                "Display Dashboard By:",
                ["Revenue (₹)", "Quantity (Units)"],
                horizontal=True,
            )
        with v2:
            show_labels = st.checkbox("Show Data Labels", value=True)

        target_col      = "revenue" if "Revenue" in view_metric else "qty_sold"
        metric_label    = "Revenue"  if "Revenue" in view_metric else "Qty"
        currency_prefix = "₹"        if "Revenue" in view_metric else ""

        st.subheader("Time Filters")
        today = datetime.now().date()

        time_preset = st.radio(
            "Period:",
            ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"],
            horizontal=True,
            index=3,
        )

        if time_preset == "Last 7 Days":
            start_date, end_date = today - timedelta(days=6), today
        elif time_preset == "Last 30 Days":
            start_date, end_date = today - timedelta(days=29), today
        elif time_preset == "Month to Date":
            start_date, end_date = today.replace(day=1), today
        elif time_preset == "All Time":
            start_date = history_df["date_dt"].min().date()
            end_date   = history_df["date_dt"].max().date()
        else:
            dr = st.date_input("Range", value=(history_df["date_dt"].min().date(), today))
            start_date, end_date = (dr[0], dr[1]) if len(dr) == 2 else (today, today)

        mask     = (history_df["date_dt"].dt.date >= start_date) & (history_df["date_dt"].dt.date <= end_date)
        range_df = history_df[mask].copy()

        f1, f2 = st.columns(2)
        avail_chans = sorted(range_df["channel"].unique())
        with f1:
            sel_chan = st.multiselect("Filter Channels", avail_chans, default=avail_chans)

        chan_mask   = range_df["channel"].isin(sel_chan)
        avail_items = sorted(range_df[chan_mask]["item_name"].unique())
        with f2:
            sel_item = st.multiselect("Filter Products", avail_items)

        final_mask = chan_mask
        if sel_item:
            final_mask &= range_df["item_name"].isin(sel_item)
        filtered = range_df[final_mask].copy()

        total_val     = filtered[target_col].sum()
        intended_days = max((end_date - start_date).days + 1, 1)
        avg_drr       = total_val / intended_days

        m1, m2 = st.columns(2)
        m1.metric(f"Total {metric_label}", f"{currency_prefix}{total_val:,.2f}")
        m2.metric(
            "Daily Run Rate (DRR)",
            f"{currency_prefix}{avg_drr:,.2f}",
            help=f"Total ÷ {intended_days} days in selected period.",
        )

        if not filtered.empty:
            color_theme = "item_name" if sel_item else "channel"
            plot_df = (
                filtered
                .groupby(["date", color_theme])[target_col]
                .sum()
                .reset_index()
                .sort_values("date")
            )
            fig = px.bar(
                plot_df, x="date", y=target_col,
                color=color_theme, barmode="stack", height=500,
            )
            fig.add_hline(
                y=avg_drr, line_dash="dash", line_color="red",
                annotation_text="Avg DRR",
            )
            if show_labels:
                fig.update_traces(texttemplate="%{y:.2s}", textposition="inside")
                totals = plot_df.groupby("date")[target_col].sum().reset_index()
                fig.add_scatter(
                    x=totals["date"],
                    y=totals[target_col],
                    text=totals[target_col].apply(lambda x: f"{x:,.0f}"),
                    mode="text",
                    textposition="top center",
                    showlegend=False,
                )
            st.plotly_chart(fig, use_container_width=True)
            display_cols = [c for c in filtered.columns if c not in ("date_dt", "id")]
            st.dataframe(filtered[display_cols], hide_index=True)

# ══════════════════════════════════════════════
# TAB 2 – SMART UPLOAD  (admin only)
# ══════════════════════════════════════════════
if role == "admin":
    with tabs[1]:
        st.subheader("Upload Sales Report")

        channels = master_chans["name"].tolist() if not master_chans.empty else []
        if not channels:
            st.warning("No channels configured. Add channels in the Configuration tab first.")
            st.stop()

        selected_channel = st.selectbox("Select Channel", channels)
        up_file = st.file_uploader("Upload File", type=["csv", "xlsx"])

        if up_file and selected_channel:
            try:
                raw_df = (
                    pd.read_csv(up_file)
                    if up_file.name.lower().endswith(".csv")
                    else pd.read_excel(up_file)
                )
            except Exception as e:
                st.error(f"Could not read file: {e}")
                st.stop()

            st.write(f"**Preview** — {len(raw_df)} rows × {len(raw_df.columns)} cols")
            st.dataframe(raw_df.head(5), hide_index=True)

            cols = ["None"] + raw_df.columns.tolist()
            c1, c2, c3 = st.columns(3)

            with c1:
                p_col = st.selectbox("Product Column *", cols, key="p_col")
                v_col = st.selectbox("Variant Column (optional)", cols, key="v_col")
            with c2:
                q_col = st.selectbox("Qty Column *",     cols, key="q_col")
                r_col = st.selectbox("Revenue Column *", cols, key="r_col")
            with c3:
                d_col     = st.selectbox("Date Column (or use manual date below)", cols, key="d_col")
                fixed_date = st.date_input("Manual Date (used if no date column)", key="fixed_date")

            # Validate mandatory column picks
            missing = [name for name, col in [("Product", p_col), ("Qty", q_col), ("Revenue", r_col)] if col == "None"]
            if missing:
                st.info(f"Please select columns for: {', '.join(missing)}")
                st.stop()

            # ── Build composite key safely ──────────────────────────────────
            work_df = raw_df.copy()
            work_df["__prod__"] = work_df[p_col].astype(str).str.strip()

            if v_col != "None":
                work_df["__var__"] = work_df[v_col].astype(str).str.strip()
                work_df["m_key"]   = work_df["__prod__"] + " | " + work_df["__var__"]
            else:
                work_df["m_key"] = work_df["__prod__"]

            # ── Filter out totals rows ──────────────────────────────────────
            SKIP_LABELS = {"total", "grand total", "subtotal", "nan", ""}
            valid_mask = ~work_df["__prod__"].str.lower().isin(SKIP_LABELS)
            work_df    = work_df[valid_mask].copy()

            if work_df.empty:
                st.error("No valid data rows found after filtering. Check column mapping.")
                st.stop()

            # ── SKU Mapping UI ──────────────────────────────────────────────
            masters = master_skus["name"].tolist() if not master_skus.empty else []
            if not masters:
                st.warning("No master SKUs configured. Add SKUs in the Configuration tab first.")
                st.stop()

            st.markdown("#### 🗺 Map Raw Product Names → Master SKUs")
            unique_keys = sorted(work_df["m_key"].unique())
            sku_map: dict[str, str] = {}

            # Pre-populate from saved mappings
            saved_map: dict[str, str] = {}
            if not item_map_df.empty:
                saved_map = dict(zip(item_map_df["raw_name"], item_map_df["master_name"]))

            for k in unique_keys:
                saved = saved_map.get(k, "")
                default_idx = masters.index(saved) if saved in masters else 0
                sku_map[k] = st.selectbox(
                    f"Map: `{k}`",
                    masters,
                    index=default_idx,
                    key=f"sku_{k}",
                )

            # ── Date Preview ────────────────────────────────────────────────
            if d_col != "None":
                def preview_date(val):
                    s = str(val).strip()
                    if " - " in s:
                        s = s.split(" - ")[0].strip()
                    try:
                        return pd.to_datetime(s).strftime("%Y-%m-%d")
                    except Exception:
                        return f"⚠️ unparseable: {val}"

                sample_dates = work_df[d_col].dropna().unique()[:5]
                parsed_preview = [preview_date(d) for d in sample_dates]
                st.info(f"📅 **Date column preview** — raw: `{sample_dates[0]}` → parsed as: `{parsed_preview[0]}`")
                if any("unparseable" in str(p) for p in parsed_preview):
                    st.warning("Some dates couldn't be parsed — those rows will use the Manual Date instead.")

            # ── Sync Button ─────────────────────────────────────────────────
            if st.button("🚀 Sync to Cloud"):
                errors: list[str] = []

                with st.spinner("Saving mappings…"):
                    for raw_name, master_name in sku_map.items():
                        try:
                            supabase.table("item_map").upsert(
                                {"raw_name": raw_name, "master_name": master_name},
                                on_conflict="raw_name",
                            ).execute()
                        except Exception as e:
                            errors.append(f"Mapping save failed for '{raw_name}': {e}")

                with st.spinner("Processing rows…"):
                    raw_rows = []
                    for _, r in work_df.iterrows():
                        # Resolve date — handles plain dates AND range strings like "20260402 - 20260402"
                        if d_col != "None":
                            raw_date_val = str(r[d_col]).strip()
                            # If it looks like a range (contains " - "), extract the start date
                            if " - " in raw_date_val:
                                raw_date_val = raw_date_val.split(" - ")[0].strip()
                            try:
                                dt_str = pd.to_datetime(raw_date_val).strftime("%Y-%m-%d")
                            except Exception:
                                dt_str = str(fixed_date)
                        else:
                            dt_str = str(fixed_date)

                        raw_rows.append({
                            "date":      dt_str,
                            "channel":   selected_channel,
                            "item_name": sku_map[r["m_key"]],
                            "qty_sold":  clean_num(r[q_col]),
                            "revenue":   clean_num(r[r_col]),
                        })

                    if not raw_rows:
                        st.error("No rows to upload after processing.")
                        st.stop()

                    final_df = (
                        pd.DataFrame(raw_rows)
                        .groupby(["date", "channel", "item_name"])
                        .agg({"qty_sold": "sum", "revenue": "sum"})
                        .reset_index()
                    )

                with st.spinner(f"Uploading {len(final_df)} records to Supabase…"):
                    try:
                        # Insert in chunks to avoid request-size limits
                        CHUNK = 500
                        records = final_df.to_dict(orient="records")
                        for i in range(0, len(records), CHUNK):
                            chunk = records[i : i + CHUNK]
                            res = supabase.table("sales").upsert(
                                chunk,
                                on_conflict="date,channel,item_name",
                            ).execute()
                            # Supabase SDK raises on HTTP errors; explicit check:
                            if hasattr(res, "error") and res.error:
                                errors.append(f"Upsert chunk {i//CHUNK+1} error: {res.error}")
                    except Exception as e:
                        errors.append(f"Upload failed: {e}")

                if errors:
                    for err in errors:
                        st.error(err)
                    st.warning(
                        "⚠️ Some records may not have synced. "
                        "Check that your `sales` table has a UNIQUE constraint on "
                        "(date, channel, item_name) in Supabase."
                    )
                else:
                    st.success(f"✅ Synced {len(final_df)} unique records for '{selected_channel}'!")
                    st.info("Refresh the Analytics tab to see the updated data.")
                    # Clear upload-related cache so analytics re-fetches fresh data
                    st.cache_resource.clear()

    # ══════════════════════════════════════════
    # TAB 3 – CONFIGURATION  (admin only)
    # ══════════════════════════════════════════
    with tabs[2]:
        st.subheader("⚙️ System Configuration")
        sc1, sc2 = st.columns(2)

        with sc1:
            st.markdown("#### 📦 Master SKUs")
            n_sku = st.text_input("New SKU Name")
            if st.button("Add SKU") and n_sku.strip():
                safe_sku = sanitize(n_sku)
                if safe_sku:
                    try:
                        supabase.table("master_skus").insert({"name": safe_sku}).execute()
                        st.success(f"Added SKU: {safe_sku}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add SKU: {e}")
            if not master_skus.empty:
                st.dataframe(master_skus, hide_index=True)

        with sc2:
            st.markdown("#### 🏢 Sales Channels")
            n_ch = st.text_input("New Channel Name")
            if st.button("Add Channel") and n_ch.strip():
                safe_ch = sanitize(n_ch)
                if safe_ch:
                    try:
                        supabase.table("master_channels").insert({"name": safe_ch}).execute()
                        st.success(f"Added channel: {safe_ch}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add channel: {e}")
            if not master_chans.empty:
                st.dataframe(master_chans, hide_index=True)

        st.divider()
        st.markdown("#### 🗺 Current Item Mappings")
        if not item_map_df.empty:
            st.dataframe(item_map_df, hide_index=True)
        else:
            st.info("No mappings saved yet.")
