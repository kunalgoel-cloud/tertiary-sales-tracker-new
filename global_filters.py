"""
global_filters.py
─────────────────
Centralized Global Filter System for Mamanourish Executive Tracker.

HOW GLOBAL STATE IS MANAGED:
─────────────────────────────
All filter values are stored in Streamlit's st.session_state under the
namespace prefix "gf_" (global filter). This means:

  - st.session_state["gf_time_preset"]   → Period preset (e.g., "Last 30 Days")
  - st.session_state["gf_start_date"]    → Resolved start date (datetime.date)
  - st.session_state["gf_end_date"]      → Resolved end date (datetime.date)
  - st.session_state["gf_channels"]      → List of selected channels (or None = all)
  - st.session_state["gf_products"]      → List of selected products (or None = all)

Tabs READ from st.session_state["gf_*"] to apply filters, instead of
rendering their own independent filter widgets. This guarantees:
  ✅ Filters persist across tab switches (session_state survives tab changes)
  ✅ Single source of truth for all filter values
  ✅ Tabs respond to filter changes immediately on next render

ARCHITECTURE:
─────────────
  global_filters.py   → renders the Global Filter Bar + manages state
  app.py              → calls init_global_filters() once at startup,
                        render_global_filter_bar() at the top of the page,
                        and get_global_filters() to pass values to tabs
  Each tab module     → calls apply_global_filters(df) or uses
                        get_global_filters() to get the resolved filter values

WHAT IS GLOBAL vs LOCAL:
─────────────────────────
Global (in this bar):
  - Date / period range  (used by Analytics, Deep Dive, SOP, Marketing)
  - Channel filter       (used by Analytics, Deep Dive, SOP)
  - Product filter       (used by Analytics, Deep Dive, SOP)

Local (remains inside each tab):
  - Marketing: campaign filter, ACOS/TACOS toggle    (marketing-specific)
  - Channel Performance: inventory file uploads, sales window N-days (upload-specific)
  - Vending: customer selector, month/year, workbook upload             (vending-specific)
  - Deep Dive: WoW metric, heatmap metric (display toggles, not data filters)
  - SOP: plan month, budget inputs (planning-specific, not historical filter)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

PERIOD_OPTIONS = ["Last 7 Days", "Last 30 Days", "Month to Date", "All Time", "Custom"]
DEFAULT_PERIOD  = "Last 30 Days"

# Session-state keys (all global filter keys share this prefix)
_PFX            = "gf_"
KEY_PRESET      = f"{_PFX}time_preset"
KEY_START       = f"{_PFX}start_date"
KEY_END         = f"{_PFX}end_date"
KEY_CHANNELS    = f"{_PFX}channels"
KEY_PRODUCTS    = f"{_PFX}products"
KEY_INITIALIZED = f"{_PFX}initialized"


# ─────────────────────────────────────────────────────────────
# INITIALIZATION
# ─────────────────────────────────────────────────────────────

def init_global_filters(history_df: pd.DataFrame) -> None:
    """
    Call ONCE at app startup (before tabs are rendered).
    Seeds session_state with default filter values if not already set.
    This ensures first-load defaults are applied and not overwritten on reruns.
    """
    if st.session_state.get(KEY_INITIALIZED):
        # Already initialized — just refresh available channels/products
        # from the current data without resetting user selections.
        return

    today          = datetime.now().date()
    last_data_date = history_df["date_dt"].max().date() if not history_df.empty else today
    effective_end  = min(today, last_data_date)

    # Default period: Last 30 Days
    default_start = effective_end - timedelta(days=29)
    default_end   = effective_end

    st.session_state[KEY_PRESET]      = DEFAULT_PERIOD
    st.session_state[KEY_START]       = default_start
    st.session_state[KEY_END]         = default_end
    st.session_state[KEY_CHANNELS]    = None   # None = "all channels"
    st.session_state[KEY_PRODUCTS]    = None   # None = "all products"
    st.session_state[KEY_INITIALIZED] = True


# ─────────────────────────────────────────────────────────────
# GLOBAL FILTER BAR RENDERER
# ─────────────────────────────────────────────────────────────

def render_global_filter_bar(history_df: pd.DataFrame) -> None:
    """
    Renders the sticky Global Filter Bar at the top of the app.
    Call this AFTER st.tabs() but BEFORE rendering tab content
    (Streamlit evaluates top-level widget calls in order).

    The bar contains:
      Row 1: Period preset radio buttons
      Row 2 (if Custom): date range picker
      Row 3: Channel multiselect | Product multiselect

    All selections are persisted in st.session_state[KEY_*].
    """
    if history_df.empty:
        # No data — nothing to filter, skip bar
        return

    today          = datetime.now().date()
    last_data_date = history_df["date_dt"].max().date()
    effective_end  = min(today, last_data_date)
    data_start     = history_df["date_dt"].min().date()

    avail_channels = sorted(history_df["channel"].dropna().unique().tolist())
    avail_products = sorted(history_df["item_name"].dropna().unique().tolist())

    # ── Visual container ────────────────────────────────────────────────────
    with st.container():
        st.markdown(
            """
            <style>
            /* Make global filter bar visually distinct */
            div[data-testid="stVerticalBlock"] > div:has(> div[data-global-filter-bar]) {
                background: var(--background-color);
                border-bottom: 1px solid rgba(128,128,128,0.2);
                padding-bottom: 0.5rem;
                margin-bottom: 0.75rem;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            "<span style='font-size:0.75rem;font-weight:600;"
            "letter-spacing:0.06em;color:gray;text-transform:uppercase'>"
            "🌐 GLOBAL FILTERS — applied across all tabs</span>",
            unsafe_allow_html=True,
        )

        # ── Row 1: Period preset ─────────────────────────────────────────────
        current_preset = st.session_state.get(KEY_PRESET, DEFAULT_PERIOD)
        preset_idx     = PERIOD_OPTIONS.index(current_preset) if current_preset in PERIOD_OPTIONS else 1

        chosen_preset = st.radio(
            "Period",
            PERIOD_OPTIONS,
            index=preset_idx,
            horizontal=True,
            key=f"{_PFX}preset_radio",
            label_visibility="collapsed",
        )

        # ── Row 2: Custom range picker (shown only for Custom) ───────────────
        if chosen_preset == "Custom":
            saved_start = st.session_state.get(KEY_START, data_start)
            saved_end   = st.session_state.get(KEY_END, effective_end)
            dr = st.date_input(
                "Custom Date Range",
                value=(saved_start, saved_end),
                min_value=data_start,
                max_value=effective_end,
                key=f"{_PFX}custom_dr",
                label_visibility="collapsed",
            )
            resolved_start = dr[0] if len(dr) == 2 else saved_start
            resolved_end   = dr[1] if len(dr) == 2 else saved_end
        else:
            resolved_start, resolved_end = _resolve_preset(
                chosen_preset, data_start, effective_end
            )

        # ── Row 3: Channel + Product multiselects ────────────────────────────
        f_col1, f_col2 = st.columns(2)

        # Channels: saved selection or all channels
        saved_chans = st.session_state.get(KEY_CHANNELS) or avail_channels
        # Guard against stale selections (channels removed from data)
        saved_chans = [c for c in saved_chans if c in avail_channels] or avail_channels

        with f_col1:
            chosen_channels = st.multiselect(
                "Channels",
                avail_channels,
                default=saved_chans,
                key=f"{_PFX}chan_select",
                placeholder="All channels",
            )

        # Products: derive from channels selection for relevance
        # Only show products that appear in the chosen channels
        chan_mask      = history_df["channel"].isin(chosen_channels or avail_channels)
        avail_products = sorted(history_df[chan_mask]["item_name"].dropna().unique().tolist())

        saved_prods = st.session_state.get(KEY_PRODUCTS) or []
        # Keep only products still relevant to current channels
        saved_prods = [p for p in saved_prods if p in avail_products]

        with f_col2:
            chosen_products = st.multiselect(
                "Products",
                avail_products,
                default=saved_prods,
                key=f"{_PFX}prod_select",
                placeholder="All products (no filter)",
            )

        st.divider()

    # ── Persist resolved values to session_state ─────────────────────────────
    # These are the canonical values that every tab reads via get_global_filters()
    st.session_state[KEY_PRESET]   = chosen_preset
    st.session_state[KEY_START]    = resolved_start
    st.session_state[KEY_END]      = resolved_end
    st.session_state[KEY_CHANNELS] = chosen_channels if chosen_channels else None
    st.session_state[KEY_PRODUCTS] = chosen_products if chosen_products else None


# ─────────────────────────────────────────────────────────────
# PUBLIC ACCESSORS — used by tabs to read global filter state
# ─────────────────────────────────────────────────────────────

def get_global_filters() -> dict:
    """
    Returns the current global filter state as a dict:
    {
        "preset":   str,            # e.g., "Last 30 Days"
        "start":    datetime.date,
        "end":      datetime.date,
        "channels": list[str] | None,  # None = no filter (all)
        "products": list[str] | None,  # None = no filter (all)
    }

    Call this inside any tab to read the shared filter state.
    """
    return {
        "preset":   st.session_state.get(KEY_PRESET, DEFAULT_PERIOD),
        "start":    st.session_state.get(KEY_START),
        "end":      st.session_state.get(KEY_END),
        "channels": st.session_state.get(KEY_CHANNELS),
        "products": st.session_state.get(KEY_PRODUCTS),
    }


def apply_global_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function: applies global date + channel + product filters
    to any DataFrame that has 'date_dt', 'channel', and 'item_name' columns.

    Returns the filtered DataFrame. Original DataFrame is not mutated.

    Usage in any tab:
        filtered = apply_global_filters(history_df)
    """
    gf    = get_global_filters()
    start = gf["start"]
    end   = gf["end"]

    if df.empty or start is None or end is None:
        return df

    result = df.copy()

    # Date filter
    if "date_dt" in result.columns:
        result = result[
            (result["date_dt"].dt.date >= start) &
            (result["date_dt"].dt.date <= end)
        ]

    # Channel filter (None = all channels)
    if gf["channels"] and "channel" in result.columns:
        result = result[result["channel"].isin(gf["channels"])]

    # Product filter (None or empty = all products)
    if gf["products"] and "item_name" in result.columns:
        result = result[result["item_name"].isin(gf["products"])]

    return result


def get_date_range() -> tuple:
    """Returns (start_date, end_date) as datetime.date objects."""
    gf = get_global_filters()
    return gf["start"], gf["end"]


def get_selected_channels() -> Optional[list]:
    """Returns selected channels list, or None if all channels are selected."""
    return st.session_state.get(KEY_CHANNELS)


def get_selected_products() -> Optional[list]:
    """Returns selected products list, or None if all products are selected."""
    return st.session_state.get(KEY_PRODUCTS)


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────

def _resolve_preset(preset: str, data_start, effective_end) -> tuple:
    """Convert a period preset string to (start_date, end_date)."""
    if preset == "Last 7 Days":
        return effective_end - timedelta(days=6), effective_end
    elif preset == "Last 30 Days":
        return effective_end - timedelta(days=29), effective_end
    elif preset == "Month to Date":
        return effective_end.replace(day=1), effective_end
    elif preset == "All Time":
        return data_start, effective_end
    else:
        # Fallback to last 30 days
        return effective_end - timedelta(days=29), effective_end
