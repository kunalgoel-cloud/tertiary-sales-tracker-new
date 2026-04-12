"""
performance.py — Mamanourish Performance Optimisation Layer
═══════════════════════════════════════════════════════════════

KEY DESIGN PRINCIPLE (memory-safe):
─────────────────────────────────────
DataFrames are NEVER stored in st.session_state.
Streamlit Cloud has a ~1 GB worker memory limit. Storing multiple copies of
history_df in session_state (one per filtered slice) would triple memory
usage and crash the worker process — which manifests as the endless
"Spinning up manager process" restart loop.

Instead:
  - Filter hashes are stored (tiny strings, ~12 bytes each).
  - The actual filter computation re-runs when needed (fast: it's a Boolean
    mask on an already-in-memory DataFrame, not a DB call).
  - Heavy DB fetches are cached at the @st.cache_data layer in each module.

WHAT THIS MODULE PROVIDES:
────────────────────────────
1. inject_perf_css()       — Top loading bar, skeleton shimmers, upload
                              progress bar styling, chart fade-in animations.
2. should_recompute()      — Hash-gate: returns True only when filter state
                              actually changed. Lets callers skip chart
                              re-computation on display-toggle reruns.
3. apply_filters_fast()    — Optimised Boolean-mask filter (no DataFrame copy
                              stored in session_state).
4. upload_progress_bar()   — Context manager: live % progress bar for chunked
                              Supabase upsert loops.
5. skeleton_*()            — Shimmer placeholder components.
6. lazy_section()          — Dependency-hash gate for expensive render blocks.
"""

import hashlib
import time
import contextlib
from typing import Any

import pandas as pd
import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
# 1. TOP LOADING BAR + PERFORMANCE CSS
# ─────────────────────────────────────────────────────────────────────────────

def inject_perf_css() -> None:
    """
    Injects animated top progress bar, skeleton shimmers, upload progress
    bar styling, and chart fade-in. Call once at app startup after inject_css().
    """
    st.markdown("""
<style>
/* TOP LOADING BAR — plays on every Streamlit rerun automatically */
@keyframes topbar-slide {
  0%   { width: 0%;   opacity: 1; }
  70%  { width: 88%;  opacity: 1; }
  100% { width: 100%; opacity: 0; }
}
body::before {
  content: '';
  position: fixed;
  top: 0; left: 0;
  height: 3px;
  background: linear-gradient(90deg, #C47A2B, #2A8A7E);
  animation: topbar-slide 1.2s cubic-bezier(.4,0,.2,1) forwards;
  z-index: 10000;
  pointer-events: none;
}

/* SKELETON SHIMMER */
@keyframes skeleton-pulse {
  0%   { background-position: -400px 0; }
  100% { background-position:  400px 0; }
}
.mn-skeleton {
  background: linear-gradient(90deg, #F0EDE8 25%, #E8E4DF 50%, #F0EDE8 75%);
  background-size: 400px 100%;
  animation: skeleton-pulse 1.4s ease-in-out infinite;
  border-radius: 8px;
}
.mn-skeleton-metric { height: 88px;  border-radius: 12px; margin-bottom: 1rem; }
.mn-skeleton-chart  { height: 320px; border-radius: 12px; margin-bottom: 1rem; }
.mn-skeleton-row    { height: 18px;  border-radius: 4px;  margin-bottom: 8px;  }

/* CHART FADE-IN */
[data-testid="stPlotlyChart"],
[data-testid="stDataFrame"] {
  animation: content-fade-in 0.25s ease both !important;
}
@keyframes content-fade-in {
  from { opacity: 0; transform: translateY(4px); }
  to   { opacity: 1; transform: translateY(0);   }
}

/* UPLOAD PROGRESS BAR */
.stProgress > div > div > div > div {
  background: linear-gradient(90deg, #C47A2B, #2A8A7E) !important;
  border-radius: 4px !important;
}
.stProgress > div > div {
  background: #F0EDE8 !important;
  border-radius: 4px !important;
  height: 6px !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. SKELETON PLACEHOLDER COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────

def skeleton_metrics(n: int = 4) -> None:
    """Render n shimmer metric placeholders."""
    cols = st.columns(n)
    for col in cols:
        col.markdown('<div class="mn-skeleton mn-skeleton-metric"></div>',
                     unsafe_allow_html=True)


def skeleton_chart(height: int = 320, label: str = "Loading…") -> None:
    """Render a shimmer chart placeholder."""
    st.markdown(
        f'<div class="mn-skeleton mn-skeleton-chart" style="height:{height}px;"'
        f' title="{label}"></div>',
        unsafe_allow_html=True)


def skeleton_table(rows: int = 5) -> None:
    """Render shimmer table row placeholders."""
    for _ in range(rows):
        st.markdown('<div class="mn-skeleton mn-skeleton-row"></div>',
                    unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 3. FILTER HASH GATE  (no DataFrames stored — only 12-byte hash strings)
# ─────────────────────────────────────────────────────────────────────────────

def _filter_hash(start, end, channels, products) -> str:
    """Stable 12-char hex hash of the four filter parameters."""
    key = f"{start}|{end}|{sorted(channels or [])}|{sorted(products or [])}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def should_recompute(cache_key: str, start, end, channels, products) -> bool:
    """
    Returns True only when the filter state changed since the last render.
    Stores only a 12-byte hash string — never a DataFrame.

    Usage:
        if should_recompute("analytics", start, end, channels, products):
            filtered = apply_filters_fast(history_df, start, end, channels, products)
            # use filtered directly — do NOT store it in session_state
    """
    hash_key = f"_fhash_{cache_key}"
    new_hash = _filter_hash(start, end, channels, products)
    changed  = (st.session_state.get(hash_key) != new_hash)
    if changed:
        st.session_state[hash_key] = new_hash
    return changed


def apply_filters_fast(
    df: pd.DataFrame,
    start,
    end,
    channels,
    products,
) -> pd.DataFrame:
    """
    Fast Boolean-mask filter — does NOT copy the DataFrame unnecessarily.
    Returns a view (or lightweight copy) of df — never stores in session_state.

    MEMORY NOTE: pandas Boolean indexing returns a copy only when the mask
    is non-trivial. For a 50k-row DataFrame this is ~4 MB — fine for a
    single render, but NOT something to persist in session_state across reruns.
    """
    mask = pd.Series(True, index=df.index)

    if start is not None and "date_dt" in df.columns:
        mask &= (df["date_dt"].dt.date >= start) & (df["date_dt"].dt.date <= end)
    if channels and "channel" in df.columns:
        mask &= df["channel"].isin(channels)
    if products and "item_name" in df.columns:
        mask &= df["item_name"].isin(products)

    return df[mask]


# Keep get_filtered_df as an alias that matches the old call signature
# but does NOT store results in session_state.
def get_filtered_df(
    cache_key: str,
    df: pd.DataFrame,
    start,
    end,
    channels,
    products,
) -> pd.DataFrame:
    """
    Drop-in replacement for the old session_state-caching version.
    Now just applies the filter directly — no DataFrame stored in session_state.
    The cache_key parameter is kept for API compatibility but is unused.
    """
    return apply_filters_fast(df, start, end, channels, products)


# ─────────────────────────────────────────────────────────────────────────────
# 4. UPLOAD PROGRESS BAR
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.contextmanager
def upload_progress_bar(total_records: int, chunk_size: int = 500, label: str = "Uploading"):
    """
    Context manager: renders a live progress bar during chunked DB upserts.

    Usage:
        with upload_progress_bar(len(records), chunk_size=CHUNK) as tick:
            for i in range(0, len(records), CHUNK):
                supabase.table(...).upsert(records[i:i+CHUNK]).execute()
                tick()
    """
    import math
    total_chunks = max(1, math.ceil(total_records / chunk_size))
    bar_slot = st.empty()
    txt_slot = st.empty()
    done     = [0]

    bar_slot.progress(0.0)
    txt_slot.caption(f"⬆️ {label}: 0 / {total_records:,} records (0%)")

    def tick():
        done[0] += 1
        frac        = min(done[0] / total_chunks, 1.0)
        uploaded    = min(done[0] * chunk_size, total_records)
        bar_slot.progress(frac)
        txt_slot.caption(
            f"⬆️ {label}: {uploaded:,} / {total_records:,} records ({int(frac*100)}%)"
        )

    try:
        yield tick
    finally:
        bar_slot.progress(1.0)
        txt_slot.caption(f"✅ {label} complete — {total_records:,} records synced.")
        time.sleep(0.6)
        bar_slot.empty()
        txt_slot.empty()


# ─────────────────────────────────────────────────────────────────────────────
# 5. LAZY SECTION GATE  (hash-based, no DataFrame storage)
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.contextmanager
def lazy_section(key: str, depends_on: Any):
    """
    Skips expensive Python computation when depends_on hasn't changed.
    Stores only an integer hash — never a DataFrame.

    Usage:
        with lazy_section("wow", depends_on=(dd_start, dd_end, wow_col)):
            if not is_lazy_skip("wow"):
                weekly = dd_df.groupby(...)...
                st.session_state["_wow_weekly"] = weekly
            weekly = st.session_state.get("_wow_weekly", pd.DataFrame())
    """
    dep_key = f"_lazy_dep_{key}"
    try:
        dep_hash = hash(depends_on)
    except TypeError:
        dep_hash = id(depends_on)

    st.session_state[f"_lazy_skip_{key}"] = (
        st.session_state.get(dep_key) == dep_hash
    )
    st.session_state[dep_key] = dep_hash
    yield
    st.session_state.pop(f"_lazy_skip_{key}", None)


def is_lazy_skip(key: str) -> bool:
    """Returns True if lazy_section determined no recomputation is needed."""
    return st.session_state.get(f"_lazy_skip_{key}", False)


# ─────────────────────────────────────────────────────────────────────────────
# 6. CACHED AGGREGATION  (lightweight — stores only the aggregated result,
#    which is much smaller than the raw filtered DataFrame)
# ─────────────────────────────────────────────────────────────────────────────

def cached_agg(
    cache_key: str,
    df: pd.DataFrame,
    group_cols: list,
    agg_col: str,
    agg_func: str = "sum",
    filter_hash: str = "",
) -> pd.DataFrame:
    """
    Cache a groupby result. The aggregated DataFrame is small (one row per
    group), so storing it in session_state is safe.
    Prunes stale entries for the same cache_key automatically.
    """
    store_key = f"_agg_{cache_key}_{filter_hash}"
    if store_key not in st.session_state:
        result = df.groupby(group_cols)[agg_col].agg(agg_func).reset_index()
        st.session_state[store_key] = result
        # Remove stale entries for this key
        for k in [k for k in st.session_state
                  if k.startswith(f"_agg_{cache_key}_") and k != store_key]:
            del st.session_state[k]
    return st.session_state[store_key]
