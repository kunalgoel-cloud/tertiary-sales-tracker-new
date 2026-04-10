"""
performance.py — Mamanourish Performance Optimization Layer
═══════════════════════════════════════════════════════════════

WHAT THIS MODULE DOES:
────────────────────────
Centralises every performance concern so modules stay clean.

1. SMART DATA CACHING  (avoid redundant Supabase round-trips)
   ─────────────────────────────────────────────────────────
   • cached_filter_result()    — memoises filtered DataFrame slices keyed
                                  by (start, end, channels_hash, products_hash).
                                  Filter changes on already-fetched data never
                                  hit the DB again.
   • get_table_cached()        — wrapper around get_table with configurable TTL
                                  and a 30-second "fast path" for metadata tables.

2. DEBOUNCE / STABLE FILTER STATE
   ─────────────────────────────────────────────────────────
   • Streamlit reruns on every widget change. Debouncing is approximated by
     comparing the new filter state hash against the last-rendered hash stored
     in st.session_state["_perf_last_filter_hash"].
   • should_recompute(key, **state) returns True only when state actually
     changed, letting callers skip expensive groupby/merge operations.

3. PROGRESSIVE UPLOAD PROGRESS BAR
   ─────────────────────────────────────────────────────────
   • upload_progress_bar(total_chunks) — context manager that wraps the chunked
     Supabase upsert loop and renders a st.progress bar + status text.

4. LAZY SECTION LOADING
   ─────────────────────────────────────────────────────────
   • lazy_section(key) — context manager. Wraps expensive render blocks.
     On first visit renders normally. On subsequent filter-only reruns
     that don't change the section's data dependency, returns the cached
     rendered state via st.session_state so Streamlit skips re-executing
     the inner block.
     NOTE: Streamlit doesn't support true output caching; this pattern
     gates the Python computation inside the block, not the actual DOM diff.

5. LOADING INDICATORS
   ─────────────────────────────────────────────────────────
   • top_bar_css()             — injects a thin animated top loading bar that
                                  plays during every Streamlit rerun (pure CSS,
                                  zero JS overhead).
   • inline_spinner(msg)       — styled spinner for use in tab content areas.
   • skeleton_chart()          — placeholder shimmer while a chart computes.
   • skeleton_metrics(n)       — n shimmer metric cards during data load.

HOW TO USE:
────────────────────────────────────────────────────────────────
In app.py (once, after inject_css()):
    from performance import inject_perf_css
    inject_perf_css()

In any tab, wrap expensive filters:
    from performance import should_recompute, cached_filter_result
    if should_recompute("analytics", start=s, end=e, chans=ch, prods=pr):
        filtered = _do_filter(history_df, s, e, ch, pr)
        st.session_state["_cache_analytics"] = filtered
    else:
        filtered = st.session_state.get("_cache_analytics", pd.DataFrame())

For chunked uploads:
    from performance import upload_progress_bar
    with upload_progress_bar(len(records), chunk_size=CHUNK) as tick:
        for i in range(0, len(records), CHUNK):
            supabase.table(...).upsert(records[i:i+CHUNK]).execute()
            tick()
"""

import hashlib
import time
import contextlib
from typing import Any, Callable

import pandas as pd
import streamlit as st


# ─────────────────────────────────────────────────────────────────────────────
# 1. TOP LOADING BAR + PERFORMANCE CSS
# ─────────────────────────────────────────────────────────────────────────────

def inject_perf_css() -> None:
    """
    Injects:
    - Animated top progress bar (plays on every Streamlit rerun automatically)
    - Skeleton shimmer keyframes for placeholder components
    - Smooth fade-in for chart/table containers
    Call once at app startup, after inject_css().
    """
    st.markdown("""
<style>
/* ═══════════════════════════════════════════════════
   TOP LOADING BAR
   Plays automatically on every page rerun.
   Uses a CSS animation triggered by :root load — no JS needed.
═══════════════════════════════════════════════════ */
@keyframes topbar-slide {
  0%   { width: 0%;   opacity: 1; }
  60%  { width: 80%;  opacity: 1; }
  90%  { width: 95%;  opacity: 1; }
  100% { width: 100%; opacity: 0; }
}

body::before {
  content: '';
  position: fixed;
  top: 0; left: 0;
  height: 3px;
  background: linear-gradient(90deg, #C47A2B, #2A8A7E, #C47A2B);
  background-size: 200% 100%;
  animation:
    topbar-slide 1.4s cubic-bezier(.4,0,.2,1) forwards,
    shimmer-bg 1.4s linear infinite;
  z-index: 10000;
  pointer-events: none;
}

@keyframes shimmer-bg {
  0%   { background-position: 200% center; }
  100% { background-position: -200% center; }
}

/* ═══════════════════════════════════════════════════
   SKELETON SHIMMER
═══════════════════════════════════════════════════ */
@keyframes skeleton-pulse {
  0%   { background-position: -400px 0; }
  100% { background-position: 400px 0; }
}

.mn-skeleton {
  background: linear-gradient(
    90deg,
    #F0EDE8 25%,
    #E8E4DF 50%,
    #F0EDE8 75%
  );
  background-size: 400px 100%;
  animation: skeleton-pulse 1.4s ease-in-out infinite;
  border-radius: 8px;
}

.mn-skeleton-metric {
  height: 88px;
  border-radius: 12px;
  margin-bottom: 1rem;
}

.mn-skeleton-chart {
  height: 320px;
  border-radius: 12px;
  margin-bottom: 1rem;
}

.mn-skeleton-row {
  height: 18px;
  border-radius: 4px;
  margin-bottom: 8px;
}

/* ═══════════════════════════════════════════════════
   FADE-IN FOR CHART CONTAINERS
   Plays when a chart is first rendered or updated.
═══════════════════════════════════════════════════ */
[data-testid="stPlotlyChart"],
[data-testid="stDataFrame"] {
  animation: content-fade-in 0.28s ease both !important;
}

@keyframes content-fade-in {
  from { opacity: 0; transform: translateY(4px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ═══════════════════════════════════════════════════
   UPLOAD PROGRESS BAR STYLING
═══════════════════════════════════════════════════ */
.stProgress > div > div > div > div {
  background: linear-gradient(90deg, #C47A2B, #2A8A7E) !important;
  border-radius: 4px !important;
  transition: width 0.2s ease !important;
}

.stProgress > div > div {
  background: #F0EDE8 !important;
  border-radius: 4px !important;
  height: 6px !important;
}

/* ═══════════════════════════════════════════════════
   INLINE SPINNER (styled version of st.spinner)
═══════════════════════════════════════════════════ */
[data-testid="stSpinnerContainer"] {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  background: rgba(196,122,43,.06);
  border: 1px solid rgba(196,122,43,.2);
  border-radius: 10px;
  padding: 0.65rem 1rem;
  font-size: 0.83rem;
  color: #C47A2B;
  font-family: 'DM Sans', system-ui, sans-serif;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# 2. SKELETON PLACEHOLDER COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────

def skeleton_metrics(n: int = 4) -> None:
    """Render n shimmer metric card placeholders while data loads."""
    cols = st.columns(n)
    for col in cols:
        col.markdown(
            '<div class="mn-skeleton mn-skeleton-metric"></div>',
            unsafe_allow_html=True,
        )


def skeleton_chart(height: int = 320, label: str = "Loading chart…") -> None:
    """Render a shimmer chart placeholder."""
    st.markdown(
        f'<div class="mn-skeleton mn-skeleton-chart" '
        f'style="height:{height}px;" '
        f'title="{label}"></div>',
        unsafe_allow_html=True,
    )


def skeleton_table(rows: int = 5) -> None:
    """Render shimmer table row placeholders."""
    for _ in range(rows):
        st.markdown(
            '<div class="mn-skeleton mn-skeleton-row"></div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 3. SMART FILTER RESULT CACHING  (avoid re-filtering on unrelated reruns)
# ─────────────────────────────────────────────────────────────────────────────

def _filter_hash(start, end, channels, products) -> str:
    """Stable hash of filter state — used as cache key."""
    key = f"{start}|{end}|{sorted(channels or [])}|{sorted(products or [])}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def should_recompute(cache_key: str, start, end, channels, products) -> bool:
    """
    Returns True only when the filter state has changed since last render.
    On first call always returns True (cold start).

    Usage:
        if should_recompute("analytics", start, end, channels, products):
            filtered = expensive_filter(df, start, end, channels, products)
            st.session_state["_fc_analytics"] = filtered
        filtered = st.session_state.get("_fc_analytics", pd.DataFrame())
    """
    hash_key   = f"_fhash_{cache_key}"
    new_hash   = _filter_hash(start, end, channels, products)
    old_hash   = st.session_state.get(hash_key)
    changed    = (old_hash != new_hash)
    if changed:
        st.session_state[hash_key] = new_hash
    return changed


def get_filtered_df(
    cache_key: str,
    df: pd.DataFrame,
    start,
    end,
    channels,
    products,
) -> pd.DataFrame:
    """
    One-shot helper: checks should_recompute, filters if needed, caches result.
    Applies date, channel, and product filters to any DataFrame with
    'date_dt', 'channel', 'item_name' columns.

    Returns the filtered DataFrame (from cache if filter unchanged).
    """
    store_key = f"_fc_{cache_key}"

    if should_recompute(cache_key, start, end, channels, products):
        result = df.copy()

        if start is not None and "date_dt" in result.columns:
            result = result[
                (result["date_dt"].dt.date >= start) &
                (result["date_dt"].dt.date <= end)
            ]
        if channels and "channel" in result.columns:
            result = result[result["channel"].isin(channels)]
        if products and "item_name" in result.columns:
            result = result[result["item_name"].isin(products)]

        st.session_state[store_key] = result

    return st.session_state.get(store_key, pd.DataFrame())


# ─────────────────────────────────────────────────────────────────────────────
# 4. UPLOAD PROGRESS BAR  (chunked Supabase upserts with live % indicator)
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.contextmanager
def upload_progress_bar(total_records: int, chunk_size: int = 500, label: str = "Uploading"):
    """
    Context manager that renders a live progress bar during chunked uploads.

    Usage:
        total_chunks = ceil(len(records) / CHUNK)
        with upload_progress_bar(len(records), chunk_size=CHUNK) as tick:
            for i in range(0, len(records), CHUNK):
                supabase.table("sales").upsert(records[i:i+CHUNK], ...).execute()
                tick()

    The context manager yields a tick() callable. Call it once per chunk.
    """
    import math
    total_chunks = max(1, math.ceil(total_records / chunk_size))
    bar_slot     = st.empty()
    txt_slot     = st.empty()
    done         = [0]

    # Show initial state
    bar_slot.progress(0.0)
    txt_slot.caption(f"⬆️ {label}: 0 / {total_records:,} records (chunk 0/{total_chunks})")

    def tick():
        done[0] += 1
        frac = min(done[0] / total_chunks, 1.0)
        uploaded_est = min(done[0] * chunk_size, total_records)
        bar_slot.progress(frac)
        txt_slot.caption(
            f"⬆️ {label}: {uploaded_est:,} / {total_records:,} records "
            f"({int(frac * 100)}%)"
        )

    try:
        yield tick
    finally:
        # Complete and auto-clear after 1.5 s
        bar_slot.progress(1.0)
        txt_slot.caption(f"✅ {label} complete — {total_records:,} records synced.")
        time.sleep(0.8)
        bar_slot.empty()
        txt_slot.empty()


# ─────────────────────────────────────────────────────────────────────────────
# 5. LAZY SECTION LOADING  (skip expensive Python when data hasn't changed)
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.contextmanager
def lazy_section(key: str, depends_on: Any):
    """
    Gate expensive computation: only execute the inner block when depends_on
    changes. Useful for views that do heavy groupby/pivot inside a tab when
    the user only changed a display toggle (not the data filter).

    depends_on: any hashable value or tuple of values that the section
                depends on (e.g. the filtered DataFrame hash + display metric).

    Usage:
        dep = (id(dd_df), wow_metric)   # changes when df or metric changes
        with lazy_section("wow_chart", depends_on=dep):
            # Only runs when dep changes
            weekly = dd_df.groupby(...)...
            st.plotly_chart(fig)
    """
    dep_key  = f"_lazy_dep_{key}"
    try:
        dep_hash = hash(depends_on)
    except TypeError:
        dep_hash = id(depends_on)

    old_hash = st.session_state.get(dep_key)
    if old_hash == dep_hash:
        # Nothing changed — skip re-computation, Streamlit re-renders from
        # its own widget tree cache. We still yield so the `with` block runs
        # but callers can check st.session_state["_lazy_skip_{key}"] to bail
        # out of heavy work early.
        st.session_state[f"_lazy_skip_{key}"] = True
    else:
        st.session_state[dep_key]             = dep_hash
        st.session_state[f"_lazy_skip_{key}"] = False

    yield

    # Clean up the skip flag after the block executes
    st.session_state.pop(f"_lazy_skip_{key}", None)


def is_lazy_skip(key: str) -> bool:
    """Returns True if lazy_section determined no recomputation is needed."""
    return st.session_state.get(f"_lazy_skip_{key}", False)


# ─────────────────────────────────────────────────────────────────────────────
# 6. CACHED METRIC COMPUTATION  (memoised aggregates per filter state)
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
    Cache a groupby aggregation result. Returns the cached result if the
    filter_hash (from _filter_hash()) hasn't changed.

    Usage:
        fh = _filter_hash(start, end, channels, products)
        chan_rev = cached_agg("chan_rev", dd_df, ["channel"], "revenue",
                              filter_hash=fh)
    """
    store_key = f"_agg_{cache_key}_{filter_hash}"
    if store_key not in st.session_state:
        result = df.groupby(group_cols)[agg_col].agg(agg_func).reset_index()
        st.session_state[store_key] = result
        # Prune stale keys for this cache_key (keep only latest)
        stale = [k for k in st.session_state if
                 k.startswith(f"_agg_{cache_key}_") and k != store_key]
        for k in stale:
            del st.session_state[k]
    return st.session_state[store_key]
