"""
ui_theme.py — Mamanourish Executive Tracker Design System
═══════════════════════════════════════════════════════════

DESIGN DIRECTION: "Refined Ink"
────────────────────────────────
A data-first executive dashboard aesthetic — dark slate canvas, warm amber
accents, crisp typography. Inspired by Bloomberg Terminal meets Notion.
Clean without being sterile. Dense without being cluttered.

FONT STACK:
  Display  → "DM Serif Display" (elegant, editorial authority)
  UI/Body  → "DM Sans" (geometric, legible at small sizes)
  Mono     → "JetBrains Mono" (code, numbers, data)

COLOR PALETTE:
  Background  #0F1117  (deep ink)
  Surface     #1A1D27  (elevated card)
  Border      #2A2D3A  (subtle divider)
  Accent      #F5A623  (warm amber — brand warmth)
  Accent2     #4ECDC4  (teal — secondary actions)
  Text-1      #F0F2F5  (primary text)
  Text-2      #9BA3B0  (secondary/muted text)
  Success     #2ECC71
  Warning     #F39C12
  Danger      #E74C3C

REUSABLE COMPONENTS (call these functions inside tabs):
  inject_css()          → inject the entire design system (call once at app start)
  card(content, ...)    → styled card container
  metric_card(...)      → KPI tile with label, value, delta
  section_header(...)   → styled section divider with icon + title
  badge(text, style)    → inline status pill
  info_banner(...)      → styled info/warning/success banners
  chart_layout(fig)     → applies consistent Plotly theme to any figure
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px


# ─────────────────────────────────────────────────────────────────────────────
# DESIGN TOKENS
# ─────────────────────────────────────────────────────────────────────────────

COLORS = {
    "bg":        "#F7F5F2",   # warm off-white canvas
    "surface":   "#FFFFFF",   # pure white cards
    "surface2":  "#F0EDE8",   # slightly warm grey for nested elements
    "border":    "#E2DDD8",   # warm grey border
    "accent":    "#C47A2B",   # deep amber — brand warmth
    "accent2":   "#2A8A7E",   # teal — secondary actions
    "text1":     "#1C1917",   # near-black
    "text2":     "#6B5F55",   # warm brown-grey
    "text3":     "#A89E95",   # muted warm grey
    "success":   "#2D7D52",   # earthy green
    "warning":   "#B45309",   # warm amber-orange
    "danger":    "#B91C1C",   # deep red
    "info":      "#1D5FA6",   # muted blue
}

# Plotly color sequences — warm, mellow, cohesive with light theme
CHART_COLORS = [
    "#C47A2B", "#2A8A7E", "#7C5CBF", "#C45454",
    "#2D7D52", "#1D5FA6", "#B45309", "#C2547A",
    "#5C7ABF", "#3A8A7A",
]


# ─────────────────────────────────────────────────────────────────────────────
# MASTER CSS INJECTION
# ─────────────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    """
    Inject the full Mamanourish design system into the Streamlit app.
    Call ONCE at the top of app.py, after set_page_config().

    Covers:
    - Google Fonts import (DM Serif Display, DM Sans, JetBrains Mono)
    - CSS custom properties (design tokens)
    - Global resets & body typography
    - Streamlit component overrides (tabs, sidebar, buttons, inputs,
      metrics, dataframes, expanders, alerts)
    - Card component classes
    - Animated global filter bar
    - Micro-interaction keyframes
    - Scrollbar styling
    """
    st.markdown(
        """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,300;1,9..40,400&family=DM+Serif+Display:ital@0;1&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">

<style>
:root {
  --bg:         #F7F5F2;
  --surface:    #FFFFFF;
  --surface2:   #F0EDE8;
  --border:     #E2DDD8;
  --accent:     #C47A2B;
  --accent2:    #2A8A7E;
  --text1:      #1C1917;
  --text2:      #6B5F55;
  --text3:      #A89E95;
  --success:    #2D7D52;
  --warning:    #B45309;
  --danger:     #B91C1C;
  --info:       #1D5FA6;
  --radius-sm:  6px;
  --radius-md:  10px;
  --radius-lg:  16px;
  --shadow-sm:  0 1px 4px rgba(0,0,0,.07);
  --shadow-md:  0 4px 16px rgba(0,0,0,.09);
  --shadow-lg:  0 8px 32px rgba(0,0,0,.11);
  --transition: 0.18s ease;
  --font-ui:    'DM Sans', system-ui, sans-serif;
  --font-disp:  'DM Serif Display', Georgia, serif;
  --font-mono:  'JetBrains Mono', monospace;
}

/* GLOBAL */
*, *::before, *::after { box-sizing: border-box; }
html, body, .stApp { background-color: var(--bg) !important; color: var(--text1) !important; font-family: var(--font-ui) !important; -webkit-font-smoothing: antialiased; }
.main .block-container { padding: 0 2rem 3rem !important; max-width: 1440px !important; }

/* STICKY GLOBAL FILTER BAR */
.mn-sticky-bar {
  position: sticky !important;
  top: 0 !important;
  z-index: 999 !important;
  background: rgba(247,245,242,0.96) !important;
  border-bottom: 1px solid var(--border) !important;
  padding: 0.6rem 2rem !important;
  margin: 0 -2rem 1rem !important;
  box-shadow: 0 2px 12px rgba(0,0,0,.06) !important;
  backdrop-filter: blur(8px) !important;
}
[data-testid="stMarkdown"]:has(.mn-sticky-bar) {
  position: sticky !important; top: 0 !important; z-index: 999 !important;
  background: rgba(247,245,242,0.96) !important; margin: 0 !important; padding: 0 !important;
}

/* TYPOGRAPHY */
h1, .stApp h1 { font-family: var(--font-disp) !important; font-size: 2rem !important; font-weight: 400 !important; color: var(--text1) !important; letter-spacing: -0.02em; }
h2, .stApp h2 { font-family: var(--font-ui) !important; font-size: 1.3rem !important; font-weight: 600 !important; color: var(--text1) !important; }
h3, .stApp h3 { font-family: var(--font-ui) !important; font-size: 1rem !important; font-weight: 600 !important; color: var(--text1) !important; }
h4, .stApp h4 { font-family: var(--font-ui) !important; font-size: 0.75rem !important; font-weight: 700 !important; color: var(--text3) !important; text-transform: uppercase; letter-spacing: 0.08em; }
p, .stMarkdown p { color: var(--text2) !important; font-size: 0.88rem; }
code { font-family: var(--font-mono) !important; background: var(--surface2) !important; color: var(--accent) !important; padding: 0.1em 0.4em !important; border-radius: var(--radius-sm) !important; font-size: 0.82em !important; border: 1px solid var(--border) !important; }

/* SIDEBAR */
[data-testid="stSidebar"] { background: var(--surface) !important; border-right: 1px solid var(--border) !important; }
[data-testid="stSidebar"] > div:first-child { padding: 1.5rem 1rem !important; }
[data-testid="stSidebar"] .stMarkdown h2 { font-size: 0.65rem !important; font-weight: 700 !important; color: var(--text3) !important; text-transform: uppercase; letter-spacing: 0.12em; margin: 1.5rem 0 0.5rem !important; padding-bottom: 0.4rem; border-bottom: 1px solid var(--border); }

/* TABS */
[data-testid="stTabs"] > div:first-child { background: var(--surface) !important; border-bottom: 1px solid var(--border) !important; gap: 0 !important; padding: 0 !important; overflow-x: auto; scrollbar-width: none; }
[data-testid="stTabs"] > div:first-child::-webkit-scrollbar { display: none; }
[data-testid="stTabs"] button[role="tab"] { font-family: var(--font-ui) !important; font-size: 0.82rem !important; font-weight: 500 !important; color: var(--text2) !important; background: transparent !important; border: none !important; border-bottom: 2px solid transparent !important; border-radius: 0 !important; padding: 0.65rem 1.1rem !important; margin: 0 !important; cursor: pointer; transition: color var(--transition), border-color var(--transition) !important; white-space: nowrap; }
[data-testid="stTabs"] button[role="tab"]:hover { color: var(--text1) !important; background: var(--surface2) !important; }
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] { color: var(--accent) !important; border-bottom-color: var(--accent) !important; font-weight: 600 !important; }
[data-testid="stTabsContent"] { padding-top: 1.25rem !important; animation: fadeInUp 0.2s ease both; }
@keyframes fadeInUp { from { opacity:0; transform:translateY(6px); } to { opacity:1; transform:translateY(0); } }

/* BUTTONS */
.stButton > button { font-family: var(--font-ui) !important; font-size: 0.82rem !important; font-weight: 600 !important; color: #FFF !important; background: var(--accent) !important; border: none !important; border-radius: var(--radius-md) !important; padding: 0.5rem 1.1rem !important; min-height: 36px !important; transition: all var(--transition) !important; box-shadow: 0 1px 4px rgba(196,122,43,.25) !important; }
.stButton > button:hover { background: #A86320 !important; transform: translateY(-1px) !important; box-shadow: 0 4px 12px rgba(196,122,43,.30) !important; }
.stButton > button:active { transform: translateY(0) !important; }
.stFormSubmitButton > button { background: var(--accent2) !important; }
.stFormSubmitButton > button:hover { background: #1F6E64 !important; }

/* INPUTS */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div > div,
.stMultiSelect > div > div > div { font-family: var(--font-ui) !important; font-size: 0.85rem !important; background: var(--surface) !important; color: var(--text1) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important; }
.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus { border-color: var(--accent) !important; box-shadow: 0 0 0 3px rgba(196,122,43,.12) !important; outline: none !important; }
.stMultiSelect [data-baseweb="tag"] { background: rgba(196,122,43,.12) !important; color: var(--accent) !important; border-radius: var(--radius-sm) !important; font-size: 0.75rem !important; font-weight: 600 !important; }
[data-baseweb="select"] [data-baseweb="menu"] { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important; box-shadow: var(--shadow-lg) !important; }
[data-baseweb="select"] [role="option"]:hover { background: var(--surface2) !important; }
[data-baseweb="select"] [aria-selected="true"] { background: rgba(196,122,43,.08) !important; color: var(--accent) !important; }
label, .stSelectbox label, .stMultiSelect label, .stTextInput label, .stNumberInput label, .stDateInput label, .stRadio label { font-family: var(--font-ui) !important; font-size: 0.72rem !important; font-weight: 700 !important; color: var(--text2) !important; text-transform: uppercase; letter-spacing: 0.06em; }
.stDateInput > div > div > input { background: var(--surface) !important; color: var(--text1) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important; }
[data-testid="stFileUploader"] { background: var(--surface2) !important; border: 1.5px dashed var(--border) !important; border-radius: var(--radius-lg) !important; }
[data-testid="stFileUploader"]:hover { border-color: var(--accent) !important; }

/* METRICS */
[data-testid="stMetric"] { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-lg) !important; padding: 1.1rem 1.25rem !important; transition: transform var(--transition), box-shadow var(--transition) !important; position: relative; overflow: hidden; }
[data-testid="stMetric"]::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: linear-gradient(90deg, var(--accent), var(--accent2)); opacity: 0; transition: opacity var(--transition); }
[data-testid="stMetric"]:hover { transform: translateY(-2px) !important; box-shadow: var(--shadow-md) !important; }
[data-testid="stMetric"]:hover::before { opacity: 1; }
[data-testid="stMetric"] label { font-size: 0.68rem !important; font-weight: 700 !important; color: var(--text3) !important; text-transform: uppercase !important; letter-spacing: 0.1em !important; }
[data-testid="stMetricValue"] { font-family: var(--font-mono) !important; font-size: 1.6rem !important; font-weight: 600 !important; color: var(--text1) !important; line-height: 1.2 !important; }

/* ALERTS */
[data-testid="stAlert"], div[class*="stAlert"] { border-radius: var(--radius-md) !important; border: 1px solid !important; font-size: 0.83rem !important; font-family: var(--font-ui) !important; padding: 0.65rem 1rem !important; }
div[class*="info"]    { background: rgba(29,95,166,.06)  !important; border-color: rgba(29,95,166,.25)  !important; color: #1D5FA6 !important; }
div[class*="success"] { background: rgba(45,125,82,.06)  !important; border-color: rgba(45,125,82,.25)  !important; color: #2D7D52 !important; }
div[class*="warning"] { background: rgba(180,83,9,.06)   !important; border-color: rgba(180,83,9,.25)   !important; color: #B45309 !important; }
div[class*="error"]   { background: rgba(185,28,28,.06)  !important; border-color: rgba(185,28,28,.25)  !important; color: #B91C1C !important; }

/* DATAFRAMES */
[data-testid="stDataFrame"] { border-radius: var(--radius-lg) !important; overflow: hidden !important; border: 1px solid var(--border) !important; box-shadow: var(--shadow-sm) !important; }
[data-testid="stDataFrame"] table { font-family: var(--font-mono) !important; font-size: 0.78rem !important; }
[data-testid="stDataFrame"] thead tr th { background: var(--surface2) !important; color: var(--text3) !important; font-family: var(--font-ui) !important; font-size: 0.68rem !important; font-weight: 700 !important; text-transform: uppercase; letter-spacing: 0.08em; border-bottom: 1px solid var(--border) !important; }
[data-testid="stDataFrame"] tbody tr:hover td { background: var(--surface2) !important; }
[data-testid="stDataFrame"] tbody tr td { border-bottom: 1px solid var(--border) !important; color: var(--text1) !important; }

/* EXPANDERS */
[data-testid="stExpander"] { background: var(--surface) !important; border: 1px solid var(--border) !important; border-radius: var(--radius-md) !important; margin-bottom: 0.5rem !important; transition: box-shadow var(--transition) !important; }
[data-testid="stExpander"]:hover { box-shadow: var(--shadow-sm) !important; border-color: rgba(196,122,43,.3) !important; }
[data-testid="stExpander"] summary { font-family: var(--font-ui) !important; font-size: 0.85rem !important; font-weight: 600 !important; color: var(--text1) !important; padding: 0.75rem 1rem !important; }

/* CHARTS */
[data-testid="stPlotlyChart"] { border-radius: var(--radius-lg) !important; overflow: hidden !important; background: var(--surface) !important; border: 1px solid var(--border) !important; padding: 0.5rem !important; box-shadow: var(--shadow-sm) !important; transition: box-shadow var(--transition) !important; }
[data-testid="stPlotlyChart"]:hover { box-shadow: var(--shadow-md) !important; }

/* CARDS */
.mn-card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius-lg); padding: 1.25rem; margin-bottom: 1rem; transition: box-shadow var(--transition), border-color var(--transition); position: relative; overflow: hidden; }
.mn-card:hover { box-shadow: var(--shadow-md); border-color: rgba(196,122,43,.2); }
.mn-card-accent::before  { content:''; position:absolute; left:0;top:0;bottom:0;width:3px;background:var(--accent); }
.mn-card-teal::before    { content:''; position:absolute; left:0;top:0;bottom:0;width:3px;background:var(--accent2); }
.mn-card-success::before { content:''; position:absolute; left:0;top:0;bottom:0;width:3px;background:var(--success); }
.mn-card-danger::before  { content:''; position:absolute; left:0;top:0;bottom:0;width:3px;background:var(--danger); }
.mn-section-header { display:flex;align-items:center;gap:0.6rem;margin:1.5rem 0 0.75rem;padding-bottom:0.6rem;border-bottom:1px solid var(--border); }
.mn-section-header .title { font-family:var(--font-ui);font-size:0.9rem;font-weight:600;color:var(--text1); }
.mn-section-header .subtitle { font-size:0.72rem;color:var(--text3);margin-left:auto; }
.mn-badge { display:inline-flex;align-items:center;gap:0.3em;padding:0.18em 0.6em;border-radius:100px;font-size:0.7rem;font-weight:700;font-family:var(--font-ui); }
.mn-badge-amber { background:rgba(196,122,43,.12);color:var(--accent); }
.mn-badge-teal  { background:rgba(42,138,126,.12);color:var(--accent2); }
.mn-badge-green { background:rgba(45,125,82,.12); color:var(--success); }
.mn-badge-red   { background:rgba(185,28,28,.12); color:var(--danger); }
.mn-badge-gray  { background:rgba(168,158,149,.15);color:var(--text2); }

/* MISC */
hr, [data-testid="stDivider"] > hr { border:none !important; border-top:1px solid var(--border) !important; margin:1.25rem 0 !important; opacity:1 !important; }
[data-testid="stCaptionContainer"] p, .stCaption p { font-size:0.75rem !important; color:var(--text3) !important; }
[data-testid="stSpinner"] svg { color: var(--accent) !important; }
::-webkit-scrollbar { width:6px; height:6px; }
::-webkit-scrollbar-track { background:var(--bg); }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
::-webkit-scrollbar-thumb:hover { background:var(--text3); }
.role-badge { display:inline-flex;align-items:center;gap:0.4rem;background:rgba(196,122,43,.08);border:1px solid rgba(196,122,43,.2);border-radius:var(--radius-md);padding:0.3rem 0.7rem;font-size:0.72rem;font-weight:700;color:var(--accent);letter-spacing:0.05em;margin-bottom:0.5rem; }
[data-testid="stToast"] { background:var(--surface) !important;border:1px solid var(--border) !important;border-radius:var(--radius-md) !important;color:var(--text1) !important;font-family:var(--font-ui) !important;box-shadow:var(--shadow-lg) !important; }
*:focus-visible { outline:2px solid var(--accent) !important; outline-offset:2px !important; }

/* ═══════════════════════════════════════════════════
   STICKY GLOBAL FILTER — targets the stMarkdown block
   that contains the .mn-sticky-bar div. We cannot use
   a pure-Streamlit container for sticky, so we target
   via CSS attribute selectors.
═══════════════════════════════════════════════════ */

/* The global filter renders BEFORE st.tabs(), so it sits in the main
   vertical block. We target any stMarkdown that wraps .mn-sticky-bar */
.mn-sticky-bar {
  position: sticky !important;
  top: 0 !important;
  z-index: 1000 !important;
  background: rgba(247,245,242,0.97) !important;
  border-bottom: 1px solid #E2DDD8 !important;
  padding: 0.5rem 0 0.25rem !important;
  margin-bottom: 0.5rem !important;
  backdrop-filter: blur(10px) !important;
  -webkit-backdrop-filter: blur(10px) !important;
  box-shadow: 0 2px 16px rgba(0,0,0,.07) !important;
}

/* Lift the stMarkdown parent that wraps the sticky bar */
[data-testid="stMarkdown"]:has(.mn-sticky-bar) {
  position: sticky !important;
  top: 0 !important;
  z-index: 1000 !important;
  background: rgba(247,245,242,0.97) !important;
  padding: 0 !important;
  margin: 0 0 0.5rem !important;
}

/* The stVerticalBlock that wraps the entire filter section */
[data-testid="stVerticalBlock"]:has(.mn-sticky-bar) {
  position: sticky !important;
  top: 0 !important;
  z-index: 1000 !important;
  background: rgba(247,245,242,0.97) !important;
}

/* Ensure tabs strip doesn't cover the filter bar */
[data-testid="stTabs"] {
  position: relative;
  z-index: 990;
}
</style>
""",
        unsafe_allow_html=True,
    )

    # JavaScript to physically move the filter bar to the top of the page
    # This runs once after Streamlit renders, finding .mn-sticky-bar and
    # inserting it before the main content block.
    st.markdown(
        """
        <script>
        (function moveStickyBar() {
          function tryMove() {
            var bar = document.querySelector('.mn-sticky-bar');
            if (!bar) { setTimeout(tryMove, 120); return; }

            // Walk up to find the stMarkdown wrapper
            var wrapper = bar;
            while (wrapper && !wrapper.hasAttribute('data-testid')) {
              wrapper = wrapper.parentElement;
            }
            if (!wrapper) { setTimeout(tryMove, 120); return; }

            // Find the main block-container
            var main = document.querySelector('.main .block-container');
            if (!main) { setTimeout(tryMove, 120); return; }

            // Create a sticky sentinel div at the very top
            var sentinel = document.createElement('div');
            sentinel.id  = 'mn-filter-sentinel';
            sentinel.style.cssText = [
              'position:sticky', 'top:0', 'z-index:1000',
              'background:rgba(247,245,242,0.97)',
              'border-bottom:1px solid #E2DDD8',
              'box-shadow:0 2px 16px rgba(0,0,0,.07)',
              'backdrop-filter:blur(10px)',
              'padding:0.5rem 0 0.25rem',
              'margin-bottom:0.5rem',
            ].join(';');

            // Move wrapper clone to top if not already moved
            if (!document.getElementById('mn-filter-sentinel')) {
              main.insertBefore(sentinel, main.firstChild);
              sentinel.appendChild(wrapper);
            }
          }
          // Run after Streamlit hydration
          if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', tryMove);
          } else {
            setTimeout(tryMove, 200);
          }
        })();
        </script>
        """,
        unsafe_allow_html=True,
    )


# ME
# ─────────────────────────────────────────────────────────────────────────────

def apply_chart_theme(fig: go.Figure, height: int = None) -> go.Figure:
    """
    Apply consistent Mamanourish dark theme to any Plotly figure.
    Call before st.plotly_chart(fig).

    Usage:
        fig = px.bar(...)
        fig = apply_chart_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
    """
    updates = dict(
        plot_bgcolor  = "rgba(0,0,0,0)",
        paper_bgcolor = "rgba(0,0,0,0)",
        font          = dict(family="DM Sans, system-ui, sans-serif",
                             color="#6B5F55", size=11),
        title_font    = dict(family="DM Sans", color="#1C1917", size=14),
        legend        = dict(
            bgcolor     = "rgba(255,255,255,0.9)",
            bordercolor = "#E2DDD8",
            borderwidth = 1,
            font        = dict(size=11, color="#6B5F55"),
        ),
        xaxis = dict(
            gridcolor    = "#F0EDE8",
            linecolor    = "#E2DDD8",
            tickcolor    = "#E2DDD8",
            tickfont     = dict(color="#A89E95", size=10),
            title_font   = dict(color="#6B5F55", size=11),
            zeroline     = False,
        ),
        yaxis = dict(
            gridcolor    = "#F0EDE8",
            linecolor    = "rgba(0,0,0,0)",
            tickcolor    = "#E2DDD8",
            tickfont     = dict(color="#A89E95", size=10),
            title_font   = dict(color="#6B5F55", size=11),
            zeroline     = False,
        ),
        hoverlabel = dict(
            bgcolor     = "#FFFFFF",
            bordercolor = "#E2DDD8",
            font        = dict(family="DM Sans", color="#1C1917", size=12),
        ),
        margin = dict(l=12, r=12, t=12, b=12),
    )
    if height:
        updates["height"] = height
    fig.update_layout(**updates)
    return fig


def brand_color_sequence():
    """Return the brand-consistent color list for Plotly charts."""
    return CHART_COLORS


# ─────────────────────────────────────────────────────────────────────────────
# REUSABLE PYTHON COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────

def section_header(icon: str, title: str, subtitle: str = "", divider: bool = True) -> None:
    """
    Render a styled section header with icon, title, and optional subtitle.

    Args:
        icon:     emoji or text icon
        title:    section heading text
        subtitle: optional right-aligned meta text (e.g., "7 items")
        divider:  whether to show a border-bottom line
    """
    sub_html = f'<span class="subtitle">{subtitle}</span>' if subtitle else ""
    div_style = "border-bottom: 1px solid #E2DDD8; padding-bottom: 0.5rem; margin-bottom: 0.75rem;" if divider else ""
    st.markdown(
        f"""
        <div class="mn-section-header" style="{div_style}">
          <span class="icon">{icon}</span>
          <span class="title">{title}</span>
          {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def badge(text: str, style: str = "amber") -> str:
    """
    Return an inline HTML badge string.

    Args:
        text:  label text
        style: 'amber' | 'teal' | 'green' | 'red' | 'gray'

    Usage:
        st.markdown(f"Status: {badge('Active', 'green')}", unsafe_allow_html=True)
    """
    cls = f"mn-badge mn-badge-{style}"
    return f'<span class="{cls}">{text}</span>'


def card_start(accent_color: str = "accent") -> None:
    """
    Open a styled card container. Must be paired with card_end().
    accent_color: 'accent' | 'teal' | 'success' | 'danger'

    Usage:
        card_start('teal')
        st.write("Card content here")
        card_end()
    """
    cls_map = {
        "accent":  "mn-card mn-card-accent",
        "teal":    "mn-card mn-card-teal",
        "success": "mn-card mn-card-success",
        "danger":  "mn-card mn-card-danger",
        "default": "mn-card",
    }
    cls = cls_map.get(accent_color, "mn-card")
    st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)


def card_end() -> None:
    """Close a card container opened by card_start()."""
    st.markdown("</div>", unsafe_allow_html=True)


def kpi_row(metrics: list[dict]) -> None:
    """
    Render a row of KPI cards using st.metric in styled columns.

    Args:
        metrics: list of dicts, each with:
                 { "label": str, "value": str, "delta": str (optional),
                   "help": str (optional) }

    Usage:
        kpi_row([
            {"label": "Revenue", "value": "₹1.2L", "delta": "+12%"},
            {"label": "Units",   "value": "3,421"},
        ])
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            st.metric(
                label=m.get("label", ""),
                value=m.get("value", "—"),
                delta=m.get("delta"),
                help=m.get("help"),
            )


def page_header(title: str, subtitle: str = "", role: str = "") -> None:
    """
    Render the top-of-page header with app brand + optional role badge.

    Usage:
        page_header("Trend Analytics", "Revenue & volume over time", role=role)
    """
    role_html = ""
    if role:
        role_label = "Admin" if role == "admin" else "Viewer"
        role_html = f'<span class="role-badge">👤 {role_label}</span>'

    st.markdown(
        f"""
        <div style="display:flex; align-items:flex-start;
                    justify-content:space-between; margin-bottom:1.25rem;
                    padding-bottom:1rem; border-bottom:1px solid #E2DDD8;">
          <div>
            <div style="font-family:'DM Serif Display',Georgia,serif;
                        font-size:1.6rem; color:#1C1917;
                        letter-spacing:-0.02em; line-height:1.2;
                        margin-bottom:0.2rem;">{title}</div>
            <div style="font-size:0.78rem; color:#A89E95;">{subtitle}</div>
          </div>
          <div>{role_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def empty_state(icon: str, title: str, body: str) -> None:
    """
    Render a centered empty-state illustration with message.

    Usage:
        empty_state("📭", "No data yet", "Upload sales data to get started.")
    """
    st.markdown(
        f"""
        <div style="text-align:center; padding:3rem 1rem; color:#A89E95;">
          <div style="font-size:2.5rem; margin-bottom:0.75rem;">{icon}</div>
          <div style="font-family:'DM Sans'; font-weight:600;
                      font-size:1rem; color:#6B5F55;
                      margin-bottom:0.4rem;">{title}</div>
          <div style="font-size:0.82rem; max-width:340px; margin:0 auto;">
            {body}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def active_filter_pill(start, end, channels=None, products=None) -> None:
    """
    Render a compact 'active filters' pill row — used inside each tab
    to show what global filters are in effect.

    Args:
        start, end:  datetime.date objects
        channels:    list[str] | None
        products:    list[str] | None
    """
    chan_text = ", ".join(channels) if channels else "All channels"
    prod_text = (", ".join(products[:2]) + (f" +{len(products)-2}" if len(products) > 2 else "")
                 if products else "All products")

    st.markdown(
        f"""
        <div style="display:flex; gap:0.5rem; flex-wrap:wrap;
                    margin-bottom:0.75rem; align-items:center;">
          <span style="font-size:0.65rem; font-weight:700;
                       color:#A89E95; text-transform:uppercase;
                       letter-spacing:0.1em;">Filters:</span>
          <span class="mn-badge mn-badge-amber">📅 {start} → {end}</span>
          <span class="mn-badge mn-badge-teal">🔀 {chan_text}</span>
          <span class="mn-badge mn-badge-gray">📦 {prod_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
