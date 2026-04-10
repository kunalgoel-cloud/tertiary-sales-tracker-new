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
    "bg":        "#0F1117",
    "surface":   "#1A1D27",
    "surface2":  "#20243A",
    "border":    "#2A2D3A",
    "accent":    "#F5A623",
    "accent2":   "#4ECDC4",
    "text1":     "#F0F2F5",
    "text2":     "#9BA3B0",
    "text3":     "#5C6370",
    "success":   "#2ECC71",
    "warning":   "#F39C12",
    "danger":    "#E74C3C",
    "info":      "#3498DB",
}

# Plotly color sequences — cohesive with brand
CHART_COLORS = [
    "#F5A623", "#4ECDC4", "#A78BFA", "#F87171",
    "#34D399", "#60A5FA", "#FBBF24", "#F472B6",
    "#818CF8", "#2DD4BF",
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
/* ═══════════════════════════════════════════════════
   DESIGN TOKENS
═══════════════════════════════════════════════════ */
:root {
  --bg:         #0F1117;
  --surface:    #1A1D27;
  --surface2:   #20243A;
  --border:     #2A2D3A;
  --accent:     #F5A623;
  --accent2:    #4ECDC4;
  --text1:      #F0F2F5;
  --text2:      #9BA3B0;
  --text3:      #5C6370;
  --success:    #2ECC71;
  --warning:    #F39C12;
  --danger:     #E74C3C;
  --info:       #3498DB;
  --radius-sm:  6px;
  --radius-md:  10px;
  --radius-lg:  16px;
  --shadow-sm:  0 1px 3px rgba(0,0,0,.4);
  --shadow-md:  0 4px 16px rgba(0,0,0,.5);
  --shadow-lg:  0 8px 32px rgba(0,0,0,.6);
  --transition: 0.18s ease;
  --font-ui:    'DM Sans', system-ui, sans-serif;
  --font-disp:  'DM Serif Display', Georgia, serif;
  --font-mono:  'JetBrains Mono', 'Fira Code', monospace;
}

/* ═══════════════════════════════════════════════════
   GLOBAL RESETS & BODY
═══════════════════════════════════════════════════ */
*, *::before, *::after { box-sizing: border-box; }

html, body, .stApp {
  background-color: var(--bg) !important;
  color: var(--text1) !important;
  font-family: var(--font-ui) !important;
  font-size: 14px;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}

/* Main content area padding */
.main .block-container {
  padding: 1.5rem 2rem 3rem !important;
  max-width: 1440px !important;
}

/* ═══════════════════════════════════════════════════
   TYPOGRAPHY HIERARCHY
═══════════════════════════════════════════════════ */
h1, .stApp h1 {
  font-family: var(--font-disp) !important;
  font-size: 2.2rem !important;
  font-weight: 400 !important;
  color: var(--text1) !important;
  letter-spacing: -0.02em;
  line-height: 1.2;
  margin-bottom: 0.25rem !important;
}

h2, .stApp h2 {
  font-family: var(--font-ui) !important;
  font-size: 1.4rem !important;
  font-weight: 600 !important;
  color: var(--text1) !important;
  letter-spacing: -0.01em;
}

h3, .stApp h3 {
  font-family: var(--font-ui) !important;
  font-size: 1.1rem !important;
  font-weight: 600 !important;
  color: var(--text1) !important;
}

h4, .stApp h4 {
  font-family: var(--font-ui) !important;
  font-size: 0.9rem !important;
  font-weight: 600 !important;
  color: var(--text2) !important;
  text-transform: uppercase;
  letter-spacing: 0.07em;
}

p, .stMarkdown p {
  color: var(--text2) !important;
  font-size: 0.88rem;
  line-height: 1.65;
}

/* Inline code */
code {
  font-family: var(--font-mono) !important;
  background: var(--surface2) !important;
  color: var(--accent) !important;
  padding: 0.1em 0.4em !important;
  border-radius: var(--radius-sm) !important;
  font-size: 0.82em !important;
}

/* ═══════════════════════════════════════════════════
   SIDEBAR
═══════════════════════════════════════════════════ */
[data-testid="stSidebar"] {
  background: var(--surface) !important;
  border-right: 1px solid var(--border) !important;
}

[data-testid="stSidebar"] > div:first-child {
  padding: 1.5rem 1rem !important;
}

[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
  font-family: var(--font-ui) !important;
}

/* Sidebar section headers */
[data-testid="stSidebar"] .stMarkdown h2 {
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  color: var(--text3) !important;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  margin: 1.5rem 0 0.5rem !important;
  padding-bottom: 0.4rem;
  border-bottom: 1px solid var(--border);
}

/* ═══════════════════════════════════════════════════
   TABS
═══════════════════════════════════════════════════ */
/* Tab strip container */
[data-testid="stTabs"] > div:first-child {
  background: transparent !important;
  border-bottom: 1px solid var(--border) !important;
  gap: 0 !important;
  padding: 0 !important;
  overflow-x: auto;
  -ms-overflow-style: none;
  scrollbar-width: none;
}
[data-testid="stTabs"] > div:first-child::-webkit-scrollbar { display: none; }

/* Individual tab buttons */
[data-testid="stTabs"] button[role="tab"] {
  font-family: var(--font-ui) !important;
  font-size: 0.82rem !important;
  font-weight: 500 !important;
  color: var(--text2) !important;
  background: transparent !important;
  border: none !important;
  border-bottom: 2px solid transparent !important;
  border-radius: 0 !important;
  padding: 0.65rem 1.1rem !important;
  margin: 0 !important;
  cursor: pointer;
  transition: color var(--transition), border-color var(--transition) !important;
  white-space: nowrap;
  letter-spacing: 0.01em;
}

[data-testid="stTabs"] button[role="tab"]:hover {
  color: var(--text1) !important;
  background: rgba(245, 166, 35, 0.06) !important;
}

[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
  color: var(--accent) !important;
  border-bottom-color: var(--accent) !important;
  background: transparent !important;
  font-weight: 600 !important;
}

/* Tab content panels */
[data-testid="stTabsContent"] {
  padding-top: 1.5rem !important;
  animation: fadeInUp 0.22s ease both;
}

@keyframes fadeInUp {
  from { opacity: 0; transform: translateY(8px); }
  to   { opacity: 1; transform: translateY(0); }
}

/* ═══════════════════════════════════════════════════
   BUTTONS
═══════════════════════════════════════════════════ */
.stButton > button {
  font-family: var(--font-ui) !important;
  font-size: 0.82rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.02em;
  color: var(--bg) !important;
  background: var(--accent) !important;
  border: none !important;
  border-radius: var(--radius-md) !important;
  padding: 0.5rem 1.1rem !important;
  height: auto !important;
  min-height: 36px !important;
  transition: all var(--transition) !important;
  box-shadow: 0 2px 8px rgba(245, 166, 35, 0.25) !important;
  cursor: pointer;
}

.stButton > button:hover {
  background: #FFB833 !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 16px rgba(245, 166, 35, 0.4) !important;
}

.stButton > button:active {
  transform: translateY(0) !important;
  box-shadow: 0 1px 4px rgba(245, 166, 35, 0.25) !important;
}

/* Secondary / danger buttons — detect by emoji/text in aria-label */
.stButton > button[kind="secondary"],
.stButton > button:has([data-testid*="delete"]) {
  background: var(--surface2) !important;
  color: var(--text1) !important;
  border: 1px solid var(--border) !important;
  box-shadow: none !important;
}
.stButton > button[kind="secondary"]:hover {
  background: var(--danger) !important;
  color: #fff !important;
  border-color: var(--danger) !important;
}

/* Form submit buttons */
.stFormSubmitButton > button {
  background: var(--accent2) !important;
  color: var(--bg) !important;
  box-shadow: 0 2px 8px rgba(78, 205, 196, 0.25) !important;
}
.stFormSubmitButton > button:hover {
  background: #5EDDD4 !important;
  box-shadow: 0 4px 16px rgba(78, 205, 196, 0.4) !important;
}

/* ═══════════════════════════════════════════════════
   INPUTS, SELECTS, TEXTAREAS
═══════════════════════════════════════════════════ */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stTextArea > div > div > textarea,
.stSelectbox > div > div > div,
.stMultiSelect > div > div > div {
  font-family: var(--font-ui) !important;
  font-size: 0.85rem !important;
  background: var(--surface) !important;
  color: var(--text1) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  transition: border-color var(--transition) !important;
}

.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(245, 166, 35, 0.15) !important;
  outline: none !important;
}

/* Multiselect tags */
.stMultiSelect [data-baseweb="tag"] {
  background: rgba(245, 166, 35, 0.18) !important;
  color: var(--accent) !important;
  border-radius: var(--radius-sm) !important;
  font-size: 0.75rem !important;
  font-weight: 600 !important;
}

/* Dropdown options */
[data-baseweb="select"] [data-baseweb="menu"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  box-shadow: var(--shadow-lg) !important;
}
[data-baseweb="select"] [role="option"]:hover {
  background: var(--surface2) !important;
}
[data-baseweb="select"] [aria-selected="true"] {
  background: rgba(245, 166, 35, 0.1) !important;
  color: var(--accent) !important;
}

/* Labels */
label, .stSelectbox label, .stMultiSelect label,
.stTextInput label, .stNumberInput label,
.stDateInput label, .stRadio label {
  font-family: var(--font-ui) !important;
  font-size: 0.75rem !important;
  font-weight: 600 !important;
  color: var(--text2) !important;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

/* Radio buttons */
.stRadio [data-testid="stMarkdownContainer"] p { color: var(--text2) !important; }
.stRadio div[role="radiogroup"] label:has(input:checked) { color: var(--accent) !important; }

/* Checkbox */
.stCheckbox > label > span:first-child {
  border-color: var(--border) !important;
  background: var(--surface) !important;
}
.stCheckbox > label > span:first-child:has(input:checked) {
  background: var(--accent) !important;
  border-color: var(--accent) !important;
}

/* Date inputs */
.stDateInput > div > div > input {
  background: var(--surface) !important;
  color: var(--text1) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
}

/* File uploader */
[data-testid="stFileUploader"] {
  background: var(--surface) !important;
  border: 1.5px dashed var(--border) !important;
  border-radius: var(--radius-lg) !important;
  transition: border-color var(--transition) !important;
}
[data-testid="stFileUploader"]:hover {
  border-color: var(--accent) !important;
  background: rgba(245, 166, 35, 0.04) !important;
}

/* ═══════════════════════════════════════════════════
   METRICS
═══════════════════════════════════════════════════ */
[data-testid="stMetric"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-lg) !important;
  padding: 1.1rem 1.25rem !important;
  transition: transform var(--transition), box-shadow var(--transition) !important;
  position: relative;
  overflow: hidden;
}

[data-testid="stMetric"]::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 2px;
  background: linear-gradient(90deg, var(--accent), var(--accent2));
  opacity: 0;
  transition: opacity var(--transition);
}

[data-testid="stMetric"]:hover {
  transform: translateY(-2px) !important;
  box-shadow: var(--shadow-md) !important;
}
[data-testid="stMetric"]:hover::before { opacity: 1; }

[data-testid="stMetric"] label {
  font-size: 0.7rem !important;
  font-weight: 700 !important;
  color: var(--text3) !important;
  text-transform: uppercase !important;
  letter-spacing: 0.1em !important;
}

[data-testid="stMetricValue"] {
  font-family: var(--font-mono) !important;
  font-size: 1.6rem !important;
  font-weight: 600 !important;
  color: var(--text1) !important;
  line-height: 1.2 !important;
}

[data-testid="stMetricDelta"] {
  font-family: var(--font-mono) !important;
  font-size: 0.78rem !important;
}

/* ═══════════════════════════════════════════════════
   ALERTS / BANNERS
═══════════════════════════════════════════════════ */
[data-testid="stAlert"],
.stInfo, .stSuccess, .stWarning, .stError,
div[class*="stAlert"] {
  border-radius: var(--radius-md) !important;
  border: 1px solid !important;
  font-size: 0.83rem !important;
  font-family: var(--font-ui) !important;
  padding: 0.7rem 1rem !important;
}

/* Info */
[data-testid="stAlert"][data-baseweb="notification"],
div[class*="info"] {
  background: rgba(52, 152, 219, 0.08) !important;
  border-color: rgba(52, 152, 219, 0.3) !important;
  color: #74B9E7 !important;
}

/* Success */
div[class*="success"] {
  background: rgba(46, 204, 113, 0.08) !important;
  border-color: rgba(46, 204, 113, 0.3) !important;
  color: #5DCEA1 !important;
}

/* Warning */
div[class*="warning"] {
  background: rgba(243, 156, 18, 0.08) !important;
  border-color: rgba(243, 156, 18, 0.3) !important;
  color: #F5B942 !important;
}

/* Error */
div[class*="error"] {
  background: rgba(231, 76, 60, 0.08) !important;
  border-color: rgba(231, 76, 60, 0.3) !important;
  color: #E87C72 !important;
}

/* ═══════════════════════════════════════════════════
   DATAFRAMES / TABLES
═══════════════════════════════════════════════════ */
[data-testid="stDataFrame"] {
  border-radius: var(--radius-lg) !important;
  overflow: hidden !important;
  border: 1px solid var(--border) !important;
}

[data-testid="stDataFrame"] table {
  font-family: var(--font-mono) !important;
  font-size: 0.78rem !important;
}

[data-testid="stDataFrame"] thead tr th {
  background: var(--surface2) !important;
  color: var(--text3) !important;
  font-family: var(--font-ui) !important;
  font-size: 0.68rem !important;
  font-weight: 700 !important;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  border-bottom: 1px solid var(--border) !important;
}

[data-testid="stDataFrame"] tbody tr:hover td {
  background: var(--surface2) !important;
}

[data-testid="stDataFrame"] tbody tr td {
  border-bottom: 1px solid rgba(42, 45, 58, 0.5) !important;
  color: var(--text1) !important;
}

/* ═══════════════════════════════════════════════════
   EXPANDERS
═══════════════════════════════════════════════════ */
[data-testid="stExpander"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  margin-bottom: 0.5rem !important;
  transition: box-shadow var(--transition) !important;
}

[data-testid="stExpander"]:hover {
  box-shadow: var(--shadow-sm) !important;
  border-color: rgba(245, 166, 35, 0.3) !important;
}

[data-testid="stExpander"] summary {
  font-family: var(--font-ui) !important;
  font-size: 0.85rem !important;
  font-weight: 600 !important;
  color: var(--text1) !important;
  padding: 0.8rem 1rem !important;
}

/* ═══════════════════════════════════════════════════
   GLOBAL FILTER BAR (from global_filters.py)
═══════════════════════════════════════════════════ */
.gf-bar {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 1rem 1.25rem 0.75rem;
  margin-bottom: 1.25rem;
  position: relative;
  animation: slideDown 0.25s ease both;
}

@keyframes slideDown {
  from { opacity: 0; transform: translateY(-6px); }
  to   { opacity: 1; transform: translateY(0); }
}

.gf-bar::before {
  content: '';
  position: absolute;
  top: 0; left: 1.5rem; right: 1.5rem;
  height: 1px;
  background: linear-gradient(90deg, transparent, var(--accent), var(--accent2), transparent);
}

.gf-label {
  font-size: 0.65rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--text3);
  margin-bottom: 0.5rem;
}

/* ═══════════════════════════════════════════════════
   CARD COMPONENTS (inject via Python helper)
═══════════════════════════════════════════════════ */
.mn-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 1.25rem;
  margin-bottom: 1rem;
  transition: box-shadow var(--transition), border-color var(--transition);
  position: relative;
  overflow: hidden;
}

.mn-card:hover {
  box-shadow: var(--shadow-md);
  border-color: rgba(245, 166, 35, 0.2);
}

.mn-card-accent::before {
  content: '';
  position: absolute;
  left: 0; top: 0; bottom: 0;
  width: 3px;
  background: var(--accent);
  border-radius: var(--radius-sm) 0 0 var(--radius-sm);
}

.mn-card-teal::before { background: var(--accent2); }
.mn-card-success::before { background: var(--success); }
.mn-card-danger::before { background: var(--danger); }

/* Section header component */
.mn-section-header {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  margin: 1.5rem 0 0.75rem;
  padding-bottom: 0.6rem;
  border-bottom: 1px solid var(--border);
}

.mn-section-header .icon {
  font-size: 1rem;
  line-height: 1;
}

.mn-section-header .title {
  font-family: var(--font-ui);
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--text1);
  letter-spacing: -0.01em;
}

.mn-section-header .subtitle {
  font-size: 0.75rem;
  color: var(--text3);
  margin-left: auto;
}

/* Badge component */
.mn-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.3em;
  padding: 0.18em 0.6em;
  border-radius: 100px;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  font-family: var(--font-ui);
}
.mn-badge-amber  { background: rgba(245,166,35,0.15); color: var(--accent); }
.mn-badge-teal   { background: rgba(78,205,196,0.15); color: var(--accent2); }
.mn-badge-green  { background: rgba(46,204,113,0.15); color: var(--success); }
.mn-badge-red    { background: rgba(231,76,60,0.15);  color: var(--danger); }
.mn-badge-gray   { background: rgba(155,163,176,0.15); color: var(--text2); }

/* ═══════════════════════════════════════════════════
   DIVIDERS
═══════════════════════════════════════════════════ */
hr, [data-testid="stDivider"] > hr {
  border: none !important;
  border-top: 1px solid var(--border) !important;
  margin: 1.25rem 0 !important;
  opacity: 1 !important;
}

/* ═══════════════════════════════════════════════════
   SPINNER
═══════════════════════════════════════════════════ */
[data-testid="stSpinner"] svg { color: var(--accent) !important; }

/* ═══════════════════════════════════════════════════
   PLOTLY CHART CONTAINERS
═══════════════════════════════════════════════════ */
[data-testid="stPlotlyChart"] {
  border-radius: var(--radius-lg) !important;
  overflow: hidden !important;
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  padding: 0.5rem !important;
  transition: box-shadow var(--transition) !important;
}
[data-testid="stPlotlyChart"]:hover {
  box-shadow: var(--shadow-md) !important;
}

/* ═══════════════════════════════════════════════════
   SCROLLBAR
═══════════════════════════════════════════════════ */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--bg); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--text3); }

/* ═══════════════════════════════════════════════════
   LOGIN PAGE
═══════════════════════════════════════════════════ */
.login-wrapper {
  max-width: 420px;
  margin: 4rem auto;
  padding: 2.5rem;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-lg);
  animation: fadeInUp 0.35s ease both;
}

.login-logo {
  font-family: var(--font-disp);
  font-size: 1.8rem;
  color: var(--accent);
  margin-bottom: 0.25rem;
}

.login-tagline {
  font-size: 0.78rem;
  color: var(--text3);
  margin-bottom: 2rem;
  letter-spacing: 0.04em;
}

/* ═══════════════════════════════════════════════════
   PROGRESS / LOADING STATES
═══════════════════════════════════════════════════ */
@keyframes shimmer {
  0%   { background-position: -200% center; }
  100% { background-position:  200% center; }
}

.mn-loading {
  background: linear-gradient(90deg,
    var(--surface) 25%,
    var(--surface2) 50%,
    var(--surface) 75%
  );
  background-size: 200% auto;
  animation: shimmer 1.5s linear infinite;
  border-radius: var(--radius-md);
  height: 1.2rem;
}

/* ═══════════════════════════════════════════════════
   CAPTION / HELPER TEXT
═══════════════════════════════════════════════════ */
[data-testid="stCaptionContainer"] p,
.stCaption p {
  font-size: 0.75rem !important;
  color: var(--text3) !important;
  font-style: italic;
}

/* ═══════════════════════════════════════════════════
   SIDEBAR LOGOUT / ROLE HEADER
═══════════════════════════════════════════════════ */
.role-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  background: rgba(245, 166, 35, 0.1);
  border: 1px solid rgba(245, 166, 35, 0.25);
  border-radius: var(--radius-md);
  padding: 0.35rem 0.75rem;
  font-size: 0.75rem;
  font-weight: 700;
  color: var(--accent);
  letter-spacing: 0.06em;
  margin-bottom: 0.5rem;
}

/* ═══════════════════════════════════════════════════
   TOAST NOTIFICATIONS
═══════════════════════════════════════════════════ */
[data-testid="stToast"] {
  background: var(--surface) !important;
  border: 1px solid var(--border) !important;
  border-radius: var(--radius-md) !important;
  color: var(--text1) !important;
  font-family: var(--font-ui) !important;
  font-size: 0.83rem !important;
  box-shadow: var(--shadow-lg) !important;
}

/* ═══════════════════════════════════════════════════
   SELECT BOXES DROPDOWN ARROW COLOR
═══════════════════════════════════════════════════ */
[data-baseweb="select"] svg { color: var(--text3) !important; }

/* ═══════════════════════════════════════════════════
   FOCUS RING (ACCESSIBILITY)
═══════════════════════════════════════════════════ */
*:focus-visible {
  outline: 2px solid var(--accent) !important;
  outline-offset: 2px !important;
}
</style>
""",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PLOTLY CHART THEME
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
                             color="#9BA3B0", size=11),
        title_font    = dict(family="DM Sans", color="#F0F2F5", size=14),
        legend        = dict(
            bgcolor     = "rgba(26,29,39,0.8)",
            bordercolor = "#2A2D3A",
            borderwidth = 1,
            font        = dict(size=11, color="#9BA3B0"),
        ),
        xaxis = dict(
            gridcolor    = "#1E2130",
            linecolor    = "#2A2D3A",
            tickcolor    = "#2A2D3A",
            tickfont     = dict(color="#5C6370", size=10),
            title_font   = dict(color="#9BA3B0", size=11),
            zeroline     = False,
        ),
        yaxis = dict(
            gridcolor    = "#1E2130",
            linecolor    = "rgba(0,0,0,0)",
            tickcolor    = "#2A2D3A",
            tickfont     = dict(color="#5C6370", size=10),
            title_font   = dict(color="#9BA3B0", size=11),
            zeroline     = False,
        ),
        hoverlabel = dict(
            bgcolor     = "#20243A",
            bordercolor = "#2A2D3A",
            font        = dict(family="DM Sans", color="#F0F2F5", size=12),
        ),
        margin = dict(l=12, r=12, t=36, b=12),
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
    div_style = "border-bottom: 1px solid #2A2D3A; padding-bottom: 0.5rem; margin-bottom: 0.75rem;" if divider else ""
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
                    padding-bottom:1rem; border-bottom:1px solid #2A2D3A;">
          <div>
            <div style="font-family:'DM Serif Display',Georgia,serif;
                        font-size:1.6rem; color:#F0F2F5;
                        letter-spacing:-0.02em; line-height:1.2;
                        margin-bottom:0.2rem;">{title}</div>
            <div style="font-size:0.78rem; color:#5C6370;">{subtitle}</div>
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
        <div style="text-align:center; padding:3rem 1rem; color:#5C6370;">
          <div style="font-size:2.5rem; margin-bottom:0.75rem;">{icon}</div>
          <div style="font-family:'DM Sans'; font-weight:600;
                      font-size:1rem; color:#9BA3B0;
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
                    margin-bottom:1rem; align-items:center;">
          <span style="font-size:0.65rem; font-weight:700;
                       color:#5C6370; text-transform:uppercase;
                       letter-spacing:0.1em;">Filters:</span>
          <span class="mn-badge mn-badge-amber">📅 {start} → {end}</span>
          <span class="mn-badge mn-badge-teal">🔀 {chan_text}</span>
          <span class="mn-badge mn-badge-gray">📦 {prod_text}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
