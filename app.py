"""
Cortex Search — Governed Natural Language to SQL on Snowflake
Run with:  streamlit run app.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import time
import pandas as pd
import streamlit as st

from agents.query_understanding import understand_query
from agents.sql_generator        import generate_sql
from agents.sql_validator        import validate_sql, format_validation_badge
from agents.kb_retriever         import retrieve_kb_context
from agents.answer_synthesizer   import synthesize_answer
from services.snowflake_client   import execute_query, DEFAULT_TENANT_ID
from services.logging_service    import QueryLog, get_recent_logs, get_log_stats
from config.schema_context       import ALLOWED_VIEWS


st.set_page_config(
    page_title="Cortex Search",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
#  DESIGN SYSTEM
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Reset ─────────────────────────────────────────── */
html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
#MainMenu, footer, header   { visibility: hidden; }

/* ── Page background ────────────────────────────────── */
.stApp                      { background: #F4F6FA !important; }
.block-container            { padding: 0 2rem 3rem 2rem !important; max-width: 1200px !important; background: transparent !important; }

/* ── Sidebar ─────────────────────────────────────────── */
[data-testid="stSidebar"]               { background: #FFFFFF !important; border-right: 1px solid #E3E8EF !important; }
[data-testid="stSidebar"] *             { color: #4B5563 !important; }
[data-testid="stSidebar"] hr            { border-color: #F1F3F7 !important; margin: 0.5rem 0 !important; }
[data-testid="stSidebar"] [data-testid="stMetric"]      { background: #F8FAFC; border: 1px solid #E3E8EF; border-radius: 8px; padding: 0.5rem 0.7rem; }
[data-testid="stSidebar"] [data-testid="stMetricValue"] { color: #2D6BE4 !important; font-size: 1.2rem !important; font-weight: 700 !important; }
[data-testid="stSidebar"] [data-testid="stMetricLabel"] { color: #9CA3AF !important; font-size: 0.68rem !important; }

/* ── Sidebar nav items ─────────────────────────────── */
.snav-logo  { display:flex; align-items:center; gap:10px; padding: 1.25rem 0 1.5rem 0; border-bottom: 1px solid #F1F3F7; margin-bottom:0.75rem; }
.snav-logo-icon { width:32px;height:32px; background:linear-gradient(135deg,#2D6BE4,#1A4FBB); border-radius:8px; display:flex;align-items:center;justify-content:center; font-size:0.95rem; flex-shrink:0; }
.snav-logo-name { font-size:0.9rem; font-weight:700; color:#111827 !important; letter-spacing:-0.01em; }
.snav-logo-sub  { font-size:0.65rem; color:#9CA3AF !important; margin-top:1px; }

.snav-section { font-size:0.62rem; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; color:#9CA3AF; margin: 1.25rem 0 0.4rem 0.5rem; }
.snav-item    { display:flex; align-items:center; gap:0.55rem; padding:0.48rem 0.75rem; border-radius:7px; font-size:0.8rem; color:#4B5563; margin-bottom:2px; cursor:pointer; border-left:3px solid transparent; transition:all 0.1s; }
.snav-item:hover  { background:#F8FAFC; color:#111827; }
.snav-item.active { background:#EEF3FF; color:#2D6BE4 !important; font-weight:600; border-left-color:#2D6BE4; }

.snav-view  { display:flex; align-items:center; gap:0.5rem; padding:0.3rem 0.6rem; font-size:0.75rem; color:#6B7280; }
.snav-dot   { width:7px;height:7px;border-radius:50%;background:#12B76A;flex-shrink:0; }
.snav-ctrl  { display:flex; align-items:center; gap:0.5rem; padding:0.25rem 0.6rem; font-size:0.75rem; color:#9CA3AF; }

.snav-hist  { background:#F8FAFC; border:1px solid #E3E8EF; border-radius:7px; padding:0.55rem 0.75rem; margin-bottom:0.35rem; cursor:pointer; }
.snav-hist:hover { border-color:#2D6BE4; }
.snav-hist-q    { font-size:0.76rem; color:#374151; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-weight:500; }
.snav-hist-meta { font-size:0.67rem; color:#9CA3AF; margin-top:2px; }

.snav-user  { display:flex; align-items:center; gap:0.6rem; padding:0.75rem 0.5rem; border-top:1px solid #F1F3F7; margin-top:1rem; }
.snav-avatar{ width:30px;height:30px;border-radius:50%;background:linear-gradient(135deg,#2D6BE4,#1A4FBB);display:flex;align-items:center;justify-content:center;font-size:0.75rem;color:white;font-weight:600;flex-shrink:0; }
.snav-uname { font-size:0.78rem; font-weight:600; color:#111827 !important; }
.snav-utenant{ font-size:0.67rem; color:#9CA3AF !important; }
.snav-plan  { background:#EEF3FF; color:#2D6BE4; border-radius:4px; padding:1px 6px; font-size:0.62rem; font-weight:600; margin-left:auto; }

/* ── Page header ─────────────────────────────────────── */
.page-header { display:flex; align-items:center; justify-content:space-between; padding:1.5rem 0 1.25rem 0; border-bottom:1px solid #E3E8EF; margin-bottom:1.75rem; }
.ph-left {}
.ph-breadcrumb { font-size:0.72rem; color:#9CA3AF; margin-bottom:0.3rem; display:flex; align-items:center; gap:0.35rem; }
.ph-breadcrumb span { color:#6B7280; }
.ph-title { font-size:1.4rem; font-weight:700; color:#111827; letter-spacing:-0.02em; margin:0; }
.ph-sub   { font-size:0.82rem; color:#6B7280; margin-top:0.25rem; }
.ph-actions { display:flex; align-items:center; gap:0.75rem; }
.ph-btn-primary { background:#2D6BE4; color:white; border:none; border-radius:8px; padding:0.5rem 1.1rem; font-size:0.82rem; font-weight:600; cursor:pointer; display:flex;align-items:center;gap:0.35rem; }
.ph-btn-secondary { background:white; color:#374151; border:1px solid #E3E8EF; border-radius:8px; padding:0.5rem 1rem; font-size:0.82rem; font-weight:500; cursor:pointer; }
.ph-live-badge { display:flex;align-items:center;gap:0.35rem;background:#F0FDF4;border:1px solid #BBF7D0;color:#16A34A;border-radius:6px;padding:0.3rem 0.75rem;font-size:0.73rem;font-weight:600; }
.ph-live-dot   { width:6px;height:6px;border-radius:50%;background:#16A34A;animation:pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }

/* ── Search card — style Streamlit's own form wrapper ── */
div[data-testid="stForm"] {
    background: white !important;
    border: 1px solid #E3E8EF !important;
    border-radius: 12px !important;
    padding: 1.25rem 1.5rem 1.1rem 1.5rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05) !important;
    margin-bottom: 0.75rem !important;
}
.sc-label { font-size:0.7rem; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; color:#9CA3AF; margin-bottom:0.4rem; }

/* ── Streamlit input override (light) ─────────────────── */
.stTextInput > label { display:none !important; }
.stTextInput > div > div > input {
    background: #FAFBFC !important;
    border: 1.5px solid #E3E8EF !important;
    border-radius: 8px !important;
    padding: 0.7rem 1rem !important;
    font-size: 0.9rem !important;
    color: #111827 !important;
}
.stTextInput > div > div > input::placeholder { color:#9CA3AF !important; }
.stTextInput > div > div > input:focus { border-color:#2D6BE4 !important; box-shadow:0 0 0 3px rgba(45,107,228,0.1) !important; }
div[data-testid="stForm"] { border:none !important; padding:0 !important; background:transparent !important; }

/* ── Buttons ─────────────────────────────────────────── */
.stButton > button[kind="primary"] {
    background: #2D6BE4 !important; border:none !important; border-radius:8px !important;
    font-weight:600 !important; font-size:0.85rem !important; color:white !important;
    padding:0.6rem 1.4rem !important;
}
.stButton > button[kind="primary"]:hover { background:#1A4FBB !important; }
.stButton > button:not([kind="primary"]) {
    background:white !important; border:1.5px solid #E3E8EF !important;
    border-radius:8px !important; color:#374151 !important; font-size:0.82rem !important;
}
.stButton > button:not([kind="primary"]):hover { border-color:#2D6BE4 !important; color:#2D6BE4 !important; }

/* ── Example chips ────────────────────────────────────── */
.chips-label { font-size:0.7rem; font-weight:500; color:#9CA3AF; margin:0.75rem 0 0.4rem 0; }

/* ── Stepper ──────────────────────────────────────────── */
.stepper {
    display:flex; align-items:center;
    background:white; border:1px solid #E3E8EF; border-radius:10px;
    padding:0.75rem 1.25rem; margin:1.25rem 0; overflow-x:auto;
    box-shadow:0 1px 2px rgba(0,0,0,0.04);
}
.s-step   { display:flex; align-items:center; gap:0.4rem; flex-shrink:0; }
.s-dot    { width:24px;height:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:0.68rem;font-weight:700;flex-shrink:0; }
.s-dot.done    { background:#D1FAE5; color:#065F46; }
.s-dot.active  { background:#DBEAFE; color:#1D4ED8; }
.s-dot.pending { background:#F3F4F6; color:#D1D5DB; }
.s-label       { font-size:0.73rem; color:#6B7280; font-weight:500; white-space:nowrap; }
.s-label.active{ color:#1D4ED8; font-weight:600; }
.s-arrow       { color:#D1D5DB; margin:0 0.6rem; font-size:0.85rem; flex-shrink:0; }

/* ── Answer card ──────────────────────────────────────── */
.answer-card {
    background:white; border:1px solid #E3E8EF; border-radius:12px;
    border-left:4px solid #2D6BE4;
    padding:1.5rem; margin:1.5rem 0 1rem 0;
    box-shadow:0 1px 4px rgba(0,0,0,0.06);
}
.ac-top    { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:0.85rem; }
.ac-label  { font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; color:#9CA3AF; }
.ac-badges { display:flex; gap:0.4rem; flex-wrap:wrap; }
.b-conf-high   { background:#D1FAE5; color:#065F46; border-radius:5px; padding:2px 9px; font-size:0.7rem; font-weight:600; }
.b-conf-med    { background:#FEF3C7; color:#92400E; border-radius:5px; padding:2px 9px; font-size:0.7rem; font-weight:600; }
.b-conf-low    { background:#FEE2E2; color:#991B1B; border-radius:5px; padding:2px 9px; font-size:0.7rem; font-weight:600; }
.b-pass        { background:#D1FAE5; color:#065F46; border-radius:5px; padding:2px 9px; font-size:0.7rem; font-weight:600; }
.b-fail        { background:#FEE2E2; color:#991B1B; border-radius:5px; padding:2px 9px; font-size:0.7rem; font-weight:600; }
.ac-answer { font-size:0.97rem; line-height:1.7; color:#1F2937; }
.ac-meta   { display:flex; gap:1.25rem; margin-top:1rem; padding-top:0.85rem; border-top:1px solid #F3F4F6; flex-wrap:wrap; }
.ac-meta-item { font-size:0.72rem; color:#9CA3AF; display:flex; align-items:center; gap:0.3rem; }

/* ── Detail cards ─────────────────────────────────────── */
.dcard { background:white; border:1px solid #E3E8EF; border-radius:10px; padding:1rem 1.15rem; box-shadow:0 1px 2px rgba(0,0,0,0.04); }
.dcard-label { font-size:0.66rem; font-weight:600; text-transform:uppercase; letter-spacing:0.08em; color:#9CA3AF; margin-bottom:0.7rem; }

/* ── Blocked ──────────────────────────────────────────── */
.blocked-card { background:#FFF5F5; border:1px solid #FECACA; border-left:4px solid #EF4444; border-radius:10px; padding:1.25rem 1.5rem; }
.blocked-title { font-weight:600; color:#B91C1C; font-size:0.9rem; margin-bottom:0.4rem; }
.blocked-body  { color:#7F1D1D; font-size:0.85rem; line-height:1.5; }

/* ── Context box ──────────────────────────────────────── */
.ctx-box { background:#F0F7FF; border:1px solid #BFDBFE; border-radius:7px; padding:0.7rem 0.9rem; font-size:0.8rem; line-height:1.6; color:#1E3A5F; margin-top:0.4rem; }

/* ── Stats bar ────────────────────────────────────────── */
.stats-bar { display:flex; gap:1rem; background:white; border:1px solid #E3E8EF; border-radius:10px; padding:1rem 1.25rem; margin-bottom:1rem; box-shadow:0 1px 2px rgba(0,0,0,0.04); }
.stat-item { flex:1; text-align:center; border-right:1px solid #F3F4F6; padding-right:1rem; }
.stat-item:last-child { border-right:none; padding-right:0; }
.stat-val  { font-size:1.5rem; font-weight:700; color:#2D6BE4; line-height:1; }
.stat-lbl  { font-size:0.68rem; color:#9CA3AF; margin-top:0.2rem; text-transform:uppercase; letter-spacing:0.06em; }

/* ── Streamlit overrides ──────────────────────────────── */
div[data-testid="stExpander"] { background:white !important; border:1px solid #E3E8EF !important; border-radius:10px !important; box-shadow:0 1px 2px rgba(0,0,0,0.04) !important; }
div[data-testid="stExpander"] summary { font-size:0.8rem !important; font-weight:600 !important; color:#374151 !important; }
[data-testid="stStatusWidget"] { background:white !important; border:1px solid #E3E8EF !important; border-radius:9px !important; }
[data-testid="stMetric"]       { background:#F8FAFC; border:1px solid #E3E8EF; border-radius:9px; padding:0.7rem 1rem; }
[data-testid="stMetricValue"]  { color:#2D6BE4 !important; }
[data-testid="stMetricLabel"]  { color:#9CA3AF !important; font-size:0.72rem !important; }
.stDataFrame  { border-radius:8px !important; overflow:hidden; border:1px solid #E3E8EF !important; }
.stAlert      { border-radius:8px !important; }
.stSpinner *  { color:#2D6BE4 !important; }
code { background:#EEF3FF !important; color:#2D6BE4 !important; border-radius:4px !important; font-size:0.83em !important; }
pre  { background:#F8FAFC !important; border:1px solid #E3E8EF !important; border-radius:8px !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════
if "history"       not in st.session_state: st.session_state.history       = []
if "last_question" not in st.session_state: st.session_state.last_question = ""


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    # Logo
    st.markdown("""
    <div class="snav-logo">
      <div class="snav-logo-icon">❄️</div>
      <div>
        <div class="snav-logo-name">Cortex Search</div>
        <div class="snav-logo-sub">Data Intelligence Platform</div>
      </div>
    </div>""", unsafe_allow_html=True)

    # Nav
    st.markdown('<div class="snav-section">Main</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="snav-item active">🔍&nbsp; Search</div>
    <div class="snav-item">📊&nbsp; Analytics</div>
    <div class="snav-item">📋&nbsp; Reports</div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="snav-section">Administration</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="snav-item">🔒&nbsp; Governance</div>
    <div class="snav-item">📝&nbsp; Audit Log</div>
    <div class="snav-item">⚙️&nbsp; Settings</div>
    """, unsafe_allow_html=True)

    # Secure views
    st.markdown('<div class="snav-section">Connected Views</div>', unsafe_allow_html=True)
    for v in ALLOWED_VIEWS:
        st.markdown(f'<div class="snav-view"><div class="snav-dot"></div>{v}</div>', unsafe_allow_html=True)

    # Governance controls
    st.markdown('<div class="snav-section">Active Controls</div>', unsafe_allow_html=True)
    for icon, label in [("✓","Tenant isolation"),("✓","PII masking"),("✓","DDL/DML blocked"),("✓","LIMIT enforced"),("✓","Full audit trail")]:
        st.markdown(f'<div class="snav-ctrl"><span style="color:#12B76A;font-weight:700;">{icon}</span>&nbsp;{label}</div>', unsafe_allow_html=True)

    # Stats
    stats = get_log_stats()
    if stats.get("total_queries"):
        st.markdown('<div class="snav-section">Session</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        c1.metric("Queries", stats.get("total_queries", 0))
        c2.metric("Blocked", stats.get("blocked", 0))

    # Recent history
    if st.session_state.history:
        st.markdown('<div class="snav-section">Recent Searches</div>', unsafe_allow_html=True)
        for entry in reversed(st.session_state.history[-4:]):
            icon = "✅" if entry["valid"] else "🚫"
            st.markdown(f"""
            <div class="snav-hist">
              <div class="snav-hist-q">{icon} {entry['question']}</div>
              <div class="snav-hist-meta">{entry['row_count']} rows · {entry['confidence']}</div>
            </div>""", unsafe_allow_html=True)

    # User
    st.markdown(f"""
    <div class="snav-user">
      <div class="snav-avatar">NT</div>
      <div>
        <div class="snav-uname">Nikitha R.</div>
        <div class="snav-utenant">{DEFAULT_TENANT_ID}</div>
      </div>
      <div class="snav-plan">Mock</div>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE HEADER
# ══════════════════════════════════════════════════════════════════════════════
stats = get_log_stats()
total_q = stats.get("total_queries", 0)

st.markdown(f"""
<div class="page-header">
  <div class="ph-left">
    <div class="ph-breadcrumb">Home <span>›</span> Search</div>
    <div class="ph-title">Data Search</div>
    <div class="ph-sub">Query your business data in plain English — governed, audited, and secure.</div>
  </div>
  <div class="ph-actions">
    <div class="ph-live-badge"><div class="ph-live-dot"></div> Mock Mode</div>
    <button class="ph-btn-primary">⚡ Connect Snowflake</button>
  </div>
</div>
""", unsafe_allow_html=True)

# Stats bar (only if we have data)
if total_q:
    st.markdown(f"""
    <div class="stats-bar">
      <div class="stat-item"><div class="stat-val">{stats.get("total_queries") or 0}</div><div class="stat-lbl">Total Queries</div></div>
      <div class="stat-item"><div class="stat-val" style="color:#12B76A;">{stats.get("successful") or 0}</div><div class="stat-lbl">Successful</div></div>
      <div class="stat-item"><div class="stat-val" style="color:#F59E0B;">{stats.get("blocked") or 0}</div><div class="stat-lbl">Blocked</div></div>
      <div class="stat-item"><div class="stat-val" style="color:#9CA3AF;">{stats.get("avg_latency_ms") or "–"}</div><div class="stat-lbl">Avg Latency ms</div></div>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  SEARCH CARD
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="sc-label">Ask a question</div>', unsafe_allow_html=True)

with st.form(key="query_form", clear_on_submit=False):
    question = st.text_input(
        "question",
        value=st.session_state.last_question,
        placeholder="e.g.  What is the average deal size for Enterprise customers in EMEA last quarter?",
        label_visibility="collapsed",
    )
    b1, _, b2 = st.columns([2, 7, 1])
    submitted = b1.form_submit_button("Search", type="primary", use_container_width=True)
    clear_btn = b2.form_submit_button("Clear", use_container_width=True)

st.markdown('<div class="chips-label">Try an example query</div>', unsafe_allow_html=True)

EXAMPLES = [
    "Top 10 customers by revenue",
    "Avg deal size in EMEA last quarter",
    "Deals closed in AMER this year",
    "Customers with high churn risk",
    "Revenue by region",
    "Deal size by segment",
]
chip_cols = st.columns(len(EXAMPLES))
for i, ex in enumerate(EXAMPLES):
    if chip_cols[i].button(ex, key=f"chip_{i}", use_container_width=True):
        st.session_state.last_question = ex
        st.rerun()

if clear_btn:
    st.session_state.last_question = ""
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
if submitted and question.strip():
    st.session_state.last_question = question
    qlog = QueryLog(tenant_id=DEFAULT_TENANT_ID, question=question)

    step_ph = st.empty()

    def render_stepper(active: int, blocked: bool = False):
        steps = [("1","Understand"),("2","Generate SQL"),("3","Validate"),("4","Execute"),("5","KB Context"),("6","Answer")]
        html = '<div class="stepper">'
        for i,(n,lbl) in enumerate(steps):
            if i > 0: html += '<span class="s-arrow">›</span>'
            if i < active:
                cls, n = "done", "✓"
            elif i == active:
                cls = "active" if not blocked else "pending"
            else:
                cls = "pending"
            lbl_cls = "active" if i == active and not blocked else ""
            html += f'<div class="s-step"><div class="s-dot {cls}">{n}</div><span class="s-label {lbl_cls}">{lbl}</span></div>'
        html += '</div>'
        step_ph.markdown(html, unsafe_allow_html=True)

    render_stepper(0)

    with st.spinner("Running agent pipeline…"):
        t0 = time.time()

        with st.status("Step 1 — Understanding query intent…", expanded=False):
            intent = understand_query(question)
            qlog.set_intent(intent.to_dict())
            st.json(intent.to_dict())
        render_stepper(1)

        with st.status("Step 2 — Generating SQL…", expanded=False):
            generated_sql = generate_sql(intent)
            qlog.set_sql(generated_sql)
            st.code(generated_sql, language="sql")
        render_stepper(2)

        with st.status("Step 3 — Validating governance rules…", expanded=False):
            validation = validate_sql(generated_sql)
            qlog.set_validation(validation.valid, validation.reason)
            if validation.valid: st.success(validation.reason)
            else:                st.error(validation.reason)
        render_stepper(3, blocked=not validation.valid)

        if not validation.valid:
            qlog.set_error("SQL blocked by validator")
            qlog.save()
            st.markdown(f"""
            <div class="blocked-card">
              <div class="blocked-title">🛡️ Query Blocked by Governance</div>
              <div class="blocked-body">{validation.reason}</div>
            </div>""", unsafe_allow_html=True)
            st.stop()

        with st.status("Step 4 — Executing against secure view…", expanded=False):
            try:
                df = execute_query(validation.safe_sql or generated_sql)
                qlog.set_results(len(df))
                st.dataframe(df, use_container_width=True)
            except Exception as exc:
                qlog.set_error(str(exc))
                qlog.save()
                st.error(f"Query execution failed: {exc}")
                st.stop()
        render_stepper(4)

        with st.status("Step 5 — Fetching knowledge base context…", expanded=False):
            results_summary = df.to_string(index=False, max_rows=5) if not df.empty else ""
            kb_context, kb_sources = retrieve_kb_context(question, results_summary)
            qlog.set_kb_sources(kb_sources)
            st.write("Sources:", ", ".join(kb_sources) if kb_sources else "None matched.")
        render_stepper(5)

        with st.status("Step 6 — Synthesizing answer…", expanded=False):
            answer_result = synthesize_answer(question, df, kb_context, kb_sources)
            st.write(answer_result.answer)
        render_stepper(6)

        log_id  = qlog.save()
        elapsed = round((time.time() - t0) * 1000)

    # ── Answer ─────────────────────────────────────────────────────────────
    conf_cls = {"High": "b-conf-high", "Medium": "b-conf-med", "Low": "b-conf-low"}.get(answer_result.confidence, "b-conf-med")
    val_html = '<span class="b-pass">✓ Passed</span>' if validation.valid else '<span class="b-fail">✗ Blocked</span>'

    st.markdown(f"""
    <div class="answer-card">
      <div class="ac-top">
        <span class="ac-label">Answer</span>
        <div class="ac-badges">
          <span class="{conf_cls}">{answer_result.confidence} Confidence</span>
          {val_html}
        </div>
      </div>
      <div class="ac-answer">{answer_result.answer}</div>
      <div class="ac-meta">
        <span class="ac-meta-item">📊 {len(df)} rows returned</span>
        <span class="ac-meta-item">⚡ {elapsed} ms</span>
        <span class="ac-meta-item">🔑 Log: <code>{log_id[:8]}…</code></span>
        <span class="ac-meta-item">🏢 Tenant: <code>{DEFAULT_TENANT_ID}</code></span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Detail grid ─────────────────────────────────────────────────────────
    col_sql, col_data, col_ctx = st.columns([2, 3, 2])

    with col_sql:
        st.markdown('<div class="dcard"><div class="dcard-label">Generated SQL</div>', unsafe_allow_html=True)
        st.code(validation.safe_sql or generated_sql, language="sql")
        st.markdown('</div>', unsafe_allow_html=True)

    with col_data:
        st.markdown('<div class="dcard"><div class="dcard-label">Query Results</div>', unsafe_allow_html=True)
        if df.empty:
            st.info("No rows returned for this query.")
        else:
            display_df = df.copy()
            for col in display_df.columns:
                if any(kw in col.lower() for kw in ["revenue","deal_size","acv","value","size"]):
                    try:
                        display_df[col] = display_df[col].apply(lambda x: f"${x:,.0f}" if isinstance(x,(int,float)) else x)
                    except Exception:
                        pass
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_ctx:
        st.markdown('<div class="dcard"><div class="dcard-label">Business Context</div>', unsafe_allow_html=True)
        if kb_sources:
            for src in kb_sources:
                st.markdown(f'<div style="font-size:0.76rem;font-weight:600;color:#2D6BE4;margin-bottom:0.2rem;">📌 {src}</div>', unsafe_allow_html=True)
            snippet = kb_context.split("\n\n")[0] if kb_context else ""
            if snippet:
                st.markdown(f'<div class="ctx-box">{snippet[:280]}{"…" if len(snippet)>280 else ""}</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="font-size:0.78rem;color:#9CA3AF;padding-top:0.2rem;">No KB definitions matched this query.</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # History
    st.session_state.history.append({
        "question":   question,
        "sql":        validation.safe_sql or generated_sql,
        "row_count":  len(df),
        "valid":      validation.valid,
        "confidence": answer_result.confidence,
        "answer":     (answer_result.answer[:120]+"…") if len(answer_result.answer)>120 else answer_result.answer,
        "log_id":     log_id,
    })

    with st.expander("🔍 Query Intent Details", expanded=False):
        st.json(intent.to_dict())


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIT LOG
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div style="margin-top:1.5rem;"></div>', unsafe_allow_html=True)
with st.expander("📋 Query Audit Log", expanded=False):
    logs = get_recent_logs(limit=15)
    if not logs:
        st.info("No queries logged yet. Run a search above to get started.")
    else:
        log_df = pd.DataFrame(logs)
        cols   = [c for c in ["timestamp","question","validation","row_count","latency_ms","status"] if c in log_df.columns]
        st.dataframe(
            log_df[cols].rename(columns={"timestamp":"Time","question":"Question","validation":"Validation","row_count":"Rows","latency_ms":"ms","status":"Status"}),
            use_container_width=True, hide_index=True,
        )
