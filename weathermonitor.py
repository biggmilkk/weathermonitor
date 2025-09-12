import os, sys, time, gc, logging, asyncio, httpx, psutil, traceback
import streamlit as st
from dateutil import parser as dateparser
from streamlit_autorefresh import st_autorefresh
import nest_asyncio

from feeds import get_feed_definitions
from utils.scraper_registry import SCRAPER_REGISTRY
from computation import compute_counts
from renderer import (
    RENDERERS,
    ec_remaining_new_total,
    nws_remaining_new_total,
    uk_remaining_new_total,  # <-- UK helper import
    ec_bucket_from_title,
    draw_badge,
    safe_int,
    alert_id,
    meteoalarm_country_has_alerts,
    meteoalarm_mark_and_sort,
    meteoalarm_snapshot_ids,
    render_empty_state,
)

# --------------------------------------------------------------------
# Setup
# --------------------------------------------------------------------
os.environ.setdefault("STREAMLIT_WATCHER_TYPE", "poll")
nest_asyncio.apply()
st.set_page_config(page_title="Global Weather Monitor", layout="wide")
logging.basicConfig(level=logging.WARNING)

#st.markdown("""
#<style>
#/* Remove top padding/decoration and default margins to kill the white bar */
#section.main > div.block-container { padding-top: 0 !important; }
#div[data-testid="stDecoration"] { display: none !important; }
#.stMarkdown, [data-testid="stMarkdown"], [data-testid="stMarkdownContainer"] { margin: 0 !important; padding: 0 !important; }
#[data-testid="stRadio"] { margin-bottom: 0 !important; }
#[data-testid="stRadio"] > div { display: flex; align-items: center; }
#[data-testid="stRadio"] div[role="radiogroup"] { display: flex; gap: 20px; }
#[data-testid="stRadio"] div[role="radiogroup"] label {
#  border: none !important;
#  background: transparent !important;
#  padding: 0 !important;
#  margin: 0 !important;
#  min-width: auto !important;
#  justify-content: flex-start;
#}
#/* Hide text inside label */
#[data-testid="stRadio"] div[role="radiogroup"] label p,
#[data-testid="stRadio"] div[role="radiogroup"] label span {
#  font-size: 0 !important;
#  margin: 0 !important;
#  padding: 0 !important;
#}
#/* Icons appear after the radio circle */
#[data-testid="stRadio"] div[role="radiogroup"] label::after {
#  content: "";
#  width: 16px;
#  height: 16px;
#  display: inline-block;
#  margin-left: 6px;
#  background: currentColor;
#  vertical-align: -2px;
#  -webkit-mask-size: cover;
#  mask-size: cover;
#  -webkit-mask-repeat: no-repeat;
#  mask-repeat: no-repeat;
#}
#/* Desktop icon */
#[data-testid="stRadio"] div[role="radiogroup"] label:nth-of-type(1)::after {
#  -webkit-mask-image: url('data:image/svg+xml;utf8,\
#<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">\
#<rect x="3" y="4" width="18" height="12" rx="2" ry="2" fill="black"/>\
#<rect x="9" y="18" width="6" height="2" fill="black"/>\
#</svg>');
#  mask-image: url('data:image/svg+xml;utf8,\
#<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">\
#<rect x="3" y="4" width="18" height="12" rx="2" ry="2" fill="black"/>\
#<rect x="9" y="18" width="6" height="2" fill="black"/>\
#</svg>');
#}
#/* Mobile icon */
#[data-testid="stRadio"] div[role="radiogroup"] label:nth-of-type(2)::after {
#  -webkit-mask-image: url('data:image/svg+xml;utf8,\
#<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">\
#<rect x="7" y="2" width="10" height="20" rx="2" ry="2" fill="black"/>\
#<circle cx="12" cy="18" r="1" fill="black"/>\
#</svg>');
#  mask-image: url('data:image/svg+xml;utf8,\
#<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24">\
#<rect x="7" y="2" width="10" height="20" rx="2" ry="2" fill="black"/>\
#<circle cx="12" cy="18" r="1" fill="black"/>\
#</svg>');
#}
#</style>
#""", unsafe_allow_html=True)

vm = psutil.virtual_memory()
MEMORY_LIMIT = int(min(0.5 * vm.total, 4 * 1024**3))
MEMORY_HIGH_WATER = 0.85 * MEMORY_LIMIT
MEMORY_LOW_WATER  = 0.50 * MEMORY_LIMIT
MIN_CONC, MAX_CONC, STEP = 5, 50, 5

def _rss_bytes(): return psutil.Process(os.getpid()).memory_info().rss
st.session_state.setdefault("concurrency", 20)
rss_before = _rss_bytes()
if rss_before > MEMORY_HIGH_WATER: st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"]-STEP)
elif rss_before < MEMORY_LOW_WATER: st.session_state["concurrency"] = min(MAX_CONC, st.session_state["concurrency"]+STEP)
MAX_CONCURRENCY = st.session_state["concurrency"]
st.caption(f"Concurrency: {MAX_CONCURRENCY}, RSS: {rss_before // (1024*1024)} MB")

# --------------------------------------------------------------------
# State & Config
# --------------------------------------------------------------------
FETCH_TTL = 60
HTTP2_ENABLED = False
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
st_autorefresh(interval=FETCH_TTL * 1000, key="auto_refresh_main")

@st.cache_data(ttl=3600)
def load_feeds(): return get_feed_definitions()

FEED_CONFIG = load_feeds()
now = time.time()
for key, conf in FEED_CONFIG.items():
    st.session_state.setdefault(f"{key}_data", [])
    st.session_state.setdefault(f"{key}_last_fetch", 0)
    st.session_state.setdefault(f"{key}_last_seen_time", 0.0)
    st.session_state.setdefault(f"{key}_pending_seen_time", None)
    if conf["type"] == "rss_meteoalarm":
        st.session_state.setdefault(f"{key}_last_seen_alerts", tuple())
st.session_state.setdefault("last_refreshed", now)
st.session_state.setdefault("active_feed", None)
st.session_state.setdefault("layout_mode", "Desktop")   # Desktop | Mobile
st.session_state.setdefault("mobile_view", "list")      # list | detail

# --------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------
async def with_retries(fn, *, attempts=3, base_delay=0.5):
    for i in range(attempts):
        try: return await fn()
        except Exception:
            if i == attempts-1: raise
            await asyncio.sleep(base_delay*(2**i))

async def _fetch_all_feeds(configs: dict):
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    limits = httpx.Limits(max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY)
    transport = httpx.AsyncHTTPTransport(retries=3)
    timeout = httpx.Timeout(30.0)
    headers = {"User-Agent":"weathermonitor.app/1.0 (+support@weathermonitor.app)"}
    async with httpx.AsyncClient(timeout=timeout, limits=limits, transport=transport, http2=HTTP2_ENABLED, headers=headers) as client:
        async def bound_fetch(key, conf):
            async with sem:
                async def call():
                    call_conf = {}
                    for k,v in conf.items():
                        if k in ("label","type"): continue
                        if k == "conf" and isinstance(v, dict): call_conf.update(v)
                        else: call_conf[k] = v
                    return await SCRAPER_REGISTRY[conf["type"]](call_conf, client)
                try: data = await with_retries(call)
                except Exception as ex:
                    logging.warning(f"[{key.upper()} FETCH ERROR] {ex}")
                    logging.warning(traceback.format_exc())
                    data = {"entries": [], "error": str(ex), "source": conf}
                return key, data
        tasks = [bound_fetch(k,cfg) for k,cfg in FEED_CONFIG.items() if k in configs]
        return await asyncio.gather(*tasks)

def run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close(); asyncio.set_event_loop(None)

def _immediate_rerun():
    if hasattr(st,"rerun"): st.rerun()
    elif hasattr(st,"experimental_rerun"): st.experimental_rerun()

# --------------------------------------------------------------------
# Refresh
# --------------------------------------------------------------------
now = time.time()
to_fetch = {k:v for k,v in FEED_CONFIG.items() if now - st.session_state[f"{k}_last_fetch"] > FETCH_TTL}
if to_fetch:
    results = run_async(_fetch_all_feeds(to_fetch))
    for key, raw in results:
        entries = raw.get("entries", [])
        st.session_state[f"{key}_data"] = entries
        st.session_state[f"{key}_last_fetch"] = now
        st.session_state["last_refreshed"] = now
        conf = FEED_CONFIG[key]
        if st.session_state.get("active_feed") == key:
            if conf["type"] == "rss_meteoalarm":
                last_seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
                _, new_count = compute_counts(entries, conf, last_seen_ids, alert_id_fn=alert_id)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
            elif conf["type"] == "ec_async":
                pass
            elif conf["type"] == "nws_grouped_compact":
                pass
            elif conf["type"] == "uk_grouped_compact":
                pass
            else:
                last_seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
                _, new_count = compute_counts(entries, conf, last_seen_ts)
                if new_count == 0:
                    st.session_state[f"{key}_last_seen_time"] = now
        if conf["type"] == "ec_async":
            st.session_state[f"{key}_remaining_new_total"] = ec_remaining_new_total(key, entries)
        elif conf["type"] == "nws_grouped_compact":
            st.session_state[f"{key}_remaining_new_total"] = nws_remaining_new_total(key, entries)
        elif conf["type"] == "uk_grouped_compact":  # <-- ensure UK total is computed
            st.session_state[f"{key}_remaining_new_total"] = uk_remaining_new_total(key, entries)
        gc.collect()

rss_after = _rss_bytes()
if rss_after > MEMORY_HIGH_WATER: st.session_state["concurrency"] = max(MIN_CONC, st.session_state["concurrency"]-STEP)

# --------------------------------------------------------------------
# Header
# --------------------------------------------------------------------
st.title("Global Weather Monitor")
st.caption(f"Last refreshed: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(st.session_state['last_refreshed']))}")

st.markdown("---")

# --------------------------------------------------------------------
# Shared Rendering
# --------------------------------------------------------------------
def _render_feed_details(active, conf, entries, badge_placeholders=None):
    data_list = sorted(entries, key=lambda x: x.get("published", ""), reverse=True)

    if conf["type"] == "rss_bom_multi":
        RENDERERS["rss_bom_multi"](entries, {**conf, "key": active})

    elif conf["type"] == "ec_async":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        _PROVINCE_NAMES = {
            "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
            "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
            "NT": "Northwest Territories", "NS": "Nova Scotia", "NU": "Nunavut",
            "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
            "SK": "Saskatchewan", "YT": "Yukon",
        }
        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for k in list(bucket_lastseen.keys()):
                    bucket_lastseen[k] = now_ts
                for e in entries:
                    bucket = ec_bucket_from_title(e.get("title","") or "")
                    if not bucket:
                        continue
                    code = e.get("province", "")
                    prov_name = _PROVINCE_NAMES.get(code, code) if isinstance(code, str) else str(code)
                    bkey = f"{prov_name}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph: draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["ec_grouped_compact"](entries, {**conf, "key": active})
        ec_total_now = ec_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(ec_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph: draw_badge(ph, safe_int(ec_total_now))

    elif conf["type"] == "nws_grouped_compact":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    state = (a.get("state") or a.get("state_name") or a.get("state_code") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
                    bkey = f"{state}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph: draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["nws_grouped_compact"](entries, {**conf, "key": active})
        nws_total_now = nws_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(nws_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph: draw_badge(ph, safe_int(nws_total_now))

    elif conf["type"] == "uk_grouped_compact":
        if not entries:
            st.info("No active warnings that meet thresholds at the moment.")
            st.session_state[f"{active}_remaining_new_total"] = 0
            return

        cols = st.columns([0.25, 0.75])
        with cols[0]:
            if st.button("Mark all as seen", key=f"{active}_mark_all_seen"):
                lastseen_key = f"{active}_bucket_last_seen"
                bucket_lastseen = st.session_state.get(lastseen_key, {}) or {}
                now_ts = time.time()
                for a in entries:
                    region = (a.get("state") or a.get("region") or "Unknown")
                    bucket = (a.get("bucket") or a.get("event") or a.get("title") or "Alert")
                    bkey = f"{region}|{bucket}"
                    bucket_lastseen[bkey] = now_ts
                st.session_state[lastseen_key] = bucket_lastseen
                st.session_state[f"{active}_remaining_new_total"] = 0
                if badge_placeholders:
                    ph = badge_placeholders.get(active)
                    if ph: draw_badge(ph, 0)
                _immediate_rerun()

        RENDERERS["uk_grouped_compact"](entries, {**conf, "key": active})
        uk_total_now = uk_remaining_new_total(active, entries)
        st.session_state[f"{active}_remaining_new_total"] = int(uk_total_now)
        if badge_placeholders:
            ph = badge_placeholders.get(active)
            if ph: draw_badge(ph, safe_int(uk_total_now))

    elif conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{active}_last_seen_alerts"])
        countries = [c for c in data_list if meteoalarm_country_has_alerts(c)]
        countries = meteoalarm_mark_and_sort(countries, seen_ids)
        for country in countries:
            RENDERERS["rss_meteoalarm"](country, {**conf, "key": active})
        st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(countries)

    elif conf["type"] == "rss_jma":
        RENDERERS["rss_jma"](entries, {**conf, "key": active})

    else:
        seen_ts = st.session_state.get(f"{active}_last_seen_time") or 0.0
        if not data_list:
            render_empty_state()
            pkey = f"{active}_pending_seen_time"
            pending = st.session_state.get(pkey, None)
            if pending is not None: st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)
        else:
            for item in data_list:
                pub = item.get("published")
                try: ts = dateparser.parse(pub).timestamp() if pub else 0.0
                except Exception: ts = 0.0
                item["is_new"] = bool(ts > seen_ts)
                RENDERERS.get(conf["type"], lambda i,c: None)(item, conf)
            pkey = f"{active}_pending_seen_time"]
            pending = st.session_state.get(pkey, None)
            if pending is not None: st.session_state[f"{active}_last_seen_time"] = float(pending)
            st.session_state.pop(pkey, None)

def _new_count_for(key, conf, entries):
    if conf["type"] == "rss_meteoalarm":
        seen_ids = set(st.session_state[f"{key}_last_seen_alerts"])
        _, new_count = compute_counts(entries, conf, seen_ids, alert_id_fn=alert_id)
        return new_count
    if conf["type"] == "ec_async":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val,int) else int(ec_remaining_new_total(key, entries) or 0)
    if conf["type"] == "nws_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val,int) else int(nws_remaining_new_total(key, entries) or 0)
    if conf["type"] == "uk_grouped_compact":
        val = st.session_state.get(f"{key}_remaining_new_total")
        return int(val) if isinstance(val, int) else int(uk_remaining_new_total(key, entries) or 0)
    seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
    _, new_count = compute_counts(entries, conf, seen_ts)
    return new_count

# --------------------------------------------------------------------
# Mobile
# --------------------------------------------------------------------
if st.session_state["layout_mode"] == "Mobile":
    if not FEED_CONFIG:
        st.info("No feeds configured."); st.stop()

    if st.session_state["mobile_view"] == "list":
        for i, (key, conf) in enumerate(FEED_CONFIG.items()):
            entries = st.session_state[f"{key}_data"]
            cnt = _new_count_for(key, conf, entries)
            with st.container():
                cols = st.columns([0.75, 0.25])
                with cols[0]:
                    clicked = st.button(conf.get("label", key.upper()),
                                        key=f"m_list_btn_{key}_{i}",
                                        use_container_width=True, type="secondary")
                with cols[1]:
                    ph = st.empty(); draw_badge(ph, safe_int(cnt))
                if clicked:
                    st.session_state["active_feed"] = key
                    st.session_state["mobile_view"] = "detail"
                    _immediate_rerun()
            st.markdown("")
    else:
        active = st.session_state.get("active_feed")
        if not active:
            st.session_state["mobile_view"] = "list"; _immediate_rerun()
        conf = FEED_CONFIG[active]; entries = st.session_state[f"{active}_data"]

        # Sticky topbar; ensure it's the very first element to avoid any preceding margins
        st.markdown('<div class="topbar"></div>', unsafe_allow_html=True)
        tb = st.columns([0.15, 0.70, 0.15])
        with tb[0]:
            if st.button("✕", key="m_detail_close", use_container_width=True):
                if conf["type"] == "rss_meteoalarm":
                    st.session_state[f"{active}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                elif conf["type"] not in ("ec_async","nws_grouped_compact","uk_grouped_compact"):
                    st.session_state[f"{active}_last_seen_time"] = time.time()
                st.session_state["mobile_view"] = "list"
                st.session_state["active_feed"] = None
                _immediate_rerun()
        with tb[1]:
            st.markdown(f"#### {conf.get('label', active.upper())}")
        with tb[2]:
            pass

        _render_feed_details(active, conf, entries, badge_placeholders=None)

# --------------------------------------------------------------------
# Desktop (original buttons row + details below)
# --------------------------------------------------------------------
else:
    if not FEED_CONFIG:
        st.info("No feeds configured."); st.stop()

    cols = st.columns(len(FEED_CONFIG))
    badge_placeholders = {}; _toggled = False
    for i, (key, conf) in enumerate(FEED_CONFIG.items()):
        entries = st.session_state[f"{key}_data"]
        if conf["type"] == "rss_meteoalarm":
            seen_ids = set(st.session_state[f"{key}_last_seen_alerts"]); _, new_count = compute_counts(entries, conf, seen_ids, alert_id_fn=alert_id)
        elif conf["type"] == "ec_async":
            ec_total = st.session_state.get(f"{key}_remaining_new_total")
            new_count = ec_total if isinstance(ec_total,int) else ec_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        elif conf["type"] == "nws_grouped_compact":
            nws_total = st.session_state.get(f"{key}_remaining_new_total")
            new_count = nws_total if isinstance(nws_total,int) else nws_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        elif conf["type"] == "uk_grouped_compact":  # <-- NEW: UK handled like NWS here
            uk_total = st.session_state.get(f"{key}_remaining_new_total")
            new_count = uk_total if isinstance(uk_total,int) else uk_remaining_new_total(key, entries)
            st.session_state[f"{key}_remaining_new_total"] = int(new_count or 0)
        else:
            seen_ts = st.session_state.get(f"{key}_last_seen_time") or 0.0
            _, new_count = compute_counts(entries, conf, seen_ts)

        with cols[i]:
            is_active = (st.session_state.get("active_feed") == key)
            clicked = st.button(conf.get("label", key.upper()),
                                key=f"btn_{key}_{i}",
                                use_container_width=True,
                                type=("primary" if is_active else "secondary"))
            badge_ph = st.empty(); badge_placeholders[key] = badge_ph
            draw_badge(badge_ph, safe_int(new_count))
            if clicked:
                if st.session_state.get("active_feed") == key:
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_last_seen_alerts"] = meteoalarm_snapshot_ids(entries)
                    elif conf["type"] in ("ec_async","nws_grouped_compact","uk_grouped_compact"):
                        pass
                    else:
                        st.session_state[f"{key}_last_seen_time"] = time.time()
                    st.session_state["active_feed"] = None
                else:
                    st.session_state["active_feed"] = key
                    if conf["type"] == "rss_meteoalarm":
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                    elif conf["type"] in ("ec_async","nws_grouped_compact","uk_grouped_compact"):
                        st.session_state[f"{key}_pending_seen_time"] = None
                    else:
                        st.session_state[f"{key}_pending_seen_time"] = time.time()
                _toggled = True

    if _toggled: _immediate_rerun()
    active = st.session_state["active_feed"]
    if active:
        st.markdown("---")
        conf = FEED_CONFIG[active]; entries = st.session_state[f"{active}_data"]
        _render_feed_details(active, conf, entries, badge_placeholders)
