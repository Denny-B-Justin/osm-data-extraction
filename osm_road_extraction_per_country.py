# Databricks notebook source
# MAGIC %md
# MAGIC # OSM roads → one Delta table per country
# MAGIC
# MAGIC Extracts OSM `highway=*` ways edited since `since_date` and writes **one table per country**:
# MAGIC `prd_mega.sgpbpi163.<country>_osm_road` (e.g. `prd_mega.sgpbpi163.afghanistan_osm_road`).
# MAGIC
# MAGIC | Old behaviour | New behaviour |
# MAGIC |---|---|
# MAGIC | Every country's GeoDataFrame was kept in `all_gdfs` on the driver → OOM, exit 137 | Each tile's Overpass response is streamed to local disk, parsed incrementally and appended to Delta in small batches. Nothing accumulates across tiles or countries |
# MAGIC | Whole-country queries → Overpass timeouts, silently reported as "no data" (AT, BE, BR, CA, CN, …) | Countries are cut into 2° tiles; a tile that times out or runs out of memory on the server is split into 4, recursively |
# MAGIC | Overpass reports timeouts as HTTP 200 + a `remark`, which was ignored | The remark is checked; such tiles are split/retried and never counted as empty |
# MAGIC | Nominatim name look-ups failed for BO, BQ, CD, … | Countries are found by ISO code (`ISO3166-1` tag) and queried by their OSM *area*, so neighbours' roads are not mixed in |
# MAGIC | Mirrors configured but never used; failures swallowed | Health-checked endpoint pool with fail-over, cooldowns and per-endpoint rate limiting |
# MAGIC | A crash lost all progress | Per-tile and per-country checkpoints. Re-run with `mode = resume` to continue where it stopped |
# MAGIC
# MAGIC Run the full 249-country pass as a **Job** (it takes many hours; a job survives browser disconnects).
# MAGIC To refresh specific countries later: put their ISO2 codes in the `countries` widget and set `mode = force`.

# COMMAND ----------

# MAGIC %pip install --quiet "pycountry>=22.3.5" "ijson>=3.2" "shapely>=2.0" "pyproj>=3.4"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters (widgets / job parameters)

# COMMAND ----------

dbutils.widgets.text("since_date", "2024-01-01T00:00:00Z", "1. Ways edited since (UTC)")
dbutils.widgets.text("countries", "", "2. ISO2 codes, comma-separated (blank = all)")
dbutils.widgets.dropdown("mode", "resume", ["resume", "force"], "3. Mode")
dbutils.widgets.text("country_workers", "3", "4. Countries in parallel")

# COMMAND ----------

import gc
import json
import logging
import math
import os
import random
import re
import socket
import sys
import tempfile
import threading
import time
import unicodedata
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import ijson
import pandas as pd
import pycountry
import requests
from pyproj import Geod
from requests.adapters import HTTPAdapter
from shapely.geometry import box, shape
from shapely.prepared import prep
from shapely.validation import make_valid

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ── Widgets ────────────────────────────────────────────────────────────────────
SINCE_DATE      = dbutils.widgets.get("since_date").strip()
COUNTRY_FILTER  = [x.strip().upper() for x in dbutils.widgets.get("countries").split(",") if x.strip()]
MODE            = dbutils.widgets.get("mode").strip().lower()
COUNTRY_WORKERS = max(1, int(dbutils.widgets.get("country_workers").strip() or "3"))

if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", SINCE_DATE):
    raise ValueError(f"since_date must look like 2024-01-01T00:00:00Z, got {SINCE_DATE!r}")
if MODE not in ("resume", "force"):
    raise ValueError(f"mode must be 'resume' or 'force', got {MODE!r}")

# ── Configuration ──────────────────────────────────────────────────────────────
CFG = {
    # Output
    "catalog_schema":      "prd_mega.sgpbpi163",
    "table_suffix":        "_osm_road",
    "table_naming":        "name",   # "name" → afghanistan_osm_road   |   "iso3" → afg_osm_road
    "create_empty_tables": True,     # territories with 0 matching roads still get an empty table

    # Overpass
    "overpass_endpoints": [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.private.coffee/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    ],
    "slots_per_endpoint":          1,     # concurrent requests per endpoint (overpass-api.de rate-limits per IP)
    "min_interval_per_endpoint_s": 1.0,   # politeness gap between requests to the same endpoint
    "overpass_timeout_s":          180,   # server-side [timeout:]
    "overpass_maxsize_bytes":      None,  # server-side [maxsize:]; None = server default (512 MiB)
    "http_connect_timeout_s":      30,
    "http_read_slack_s":           120,   # HTTP read timeout = overpass_timeout_s + this
    "force_ipv4":                  True,  # avoid failed IPv6 attempts ("[Errno 101] Network is unreachable")

    # Tiling
    "initial_tile_deg": 2.0,   # starting tile size in degrees
    "max_split_depth":  6,     # 2° → 1° → 0.5° → … → ~0.03°
    "prune_min_tiles":  12,    # drop tiles outside the country outline only when the bbox has more tiles than this
    "prune_margin_deg": 0.25,  # safety margin around the (simplified) outline when pruning

    # Retries / parallelism
    "tile_workers_per_country": 2,
    "max_tile_attempts":        10,
    "max_wait_for_endpoint_s":  1800,  # how long a tile waits for any healthy endpoint before giving up
    "final_retry_pass":         True,  # re-run FAILED countries once at the end
    "final_retry_cooldown_s":   300,

    # Memory / writes
    "write_batch_rows":     50_000,    # rows per Delta append (bounds driver memory)
    "tile_log_flush_every": 25,

    # Nominatim (only for outlines of large countries and name fallback)
    "nominatim_url":               "https://nominatim.openstreetmap.org",
    "nominatim_polygon_threshold": 0.005,

    "user_agent": "PIA-Pipeline/1.0 (contact: dbethaniyiljusti@worldbank.org)",
}

def fq(name: str) -> str:
    return f"{CFG['catalog_schema']}.{name}"

STAGING_TABLE  = fq("osm_road_staging")         # scratch rows while a country is in progress
TILE_LOG_TABLE = fq("osm_road_tile_log")        # per-tile checkpoints (for resume)
RUN_LOG_TABLE  = fq("osm_road_extraction_log")  # one row per country attempt
ALL_VIEW       = fq("osm_road_all")             # optional UNION ALL view (built at the end)

AREA_ID_OFFSET = 3_600_000_000   # Overpass area id = relation id + 3600000000

# ── Logging (threads log here; urllib3's giant URL warnings are silenced) ─────
log = logging.getLogger("osm_road")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))
    log.addHandler(_h)
for _noisy in ("urllib3", "requests", "py4j"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

if CFG["force_ipv4"]:
    import urllib3.util.connection as _u3conn
    _u3conn.allowed_gai_family = lambda: socket.AF_INET

def _pick_tmp_dir() -> str:
    for base in ("/local_disk0/tmp", tempfile.gettempdir()):
        try:
            d = os.path.join(base, "osm_road_extract")
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".probe")
            open(probe, "w").close()
            os.remove(probe)
            return d
        except OSError:
            continue
    raise RuntimeError("No writable local temp directory found.")

TMP_DIR = _pick_tmp_dir()
STOP = threading.Event()   # set on cancel so worker threads wind down

try:   # already on by default on serverless / shared clusters, where setting it may be refused
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
except Exception:
    pass

print("✅ Configuration loaded")
print(f"   since_date      : {SINCE_DATE}")
print(f"   countries       : {COUNTRY_FILTER or 'ALL'}")
print(f"   mode            : {MODE}")
print(f"   country_workers : {COUNTRY_WORKERS}")
print(f"   output          : {CFG['catalog_schema']}.<country>{CFG['table_suffix']}")
print(f"   temp dir        : {TMP_DIR}")
print(f"   ijson backend   : {ijson.backend}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Road classification helpers (same logic as before)

# COMMAND ----------

HIGHWAY_TAGS = [
    "motorway", "motorway_link",
    "trunk", "trunk_link",
    "primary", "primary_link",
    "secondary", "secondary_link",
    "tertiary", "tertiary_link",
    "residential", "living_street",
    "unclassified", "service",
    "pedestrian", "busway",
    "track", "path",
    "footway", "cycleway",
    "bridleway", "road",
]
HIGHWAY_REGEX = "|".join(HIGHWAY_TAGS)

_PAVED_SURFACES = {
    "paved", "asphalt", "concrete", "concrete:plates", "concrete:lanes",
    "paving_stones", "sett", "unhewn_cobblestone", "cobblestone",
    "cobblestone:flattened", "metal", "wood", "tartan", "artificial_turf",
}
_UNPAVED_SURFACES = {
    "unpaved", "compacted", "fine_gravel", "gravel", "shells", "rock",
    "pebblestone", "ground", "dirt", "earth", "grass", "grass_paver",
    "mud", "sand", "woodchips", "snow", "ice", "salt", "clay",
    "caliche", "laterite",
}
_INFERRED_PAVED_HIGHWAYS = {
    "motorway", "motorway_link", "trunk", "trunk_link",
    "primary", "primary_link", "secondary", "secondary_link",
    "tertiary", "tertiary_link", "residential", "living_street",
    "unclassified", "service", "pedestrian", "busway",
}
_INFERRED_UNPAVED_HIGHWAYS = {"track", "path", "footway", "bridleway", "cycleway"}

_CLASS_MAP = {
    "motorway": "motorway",     "motorway_link": "motorway",
    "trunk": "trunk",           "trunk_link": "trunk",
    "primary": "primary",       "primary_link": "primary",
    "secondary": "secondary",   "secondary_link": "secondary",
    "tertiary": "tertiary",     "tertiary_link": "tertiary",
    "residential": "residential", "living_street": "residential",
    "unclassified": "unclassified", "road": "unclassified",
    "service": "service",       "busway": "service",
    "pedestrian": "pedestrian",
    "track": "track",
    "path": "path", "footway": "path", "cycleway": "path", "bridleway": "path",
}


def classify_surface(surface: Optional[str], highway: Optional[str]) -> str:
    """'paved' | 'unpaved' | 'unknown' — mirrors carto_road_surface_type()."""
    if surface:
        s = surface.lower().strip()
        if s in _PAVED_SURFACES:
            return "paved"
        if s in _UNPAVED_SURFACES:
            return "unpaved"
    if highway:
        h = highway.lower().strip()
        if h in _INFERRED_PAVED_HIGHWAYS:
            return "paved"
        if h in _INFERRED_UNPAVED_HIGHWAYS:
            return "unpaved"
    return "unknown"


def classify_highway(highway: Optional[str]) -> str:
    """Mirrors carto_highway_class()."""
    return _CLASS_MAP.get((highway or "").lower().strip(), "other")


_MPH_RE   = re.compile(r"^([\d.]+)\s*mph$", re.I)
_KNOTS_RE = re.compile(r"^([\d.]+)\s*knots?$", re.I)
_KMH_RE   = re.compile(r"^([\d.]+)\s*(?:km/h|kmh|kph)?$", re.I)


def parse_maxspeed(value: Optional[str]) -> Optional[float]:
    """Raw maxspeed= tag → km/h, or None for 'walk', 'none', 'RU:urban', '50;30', …"""
    if not value:
        return None
    v = value.strip()
    try:
        m = _MPH_RE.match(v)
        if m:
            return round(float(m.group(1)) * 1.60934, 1)
        m = _KNOTS_RE.match(v)
        if m:
            return round(float(m.group(1)) * 1.852, 1)
        m = _KMH_RE.match(v)
        if m:
            return float(m.group(1))
    except ValueError:
        return None
    return None


def _flag(value: Optional[str]) -> bool:
    """bridge=/tunnel= → bool (anything except no/false/0/empty counts as yes)."""
    return value is not None and str(value).strip().lower() not in ("no", "false", "0", "")


print("✅ Classification helpers defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Country list and table names

# COMMAND ----------

# Kosovo is not in ISO 3166 / pycountry but has its own OSM boundary (ISO3166-1=XK).
EXTRA_COUNTRIES = [{"iso2": "XK", "iso3": "XKX", "name": "Kosovo"}]

# Large countries first, so they overlap with the many small ones instead of running alone at the end.
COUNTRY_PRIORITY = ["US", "RU", "CA", "BR", "CN", "IN", "AU", "FR", "DE", "ID", "MX", "JP",
                    "GB", "IT", "ES", "PL", "UA", "TR", "AR", "ZA", "KZ", "IR", "NG", "PH"]


def _slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def _readable_name(c: dict) -> str:
    """'Congo, The Democratic Republic of the' → 'Democratic Republic of the Congo',
    'Holy See (Vatican City State)' → 'Holy See'."""
    n = c.get("common_name") or c["name"]
    n = re.sub(r"\s*\(.*?\)", "", n).strip()
    if "," in n:
        head, tail = [p.strip() for p in n.split(",", 1)]
        if re.search(r"\bof(\s+the)?$", tail, flags=re.I):
            n = re.sub(r"^the\s+", "", f"{tail} {head}", flags=re.I)
    return n


def table_name_for(c: dict) -> str:
    if CFG["table_naming"] == "iso3":
        base = c["iso3"].lower()
    else:
        base = _slug(_readable_name(c))
    return fq(f"{base}{CFG['table_suffix']}")


def build_country_list(iso2_filter: list) -> list:
    out = []
    for c in pycountry.countries:
        out.append({
            "iso2": c.alpha_2,
            "iso3": c.alpha_3,
            "name": c.name,
            "common_name": getattr(c, "common_name", None),
            "official_name": getattr(c, "official_name", None),
        })
    out.extend(dict(x) for x in EXTRA_COUNTRIES)

    if iso2_filter:
        wanted = set(iso2_filter)
        unknown = wanted - {c["iso2"] for c in out}
        if unknown:
            print(f"⚠️  Unknown ISO2 codes ignored: {sorted(unknown)}")
        out = [c for c in out if c["iso2"] in wanted]

    for c in out:
        c["table"] = table_name_for(c)

    dupes = pd.Series([c["table"] for c in out]).value_counts()
    dupes = dupes[dupes > 1]
    if len(dupes):
        raise ValueError(f"Table name collision(s): {list(dupes.index)} — switch CFG['table_naming'] to 'iso3'.")

    prio = {iso: i for i, iso in enumerate(COUNTRY_PRIORITY)}
    return sorted(out, key=lambda c: (prio.get(c["iso2"], len(prio)), c["name"]))


COUNTRIES = build_country_list(COUNTRY_FILTER)
print(f"✅ {len(COUNTRIES)} countries loaded.")
display(pd.DataFrame([{"iso2": c["iso2"], "name": c["name"], "table": c["table"]} for c in COUNTRIES]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. HTTP layer: endpoint pool, Overpass and Nominatim clients

# COMMAND ----------

_tls = threading.local()


def _session() -> requests.Session:
    """One requests.Session per thread; no urllib3 auto-retries (we retry ourselves, across endpoints)."""
    s = getattr(_tls, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=0, pool_connections=8, pool_maxsize=8)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": CFG["user_agent"]})
        _tls.session = s
    return s


def _host(url: str) -> str:
    return urlparse(url).netloc


def _short(e: Exception, n: int = 300) -> str:
    return re.sub(r"\s+", " ", str(e))[:n]


def _silent_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


# Outcome kinds of one Overpass request
OK, TOO_BIG, TRANSIENT, RATE_LIMITED, AREA_MISSING, FATAL = (
    "ok", "too_big", "transient", "rate_limited", "area_missing", "fatal")

# kind → (effect on endpoint health, base cooldown seconds)
_POOL_FEEDBACK = {
    OK:           ("ok", 0),
    TOO_BIG:      ("neutral", 0),   # query too heavy — not the endpoint's fault
    FATAL:        ("neutral", 0),
    AREA_MISSING: ("neutral", 0),   # handled by excluding that endpoint for the tile
    RATE_LIMITED: ("fail", 60),
    TRANSIENT:    ("fail", 20),
}


class NoEndpointAvailable(RuntimeError):
    pass


class EndpointPool:
    """Thread-safe pool of Overpass endpoints with per-endpoint slots, politeness gap,
    and exponential cooldown after failures (429 / 5xx / network errors)."""

    def __init__(self, urls, slots, min_interval_s):
        if not urls:
            raise ValueError("EndpointPool needs at least one URL")
        self._lock = threading.Lock()
        self._min_interval = float(min_interval_s)
        self._state = {u: {"free": int(slots), "not_before": 0.0, "fails": 0, "ok": 0, "errors": 0}
                       for u in urls}

    @property
    def urls(self):
        return list(self._state)

    def acquire(self, max_wait_s: float, stop_event=None, exclude=()) -> str:
        deadline = time.time() + max_wait_s
        while True:
            if stop_event is not None and stop_event.is_set():
                raise NoEndpointAvailable("stop requested")
            with self._lock:
                now = time.time()
                candidates = [u for u in self._state if u not in exclude] or list(self._state)
                ready = [u for u in candidates
                         if self._state[u]["free"] > 0 and self._state[u]["not_before"] <= now]
                if ready:
                    url = min(ready, key=lambda u: (self._state[u]["fails"], self._state[u]["not_before"]))
                    self._state[url]["free"] -= 1
                    return url
            if time.time() >= deadline:
                raise NoEndpointAvailable(
                    f"no healthy Overpass endpoint for {max_wait_s:.0f}s ({self.describe()})")
            time.sleep(1.0)

    def release(self, url: str, kind: str) -> None:
        outcome, base_cooldown = _POOL_FEEDBACK.get(kind, ("fail", 20))
        with self._lock:
            s = self._state[url]
            s["free"] += 1
            now = time.time()
            if outcome == "fail":
                s["fails"] += 1
                s["errors"] += 1
                cooldown = min(base_cooldown * (2 ** min(s["fails"] - 1, 5)), 900)
                s["not_before"] = max(s["not_before"], now + cooldown)
            else:
                if outcome == "ok":
                    s["fails"] = 0
                    s["ok"] += 1
                s["not_before"] = max(s["not_before"], now + self._min_interval)

    def describe(self) -> str:
        with self._lock:
            now = time.time()
            return ", ".join(
                f"{_host(u)}[ok={s['ok']} err={s['errors']} cooldown={max(0.0, s['not_before'] - now):.0f}s]"
                for u, s in self._state.items())


POOL: Optional[EndpointPool] = None   # built by build_pool() in the run section

_HEAD_BYTES = 8192
_TAIL_BYTES = 8192
_REMARK_RE = re.compile(r'\]\s*,\s*"remark"\s*:\s*"((?:[^"\\]|\\.)*)"\s*\}\s*$', re.S)
_CLOSED_RE = re.compile(r'\]\s*(?:,\s*"remark"\s*:\s*"(?:[^"\\]|\\.)*"\s*)?\}\s*$', re.S)
_BASE_RE = re.compile(r'"timestamp_osm_base"\s*:\s*"([^"]+)"')
_ANY_AREA_RE = re.compile(r'"type"\s*:\s*"area"')


def overpass_fetch_to_file(url: str, query: str, dest_path: str, expect_area_id: Optional[int] = None):
    """POST one Overpass query and stream the body to dest_path (never held in memory).
    Returns (kind, detail, meta)."""
    read_timeout = CFG["overpass_timeout_s"] + CFG["http_read_slack_s"]
    try:
        with _session().post(url, data={"data": query}, stream=True,
                             timeout=(CFG["http_connect_timeout_s"], read_timeout)) as resp:
            code = resp.status_code
            if code != 200:
                try:
                    snippet = re.sub(r"<[^>]+>", " ", resp.text[:2000])
                    snippet = re.sub(r"\s+", " ", snippet).strip()[:300]
                except Exception:
                    snippet = ""
                if code == 429:
                    return RATE_LIMITED, f"HTTP 429 {snippet}", {}
                if code == 400:
                    return FATAL, f"HTTP 400 (bad query) {snippet}", {}
                return TRANSIENT, f"HTTP {code} {snippet}", {}
            with open(dest_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)
    except requests.exceptions.RequestException as e:
        return TRANSIENT, f"{type(e).__name__}: {_short(e)}", {}
    except OSError as e:
        return TRANSIENT, f"local IO error: {_short(e)}", {}

    size = os.path.getsize(dest_path)
    with open(dest_path, "rb") as fh:
        head = fh.read(_HEAD_BYTES).decode("utf-8", "ignore")
        fh.seek(max(0, size - _TAIL_BYTES))
        tail = fh.read().decode("utf-8", "ignore")

    base = _BASE_RE.search(head)
    meta = {"bytes": size, "osm_base": base.group(1) if base else None}
    if expect_area_id is not None:
        meta["has_area"] = bool(re.search(
            rf'"type"\s*:\s*"area"\s*,\s*"id"\s*:\s*{int(expect_area_id)}\b', head))

    if not head.lstrip().startswith("{"):
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", head)).strip()[:300]
        low = text.lower()
        if "timed out" in low or "out of memory" in low:
            return TOO_BIG, text, meta
        return TRANSIENT, f"non-JSON response: {text}", meta

    m = _REMARK_RE.search(tail)
    remark = m.group(1) if m else ""
    if remark:
        low = remark.lower()
        if "timed out" in low or "out of memory" in low:
            return TOO_BIG, remark, meta
        if "error" in low:
            return TRANSIENT, remark, meta

    if not _CLOSED_RE.search(tail):
        return TRANSIENT, f"truncated response ({size:,} bytes)", meta
    return OK, remark, meta


def overpass_json(query: str, purpose: str, max_attempts: int = 8, exclude=(), with_url: bool = False):
    """Small Overpass query → parsed JSON (and the endpoint used), retried across endpoints."""
    path = os.path.join(TMP_DIR, f"q_{uuid.uuid4().hex}.json")
    last = "no attempt made"
    try:
        for attempt in range(1, max_attempts + 1):
            if STOP.is_set():
                raise RuntimeError("stop requested")
            url = POOL.acquire(CFG["max_wait_for_endpoint_s"], STOP, exclude)
            kind, detail, _ = overpass_fetch_to_file(url, query, path)
            POOL.release(url, kind)
            if kind == OK:
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                return (data, url) if with_url else data
            last = f"{kind} @ {_host(url)}: {detail}"
            if kind == FATAL:
                break
            STOP.wait(min(5 * attempt, 60))
    finally:
        _silent_remove(path)
    raise RuntimeError(f"Overpass {purpose} failed: {last}")


# ── Nominatim: max 1 request/second across all threads (usage policy) ───────
_NOMI_LOCK = threading.Lock()
_NOMI_LAST = [0.0]


def nominatim_get(endpoint: str, params: dict, attempts: int = 3):
    url = f"{CFG['nominatim_url'].rstrip('/')}/{endpoint}"
    for i in range(1, attempts + 1):
        with _NOMI_LOCK:
            gap = _NOMI_LAST[0] + 1.2 - time.time()
            if gap > 0:
                time.sleep(gap)
            _NOMI_LAST[0] = time.time()
        try:
            r = _session().get(url, params=params, timeout=(20, 180))
            if r.status_code == 200:
                return r.json()
            if r.status_code not in (429, 500, 502, 503, 504):
                log.warning(f"Nominatim {endpoint} HTTP {r.status_code}")
                return None
        except (requests.exceptions.RequestException, ValueError) as e:
            log.warning(f"Nominatim {endpoint} error: {_short(e)}")
        time.sleep(10 * i)
    return None


print("✅ HTTP layer defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Country resolution (ISO code → OSM boundary relation → area + bbox)

# COMMAND ----------

@dataclass
class CountryGeo:
    bbox: tuple                        # (south, west, north, east)
    relation_id: Optional[int] = None
    area_id: Optional[int] = None      # None → bbox-only queries
    outline: object = None             # simplified shapely outline (only fetched when needed)
    source: str = ""
    prune_geom: object = None          # prepared outline, set only when tile pruning is on


# Last resort when OSM has no boundary relation for the code.
FALLBACK_BBOX = {
    "AQ": (-90.0, -180.0, -60.0, 180.0),
}


def q_country_relation(iso2: str) -> str:
    return f"""[out:json][timeout:120];
(
  rel["ISO3166-1"="{iso2}"];
  rel["ISO3166-1:alpha2"="{iso2}"];
);
out tags bb;"""


def _pick_relation(elements: list, iso2: str):
    rels = [e for e in elements if e.get("type") == "relation"]
    if not rels:
        return None

    def rank(e):
        t = e.get("tags", {})
        try:
            lvl = int(t.get("admin_level", 99))
        except (TypeError, ValueError):
            lvl = 99
        return (0 if t.get("ISO3166-1") == iso2 else 1,
                0 if t.get("boundary") == "administrative" else 1,
                lvl, e["id"])

    return sorted(rels, key=rank)[0]


def area_exists(area_id: int) -> bool:
    """True if at least one endpoint has built this area (mirrors refresh areas at different times;
    tiles automatically avoid endpoints that lack it)."""
    tried = set()
    while len(tried) < len(POOL.urls):
        data, url = overpass_json(f"[out:json][timeout:60];area({area_id});out ids;",
                                  purpose=f"area {area_id} check", exclude=tried, with_url=True)
        if any(e.get("type") == "area" for e in data.get("elements", [])):
            return True
        tried.add(url)
    return False


def _geom_from_nominatim(item: dict):
    gj = item.get("geojson")
    if not gj or gj.get("type") not in ("Polygon", "MultiPolygon"):
        return None
    try:
        g = shape(gj)
        if not g.is_valid:
            g = make_valid(g)
        return None if g.is_empty else g
    except Exception:
        return None


def nominatim_outline(rel_id: int):
    """Simplified outline of a relation (used only to skip ocean tiles for big/spread-out countries)."""
    data = nominatim_get("lookup", {
        "osm_ids": f"R{rel_id}", "format": "json",
        "polygon_geojson": 1, "polygon_threshold": CFG["nominatim_polygon_threshold"],
    })
    return _geom_from_nominatim(data[0]) if data else None


def _name_variants(c: dict) -> list:
    names = []
    for n in (c.get("common_name"), c.get("name"), c.get("official_name")):
        if not n:
            continue
        cands = [n]
        if "," in n:   # "Congo, The Democratic Republic of the" → "Democratic Republic of the Congo"
            head, tail = [p.strip() for p in n.split(",", 1)]
            cands += [re.sub(r"^the\s+", "", f"{tail} {head}", flags=re.I), head]
        for x in cands:
            if x not in names:
                names.append(x)
    return names


def nominatim_find_country(c: dict):
    """Fallback when no relation carries the ISO code: search by name variants."""
    for q in _name_variants(c):
        data = nominatim_get("search", {
            "q": q, "format": "json", "limit": 5,
            "polygon_geojson": 1, "polygon_threshold": CFG["nominatim_polygon_threshold"],
        })
        for item in data or []:
            if item.get("osm_type") != "relation" or item.get("class") != "boundary":
                continue
            rank = item.get("place_rank")
            if rank is not None and int(rank) > 8:
                continue
            return int(item["osm_id"]), _geom_from_nominatim(item)
    return None


def _normalize_bbox(b: tuple) -> tuple:
    s, w, n, e = (float(x) for x in b)
    s, n = max(-90.0, min(s, n)), min(90.0, max(s, n))
    w, e = max(-180.0, min(w, e)), min(180.0, max(w, e))
    if n - s < 1e-3:
        s, n = s - 0.01, n + 0.01
    if e - w < 1e-3:
        w, e = w - 0.01, e + 0.01
    return (s, w, n, e)


def resolve_country(c: dict) -> Optional[CountryGeo]:
    """Returns None only when OSM genuinely has nothing for this code.
    Network/Overpass failures raise, so the country is marked FAILED (and retried), not UNRESOLVED."""
    iso2 = c["iso2"]
    data = overpass_json(q_country_relation(iso2), purpose=f"{iso2} boundary lookup")
    rel = _pick_relation(data.get("elements", []), iso2)

    rel_id, bbox, outline, source = None, None, None, ""
    if rel is not None:
        rel_id = int(rel["id"])
        b = rel.get("bounds")
        if b:
            bbox = (b["minlat"], b["minlon"], b["maxlat"], b["maxlon"])
        source = f"relation {rel_id} (ISO3166-1)"
    else:
        found = nominatim_find_country(c)
        if found:
            rel_id, outline = found
            source = f"relation {rel_id} (Nominatim name match)"

    if bbox is None and rel_id is not None and outline is None:
        outline = nominatim_outline(rel_id)
    if bbox is None and outline is not None:
        minx, miny, maxx, maxy = outline.bounds
        bbox = (miny, minx, maxy, maxx)
    if bbox is None and iso2 in FALLBACK_BBOX:
        bbox, source = FALLBACK_BBOX[iso2], "hard-coded bbox"
    if bbox is None:
        return None

    area_id = None
    if rel_id is not None:
        cand = AREA_ID_OFFSET + rel_id
        if area_exists(cand):
            area_id = cand
        else:
            log.warning(f"[{iso2}] Overpass area {cand} missing — bbox-only query (may include neighbours' roads).")
            source += "; area missing → bbox-only"

    return CountryGeo(bbox=_normalize_bbox(bbox), relation_id=rel_id, area_id=area_id,
                      outline=outline, source=source)


print("✅ Country resolution defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tiling and the per-tile Overpass query

# COMMAND ----------

@dataclass(frozen=True)
class Tile:
    tile_id: str
    s: float
    w: float
    n: float
    e: float
    depth: int = 0


def initial_tiles(bbox: tuple, deg: float) -> list:
    """Grid aligned to a global lattice (stable tile ids across runs), clipped to the bbox."""
    s, w, n, e = bbox
    tiles = []
    for i in range(math.floor(s / deg), math.ceil(n / deg)):
        for j in range(math.floor(w / deg), math.ceil(e / deg)):
            ts, tn = max(s, i * deg), min(n, (i + 1) * deg)
            tw, te = max(w, j * deg), min(e, (j + 1) * deg)
            if tn - ts > 1e-9 and te - tw > 1e-9:
                tiles.append(Tile(f"g{deg:g}_{i}_{j}", ts, tw, tn, te, 0))
    return tiles


def split_tile(t: Tile) -> list:
    ms, mw = (t.s + t.n) / 2.0, (t.w + t.e) / 2.0
    quads = [(t.s, t.w, ms, mw), (t.s, mw, ms, t.e), (ms, t.w, t.n, mw), (ms, mw, t.n, t.e)]
    return [Tile(f"{t.tile_id}.{k}", *q, t.depth + 1) for k, q in enumerate(quads)]


def prune_tiles(tiles: list, geo: CountryGeo) -> list:
    if geo.prune_geom is None:
        return list(tiles)
    m = CFG["prune_margin_deg"]
    return [t for t in tiles if geo.prune_geom.intersects(box(t.w - m, t.s - m, t.e + m, t.n + m))]


def plan_tiles(tiles: list, prior: dict, geo: CountryGeo) -> list:
    """Skip tiles already done in a previous (interrupted) run; expand tiles that were split."""
    todo, stack = [], list(tiles)
    while stack:
        t = stack.pop()
        st = prior.get(t.tile_id)
        if st == "done":
            continue
        if st == "split" and t.depth < CFG["max_split_depth"]:
            stack.extend(prune_tiles(split_tile(t), geo))
            continue
        todo.append(t)
    return sorted(todo, key=lambda t: t.tile_id)


def build_tile_query(t: Tile, area_id: Optional[int], since: str) -> str:
    head = f"[out:json][timeout:{int(CFG['overpass_timeout_s'])}]"
    if CFG["overpass_maxsize_bytes"]:
        head += f"[maxsize:{int(CFG['overpass_maxsize_bytes'])}]"
    bbox = f"{t.s:.7f},{t.w:.7f},{t.n:.7f},{t.e:.7f}"
    way = f'way["highway"~"^({HIGHWAY_REGEX})$"](newer:"{since}")'
    if area_id:
        # ".c out ids" echoes the area first, so an endpoint that lacks the area is detected
        # instead of silently returning an empty result.
        return f"""{head};
area({area_id})->.c;
.c out ids;
{way}(area.c)({bbox});
out meta geom qt;"""
    return f"""{head};
{way}({bbox});
out meta geom qt;"""


print("✅ Tiling defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Streaming parser → Spark → Delta (staging)

# COMMAND ----------

_geod_tls = threading.local()


def _geod() -> Geod:
    g = getattr(_geod_tls, "g", None)
    if g is None:
        g = _geod_tls.g = Geod(ellps="WGS84")
    return g


# Columns produced in Python for each way
INPUT_SCHEMA = T.StructType([
    T.StructField("osm_id", T.LongType()),
    T.StructField("osm_version", T.LongType()),
    T.StructField("osm_timestamp_raw", T.StringType()),
    T.StructField("highway", T.StringType()),
    T.StructField("highway_class", T.StringType()),
    T.StructField("surface_tag", T.StringType()),
    T.StructField("surface_type", T.StringType()),
    T.StructField("road_name", T.StringType()),
    T.StructField("ref", T.StringType()),
    T.StructField("lanes_raw", T.StringType()),
    T.StructField("maxspeed_raw", T.StringType()),
    T.StructField("maxspeed_kmh", T.DoubleType()),
    T.StructField("oneway", T.StringType()),
    T.StructField("bridge", T.BooleanType()),
    T.StructField("tunnel", T.BooleanType()),
    T.StructField("access", T.StringType()),
    T.StructField("length_m", T.DoubleType()),
    T.StructField("geometry_wkt", T.StringType()),
])
INPUT_COLS = INPUT_SCHEMA.names

# Final column layout (staging has tile_id too; per-country tables drop it)
STAGING_COLUMNS = [
    ("osm_id", "BIGINT"), ("osm_version", "BIGINT"), ("osm_timestamp", "TIMESTAMP"),
    ("country_iso2", "STRING"), ("country_iso3", "STRING"), ("country_name", "STRING"),
    ("highway", "STRING"), ("highway_class", "STRING"),
    ("surface_tag", "STRING"), ("surface_type", "STRING"),
    ("road_name", "STRING"), ("ref", "STRING"), ("lanes", "INT"),
    ("maxspeed_raw", "STRING"), ("maxspeed_kmh", "DOUBLE"),
    ("oneway", "STRING"), ("bridge", "BOOLEAN"), ("tunnel", "BOOLEAN"), ("access", "STRING"),
    ("length_m", "DOUBLE"), ("length_km", "DOUBLE"), ("geometry_wkt", "STRING"),
    ("since_date", "STRING"), ("run_id", "STRING"), ("tile_id", "STRING"), ("extracted_at", "TIMESTAMP"),
]


def _new_buffer() -> dict:
    return {k: [] for k in INPUT_COLS}


def _append_way(buf: dict, el: dict) -> bool:
    geom = el.get("geometry") or []
    lons, lats = [], []
    for p in geom:
        if p:
            lon, lat = p.get("lon"), p.get("lat")
            if lon is not None and lat is not None:
                lons.append(float(lon))
                lats.append(float(lat))
    if len(lons) < 2:
        return False
    tags = el.get("tags") or {}
    hw = (tags.get("highway") or "").strip()
    if not hw:
        return False
    surf = tags.get("surface")
    ms = tags.get("maxspeed")

    buf["osm_id"].append(int(el["id"]))
    buf["osm_version"].append(int(el.get("version") or 0))
    buf["osm_timestamp_raw"].append(el.get("timestamp"))
    buf["highway"].append(hw)
    buf["highway_class"].append(classify_highway(hw))
    buf["surface_tag"].append(surf)
    buf["surface_type"].append(classify_surface(surf, hw))
    buf["road_name"].append(tags.get("name"))
    buf["ref"].append(tags.get("ref"))
    buf["lanes_raw"].append(tags.get("lanes"))
    buf["maxspeed_raw"].append(ms)
    buf["maxspeed_kmh"].append(parse_maxspeed(ms))
    buf["oneway"].append(tags.get("oneway", "no"))
    buf["bridge"].append(_flag(tags.get("bridge")))
    buf["tunnel"].append(_flag(tags.get("tunnel")))
    buf["access"].append(tags.get("access"))
    # Geodesic length on WGS84 (EPSG:3857 lengths are inflated by 1/cos(latitude))
    buf["length_m"].append(round(float(_geod().line_length(lons, lats)), 2))
    buf["geometry_wkt"].append("LINESTRING (" + ", ".join(f"{x} {y}" for x, y in zip(lons, lats)) + ")")
    return True


def shape_rows(sdf, country: dict, since: str, run_id: str, tile_id: Optional[str]):
    """Input rows → staging layout (typed, ordered)."""
    sdf = (sdf
           .withColumn("osm_timestamp", F.expr("try_cast(osm_timestamp_raw AS TIMESTAMP)"))
           .withColumn("lanes", F.expr("try_cast(lanes_raw AS INT)"))
           .withColumn("maxspeed_kmh", F.when(F.isnan("maxspeed_kmh"), F.lit(None))
                                        .otherwise(F.col("maxspeed_kmh")))
           .withColumn("length_km", F.round(F.col("length_m") / F.lit(1000.0), 4))
           .withColumn("country_iso2", F.lit(country["iso2"]))
           .withColumn("country_iso3", F.lit(country["iso3"]))
           .withColumn("country_name", F.lit(country["name"]))
           .withColumn("since_date", F.lit(since))
           .withColumn("run_id", F.lit(run_id))
           .withColumn("tile_id", F.lit(tile_id))
           .withColumn("extracted_at", F.current_timestamp()))
    return sdf.select(*[F.col(n).cast(t).alias(n) for n, t in STAGING_COLUMNS])


_CONCURRENCY_MARKERS = ("Concurrent", "DELTA_CONCURRENT", "MetadataChanged", "ProtocolChanged")


def delta_retry(fn, what: str, attempts: int = 6):
    """Retry Delta operations on optimistic-concurrency conflicts."""
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if i < attempts and any(m in str(e) for m in _CONCURRENCY_MARKERS):
                log.info(f"{what}: concurrent commit, retrying ({i}/{attempts})")
                time.sleep(2 * i + random.random() * 3)
                continue
            raise


def write_rows(buf: dict, country: dict, since: str, run_id: str, tile_id: str) -> None:
    pdf = pd.DataFrame(buf, columns=INPUT_COLS)
    sdf = spark.createDataFrame(pdf, schema=INPUT_SCHEMA)
    out = shape_rows(sdf, country, since, run_id, tile_id)
    delta_retry(lambda: out.write.format("delta").mode("append").saveAsTable(STAGING_TABLE),
                f"[{country['iso2']}] staging append")


def parse_and_write(path: str, country: dict, tile: Tile, since: str, run_id: str) -> int:
    """Stream-parse the saved Overpass JSON and append rows in batches. Returns rows written."""
    batch = CFG["write_batch_rows"]
    buf, n_buf, total = _new_buffer(), 0, 0
    with open(path, "rb") as fh:
        for el in ijson.items(fh, "elements.item", use_float=True):
            if el.get("type") != "way":
                continue
            if _append_way(buf, el):
                n_buf += 1
                if n_buf >= batch:
                    write_rows(buf, country, since, run_id, tile.tile_id)
                    total += n_buf
                    buf, n_buf = _new_buffer(), 0
                    gc.collect()
    if n_buf:
        write_rows(buf, country, since, run_id, tile.tile_id)
        total += n_buf
    return total


print("✅ Parser and writer defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Bookkeeping tables (staging, tile checkpoints, run log)

# COMMAND ----------

TILE_LOG_COLUMNS = [
    ("country_iso2", "STRING"), ("job_key", "STRING"), ("tile_id", "STRING"), ("status", "STRING"),
    ("depth", "INT"), ("row_count", "BIGINT"), ("attempts", "INT"),
    ("south", "DOUBLE"), ("west", "DOUBLE"), ("north", "DOUBLE"), ("east", "DOUBLE"),
    ("endpoint", "STRING"), ("detail", "STRING"), ("osm_base", "STRING"),
    ("run_id", "STRING"), ("logged_at", "TIMESTAMP"),
]
RUN_LOG_COLUMNS = [
    ("run_id", "STRING"), ("country_iso2", "STRING"), ("country_iso3", "STRING"),
    ("country_name", "STRING"), ("table_name", "STRING"), ("since_date", "STRING"),
    ("status", "STRING"), ("row_count", "BIGINT"),
    ("tiles_done", "INT"), ("tiles_split", "INT"), ("tiles_failed", "INT"), ("failed_tiles", "STRING"),
    ("resolution", "STRING"), ("relation_id", "BIGINT"), ("error", "STRING"),
    ("started_at", "TIMESTAMP"), ("finished_at", "TIMESTAMP"), ("duration_s", "DOUBLE"),
]

_SQL2SPARK = {"STRING": T.StringType(), "BIGINT": T.LongType(), "INT": T.IntegerType(),
              "DOUBLE": T.DoubleType(), "TIMESTAMP": T.TimestampType(), "BOOLEAN": T.BooleanType()}


def _struct(cols) -> T.StructType:
    return T.StructType([T.StructField(n, _SQL2SPARK[t], True) for n, t in cols])


def _ddl(cols) -> str:
    return ",\n  ".join(f"{n} {t}" for n, t in cols)


TILE_LOG_SCHEMA = _struct(TILE_LOG_COLUMNS)
RUN_LOG_SCHEMA = _struct(RUN_LOG_COLUMNS)


def ensure_tables() -> None:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
  {_ddl(STAGING_COLUMNS)}
) USING DELTA PARTITIONED BY (country_iso2)
COMMENT 'Scratch rows for OSM road extraction; emptied per country once its table is published.'""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {TILE_LOG_TABLE} (
  {_ddl(TILE_LOG_COLUMNS)}
) USING DELTA PARTITIONED BY (country_iso2)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
  {_ddl(RUN_LOG_COLUMNS)}
) USING DELTA""")


def write_tile_log(rows: list) -> None:
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=TILE_LOG_SCHEMA)
    delta_retry(lambda: df.write.format("delta").mode("append").saveAsTable(TILE_LOG_TABLE), "tile log append")


def write_run_log(rec: dict) -> None:
    def conv(name, sql_type):
        v = rec.get(name)
        if v is None:
            return None
        if sql_type in ("BIGINT", "INT"):
            return int(v)
        if sql_type == "DOUBLE":
            return float(v)
        if sql_type == "STRING":
            return str(v)
        return v
    row = tuple(conv(n, t) for n, t in RUN_LOG_COLUMNS)
    df = spark.createDataFrame([row], schema=RUN_LOG_SCHEMA)
    delta_retry(lambda: df.write.format("delta").mode("append").saveAsTable(RUN_LOG_TABLE), "run log append")


def load_tile_state(iso2: str, job_key: str):
    """→ ({tile_id: latest status}, settings_mismatch)"""
    rows = (spark.table(TILE_LOG_TABLE)
            .where(F.col("country_iso2") == iso2)
            .select("job_key", "tile_id", "status", "logged_at")
            .collect())
    if not rows:
        return {}, False
    if any(r["job_key"] != job_key for r in rows):
        return {}, True
    latest = {}
    for r in sorted(rows, key=lambda r: r["logged_at"]):
        latest[r["tile_id"]] = r["status"]
    return latest, False


def wipe_country_work(iso2: str) -> None:
    # Tile log first: if we stop between the two deletes, the next run re-queries tiles
    # rather than trusting "done" markers whose rows are gone.
    delta_retry(lambda: spark.sql(f"DELETE FROM {TILE_LOG_TABLE} WHERE country_iso2 = '{iso2}'"),
                f"[{iso2}] tile log delete")
    delta_retry(lambda: spark.sql(f"DELETE FROM {STAGING_TABLE} WHERE country_iso2 = '{iso2}'"),
                f"[{iso2}] staging delete")


def completed_countries(since: str) -> set:
    """ISO2 codes whose latest attempt for this since_date ended SUCCESS/EMPTY."""
    w = Window.partitionBy("country_iso2").orderBy(F.col("finished_at").desc())
    rows = (spark.table(RUN_LOG_TABLE)
            .where(F.col("since_date") == since)
            .withColumn("_rn", F.row_number().over(w))
            .where((F.col("_rn") == 1) & F.col("status").isin("SUCCESS", "EMPTY"))
            .select("country_iso2", "table_name", "status")
            .collect())
    done = set()
    for r in rows:
        if r["status"] == "EMPTY" and not CFG["create_empty_tables"]:
            done.add(r["country_iso2"])
        elif spark.catalog.tableExists(r["table_name"]):
            done.add(r["country_iso2"])
    return done


def finalize_country(c: dict, since: str, run_id: str) -> int:
    """Publish staging rows as the country table (deduplicated — a way crossing tile edges
    is returned by every tile it touches), then clear the country's scratch data."""
    iso2, table = c["iso2"], c["table"]
    final_df = (spark.table(STAGING_TABLE)
                .where(F.col("country_iso2") == iso2)
                .dropDuplicates(["osm_id"])
                .drop("tile_id"))
    delta_retry(lambda: (final_df.write.format("delta").mode("overwrite")
                         .option("overwriteSchema", "true").saveAsTable(table)),
                f"[{iso2}] publish {table}")
    n = spark.table(table).count()
    if n == 0 and not CFG["create_empty_tables"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    else:
        try:
            label = c["name"].replace("'", "")
            spark.sql(f"COMMENT ON TABLE {table} IS "
                      f"'OSM highway ways in {label} ({iso2}) edited since {since}. run_id={run_id}'")
        except Exception:
            pass
    wipe_country_work(iso2)
    return n


print("✅ Bookkeeping defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Per-tile and per-country pipeline

# COMMAND ----------

@dataclass
class TileResult:
    tile: Tile
    status: str                 # done | split | failed
    rows: int = 0
    attempts: int = 0
    endpoint: str = ""
    detail: str = ""
    osm_base: str = ""
    children: list = field(default_factory=list)


def process_tile(c: dict, geo: CountryGeo, tile: Tile, since: str, run_id: str) -> TileResult:
    query = build_tile_query(tile, geo.area_id, since)
    path = os.path.join(TMP_DIR, f"{c['iso2']}_{uuid.uuid4().hex}.json")
    exclude, attempts, last = set(), 0, ""
    try:
        while attempts < CFG["max_tile_attempts"]:
            if STOP.is_set():
                return TileResult(tile, "failed", attempts=attempts, detail="stop requested")
            attempts += 1
            try:
                url = POOL.acquire(CFG["max_wait_for_endpoint_s"], STOP, exclude)
            except NoEndpointAvailable as e:
                return TileResult(tile, "failed", attempts=attempts, detail=str(e)[:1000])

            kind, detail, meta = overpass_fetch_to_file(url, query, path, geo.area_id)
            if kind == OK and geo.area_id and not meta.get("has_area"):
                kind, detail = AREA_MISSING, f"area {geo.area_id} not present on this endpoint"
            POOL.release(url, kind)
            host = _host(url)

            if kind == OK:
                try:
                    n = parse_and_write(path, c, tile, since, run_id)
                except (ijson.JSONError, ValueError) as e:
                    # Corrupt/truncated body. Rows already appended are harmless: the
                    # country is deduplicated on osm_id when it is published.
                    last = f"parse error @ {host}: {_short(e)}"
                    continue
                return TileResult(tile, "done", rows=n, attempts=attempts, endpoint=host,
                                  detail=detail, osm_base=meta.get("osm_base") or "")

            if kind == TOO_BIG and tile.depth < CFG["max_split_depth"]:
                return TileResult(tile, "split", attempts=attempts, endpoint=host,
                                  detail=detail, children=split_tile(tile))
            if kind == FATAL:
                return TileResult(tile, "failed", attempts=attempts, endpoint=host, detail=detail)
            if kind == AREA_MISSING:
                exclude.add(url)

            last = f"{kind} @ {host}: {detail}"
            STOP.wait(min(5 * attempts, 60))
        return TileResult(tile, "failed", attempts=attempts, detail=last[:1000])
    finally:
        _silent_remove(path)


def _tile_log_row(iso2: str, job_key: str, run_id: str, r: TileResult) -> tuple:
    t = r.tile
    return (iso2, job_key, t.tile_id, r.status, int(t.depth), int(r.rows), int(r.attempts),
            float(t.s), float(t.w), float(t.n), float(t.e),
            r.endpoint or None, (r.detail or "")[:1000] or None, r.osm_base or None,
            run_id, datetime.now(timezone.utc))


def run_tiles(c: dict, geo: CountryGeo, todo: list, since: str, run_id: str, job_key: str) -> dict:
    iso2 = c["iso2"]
    workers = max(1, CFG["tile_workers_per_country"])
    queue = deque(todo)
    stats = {"done": 0, "split": 0, "rows": 0, "failed": []}
    pending_log = []
    last_progress = time.time()

    def flush_log():
        nonlocal pending_log
        if pending_log:
            try:
                write_tile_log(pending_log)
            except Exception as e:   # checkpoint loss only affects resume, never the data
                log.warning(f"[{iso2}] tile log write failed: {_short(e)}")
            pending_log = []

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"{iso2}-tile") as tex:
        running = {}
        while queue or running:
            while queue and len(running) < workers and not STOP.is_set():
                t = queue.popleft()
                running[tex.submit(process_tile, c, geo, t, since, run_id)] = t
            if not running:
                break
            finished, _ = wait(list(running), return_when=FIRST_COMPLETED)
            for fut in finished:
                t = running.pop(fut)
                try:
                    res = fut.result()
                except Exception as e:
                    res = TileResult(t, "failed", detail=f"{type(e).__name__}: {_short(e, 900)}")
                pending_log.append(_tile_log_row(iso2, job_key, run_id, res))
                if res.status == "done":
                    stats["done"] += 1
                    stats["rows"] += res.rows
                elif res.status == "split":
                    stats["split"] += 1
                    queue.extend(prune_tiles(res.children, geo))
                    log.debug(f"[{iso2}] {t.tile_id} too big ({res.detail[:80]}) → split")
                else:
                    stats["failed"].append(res)
                    log.warning(f"[{iso2}] tile {t.tile_id} FAILED after {res.attempts} attempts: {res.detail[:200]}")
            if len(pending_log) >= CFG["tile_log_flush_every"]:
                flush_log()
            if time.time() - last_progress >= 120:
                log.info(f"[{iso2}] progress: {stats['done']} tiles done, {stats['split']} split, "
                         f"{len(stats['failed'])} failed, {len(queue)} queued | {stats['rows']:,} rows staged")
                last_progress = time.time()
    flush_log()

    for t in queue:   # only non-empty if STOP was set
        stats["failed"].append(TileResult(t, "failed", detail="not run (stop requested)"))
    return stats


def run_country(c: dict, run_id: str, since: str, mode: str) -> dict:
    """Never raises. Always writes one row to the run log."""
    iso2 = c["iso2"]
    t0, started = time.time(), datetime.now(timezone.utc)
    job_key = f"since={since}|deg={CFG['initial_tile_deg']:g}"
    rec = dict(run_id=run_id, country_iso2=iso2, country_iso3=c["iso3"], country_name=c["name"],
               table_name=c["table"], since_date=since, status="FAILED", row_count=0,
               tiles_done=0, tiles_split=0, tiles_failed=0, failed_tiles=None,
               resolution=None, relation_id=None, error=None, started_at=started)
    try:
        if STOP.is_set():
            raise RuntimeError("stop requested before start")

        geo = resolve_country(c)
        if geo is None:
            rec.update(status="UNRESOLVED", error="No OSM boundary relation or bbox found for this ISO code")
            return rec
        rec.update(resolution=geo.source, relation_id=geo.relation_id)

        prior = {}
        if mode == "resume":
            prior, mismatch = load_tile_state(iso2, job_key)
            if mismatch:
                log.info(f"[{iso2}] Earlier partial work used other settings — starting this country fresh.")
                wipe_country_work(iso2)
                prior = {}
        else:
            wipe_country_work(iso2)

        tiles = initial_tiles(geo.bbox, CFG["initial_tile_deg"])
        n_grid = len(tiles)
        if n_grid > CFG["prune_min_tiles"]:
            if geo.outline is None and geo.relation_id is not None:
                geo.outline = nominatim_outline(geo.relation_id)
            if geo.outline is not None:
                geo.prune_geom = prep(geo.outline)
                pruned = prune_tiles(tiles, geo)
                if pruned:
                    tiles = pruned
                else:
                    geo.prune_geom = None

        todo = plan_tiles(tiles, prior, geo)
        n_prior_done = sum(1 for s in prior.values() if s == "done")
        log.info(f"[{iso2}] {c['name']} — {geo.source}; {len(tiles)} tiles"
                 f"{f' (of {n_grid} in bbox)' if len(tiles) != n_grid else ''}, {len(todo)} to run"
                 f"{f', {n_prior_done} already done' if n_prior_done else ''}")

        stats = run_tiles(c, geo, todo, since, run_id, job_key)
        rec.update(tiles_done=stats["done"] + n_prior_done, tiles_split=stats["split"],
                   tiles_failed=len(stats["failed"]))

        if stats["failed"]:
            ids = [r.tile.tile_id for r in stats["failed"]]
            rec.update(status="FAILED", failed_tiles=",".join(ids)[:4000],
                       error=f"{len(ids)} tile(s) failed (progress kept for resume); last: "
                             f"{stats['failed'][-1].detail}"[:2000])
            return rec

        n = finalize_country(c, since, run_id)
        rec.update(row_count=n, status="SUCCESS" if n > 0 else "EMPTY")
        return rec

    except Exception as e:
        rec.update(status="FAILED", error=f"{type(e).__name__}: {_short(e, 1900)}")
        log.error(f"[{iso2}] FAILED: {rec['error']}")
        return rec
    finally:
        rec["finished_at"] = datetime.now(timezone.utc)
        rec["duration_s"] = round(time.time() - t0, 1)
        try:
            write_run_log(rec)
        except Exception as e:
            log.error(f"[{iso2}] could not write run log: {_short(e)}")
        gc.collect()


print("✅ Pipeline defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Pre-flight: which Overpass endpoints are reachable from this cluster?

# COMMAND ----------

# Tiny query against the Germany area: proves the endpoint is reachable, speaks JSON and has areas.
AREA_PROBE_QUERY = "[out:json][timeout:25];area(3600051477);out ids;"


def probe_endpoint(url: str):
    reason = "unknown"
    for _ in range(3):
        try:
            r = _session().post(url, data={"data": AREA_PROBE_QUERY}, timeout=(10, 60))
            if r.status_code == 200:
                if _ANY_AREA_RE.search(r.text):
                    return True, "ok"
                return False, "reachable, but no area support"
            reason = f"HTTP {r.status_code}"
        except requests.exceptions.RequestException as e:
            reason = f"{type(e).__name__}: {_short(e, 150)}"
        time.sleep(5)
    return False, reason


def build_pool() -> EndpointPool:
    print("Overpass endpoints:")
    good = []
    for url in CFG["overpass_endpoints"]:
        ok, why = probe_endpoint(url)
        print(f"  {'✅' if ok else '❌'} {_host(url):<28} {why}")
        if ok:
            good.append(url)
    if not good:
        raise RuntimeError(
            "No Overpass endpoint is reachable from this cluster. Check outbound internet access "
            "(firewall / NAT / proxy), then re-run: with mode=resume nothing already done is repeated.")
    return EndpointPool(good, CFG["slots_per_endpoint"], CFG["min_interval_per_endpoint_s"])


ensure_tables()
POOL = build_pool()
_nomi = nominatim_get("status", {"format": "json"}, attempts=1)
print(f"Nominatim: {'✅ reachable' if _nomi else '⚠️ unreachable (only used for large-country outlines / fallbacks)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Run the extraction
# MAGIC Each country is published to its own table as soon as it completes. Nothing is kept on the driver.

# COMMAND ----------

STATUS_ICON = {"SUCCESS": "✅", "EMPTY": "⚪", "FAILED": "❌", "UNRESOLVED": "❓"}
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]


def run_countries(countries: list, mode: str, label: str) -> dict:
    results = {}
    total = len(countries)
    if not total:
        print(f"[{label}] Nothing to do.")
        return results
    print(f"[{label}] {total} countries | {COUNTRY_WORKERS} in parallel | since {SINCE_DATE} | mode={mode} | run_id={RUN_ID}")
    print("-" * 100)
    t0 = time.time()
    ex = ThreadPoolExecutor(max_workers=COUNTRY_WORKERS, thread_name_prefix="country")
    try:
        futs = {ex.submit(run_country, c, RUN_ID, SINCE_DATE, mode): c for c in countries}
        for i, fut in enumerate(as_completed(futs), 1):
            c = futs[fut]
            try:
                rec = fut.result()
            except Exception as e:
                rec = {"status": "FAILED", "error": repr(e), "row_count": 0, "duration_s": 0}
            results[c["iso2"]] = rec
            st = rec["status"]
            info = (f"{rec.get('row_count') or 0:>12,} rows" if st in ("SUCCESS", "EMPTY")
                    else (rec.get("error") or "")[:110])
            print(f"  [{i:>3}/{total}] {STATUS_ICON.get(st, '?')} {c['iso2']}  "
                  f"{c['table'].split('.')[-1]:<42} {info}  ({(rec.get('duration_s') or 0) / 60:.1f} min)")
    except BaseException:
        STOP.set()
        print("⏹  Stop requested — letting in-flight requests finish (can take a few minutes)…")
        raise
    finally:
        ex.shutdown(wait=True, cancel_futures=True)

    counts = pd.Series([r["status"] for r in results.values()]).value_counts().to_dict()
    print("-" * 100)
    print(f"[{label}] finished in {(time.time() - t0) / 60:.1f} min — {counts}")
    return results


STOP.clear()
todo_countries = COUNTRIES
if MODE == "resume":
    already = completed_countries(SINCE_DATE)
    todo_countries = [c for c in COUNTRIES if c["iso2"] not in already]
    print(f"Resume: {len(COUNTRIES) - len(todo_countries)} countries already complete for since={SINCE_DATE}; "
          f"{len(todo_countries)} to run.")

RESULTS = run_countries(todo_countries, MODE, "pass 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Retry pass for failed countries (keeps completed tiles)

# COMMAND ----------

failed = [c for c in todo_countries if RESULTS.get(c["iso2"], {}).get("status") == "FAILED"]
if failed and CFG["final_retry_pass"] and not STOP.is_set():
    print(f"{len(failed)} failed: {[c['iso2'] for c in failed]}")
    print(f"Cooling down {CFG['final_retry_cooldown_s']}s, then re-checking endpoints and retrying…")
    time.sleep(CFG["final_retry_cooldown_s"])
    POOL = build_pool()
    RESULTS.update(run_countries(failed, "resume", "retry"))
else:
    print("No retry needed." if not failed else "Retry pass disabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Run summary
# MAGIC Anything still `FAILED` keeps its finished tiles: re-run the notebook with `mode = resume` and only the missing tiles are fetched.

# COMMAND ----------

summary_df = (spark.table(RUN_LOG_TABLE)
              .where(F.col("run_id") == RUN_ID)
              .withColumn("_rn", F.row_number().over(
                  Window.partitionBy("country_iso2").orderBy(F.col("finished_at").desc())))
              .where("_rn = 1").drop("_rn")
              .orderBy("status", "country_iso2"))
display(summary_df)

final_counts = pd.Series([r["status"] for r in RESULTS.values()]).value_counts().to_dict() if RESULTS else {}
print(f"Final status counts: {final_counts}")
still_failed = sorted(k for k, v in RESULTS.items() if v["status"] == "FAILED")
if still_failed:
    print(f"Still failed (re-run with mode=resume): {still_failed}")

try:
    for f_ in os.listdir(TMP_DIR):
        _silent_remove(os.path.join(TMP_DIR, f_))
except OSError:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Optional: all-countries view and summaries (computed in Spark, not pandas)
# MAGIC Border-crossing ways appear in both neighbours' tables; deduplicate on `osm_id` if you need a global total.

# COMMAND ----------

published = (spark.table(RUN_LOG_TABLE)
             .where(F.col("since_date") == SINCE_DATE)
             .withColumn("_rn", F.row_number().over(
                 Window.partitionBy("country_iso2").orderBy(F.col("finished_at").desc())))
             .where((F.col("_rn") == 1) & F.col("status").isin("SUCCESS", "EMPTY"))
             .select("table_name").collect())
tables = sorted({r["table_name"] for r in published if spark.catalog.tableExists(r["table_name"])})

if tables:
    union_sql = "\nUNION ALL\n".join(f"SELECT * FROM {t}" for t in tables)
    spark.sql(f"CREATE OR REPLACE VIEW {ALL_VIEW} AS\n{union_sql}")
    print(f"✅ View {ALL_VIEW} over {len(tables)} country tables")
else:
    print("No published country tables yet.")

# COMMAND ----------

if tables:
    display(spark.sql(f"""
        SELECT country_iso2, country_name, highway_class, surface_type,
               COUNT(*)                                                         AS road_count,
               ROUND(SUM(length_km), 2)                                         AS total_km,
               ROUND(AVG(length_km), 4)                                         AS avg_km,
               ROUND(100 * AVG(CASE WHEN road_name    IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_named,
               ROUND(100 * AVG(CASE WHEN maxspeed_kmh IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_with_speed
        FROM {ALL_VIEW}
        GROUP BY country_iso2, country_name, highway_class, surface_type
        ORDER BY country_iso2, highway_class
    """))

# COMMAND ----------

if tables:
    display(spark.sql(f"""
        SELECT country_iso2, country_name,
               SUM(CASE WHEN surface_type = 'paved'   THEN 1 ELSE 0 END)                  AS segments_paved,
               SUM(CASE WHEN surface_type = 'unpaved' THEN 1 ELSE 0 END)                  AS segments_unpaved,
               SUM(CASE WHEN surface_type = 'unknown' THEN 1 ELSE 0 END)                  AS segments_unknown,
               ROUND(SUM(CASE WHEN surface_type = 'paved'   THEN length_km ELSE 0 END), 2) AS total_km_paved,
               ROUND(SUM(CASE WHEN surface_type = 'unpaved' THEN length_km ELSE 0 END), 2) AS total_km_unpaved,
               ROUND(SUM(CASE WHEN surface_type = 'unknown' THEN length_km ELSE 0 END), 2) AS total_km_unknown,
               COUNT(*)                                                                   AS total_segments,
               ROUND(100 * AVG(CASE WHEN surface_type = 'paved' THEN 1.0 ELSE 0.0 END), 1) AS pct_paved
        FROM {ALL_VIEW}
        GROUP BY country_iso2, country_name
        ORDER BY total_km_paved DESC
    """))
