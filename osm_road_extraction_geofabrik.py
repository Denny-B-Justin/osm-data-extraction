# Databricks notebook source
# MAGIC %md
# MAGIC # OSM roads from Geofabrik extracts → one table and one CSV per country (World Bank boundaries)
# MAGIC
# MAGIC Reads every `*.osm.pbf` file in `/Volumes/prd_mega/saiana95/vaiana95/OSM/geofabrik/`, splits the roads by the
# MAGIC **World Bank official boundaries** in `/Volumes/prd_mega/saiana95/vaiana95/OSM/wb_boundaries/`
# MAGIC (`wb_admin0.geojson` for countries; `wb_admin1/2.geojson` add province/district columns) and produces:
# MAGIC
# MAGIC | Output | Where |
# MAGIC |---|---|
# MAGIC | All roads, all countries (partitioned by `country_iso3`) | `prd_mega.saiana95.osm_road_geofabrik` |
# MAGIC | One table per country | `prd_mega.saiana95.<country>_osm_road` |
# MAGIC | One CSV per country (`_part001…` above 1M rows) | `/Volumes/prd_mega/saiana95/vaiana95/OSM/extraction/osm_road_csv/` |
# MAGIC | Run log | `prd_mega.saiana95.osm_road_geofabrik_log` |
# MAGIC
# MAGIC **No pip installs.** Everything runs on libraries already in the Databricks Runtime (numpy, pandas, pyarrow,
# MAGIC PySpark). Point-in-polygon, geodesic length and WKT are implemented here instead of Shapely/pyproj.
# MAGIC The only external tool is **osmium-tool** (found on the cluster, restored from a copy in the Volume, or installed).
# MAGIC
# MAGIC **How it works**
# MAGIC 1. The WB boundaries are loaded into an in-memory scanline index (exact point-in-polygon, fast for millions of points).
# MAGIC 2. **osmium** (C++, on the driver) cuts each continent file down to roads and streams them as GeoJSON lines into
# MAGIC    Python, where Arrow parses them and numpy computes length, country (every vertex is tested, so a road crossing a
# MAGIC    border belongs to both countries), province and district. Results go to a staging folder as Parquet.
# MAGIC 3. **Spark** removes roads that appear in two continent files, builds WKT and the attributes, writes the Delta
# MAGIC    tables and exports all CSVs in one distributed job.
# MAGIC
# MAGIC Every stage is checkpointed: re-run with `mode = resume` to skip finished work.
# MAGIC
# MAGIC **Cluster:** the heavy lifting runs on the driver: give it plenty of cores, ≥ 64 GB RAM and a large local disk
# MAGIC (≈ 0.7 × total size of the `.osm.pbf` files free on `/local_disk0`). A few workers are enough for the Spark part.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

dbutils.widgets.text("since_date", "2024-01-01T00:00:00Z", "1. Ways edited since (UTC; blank = whole network)")
dbutils.widgets.text("countries", "", "2. ISO3/ISO2 codes to (re)publish (blank = all)")
dbutils.widgets.dropdown("mode", "resume", ["resume", "force"], "3. Mode")
dbutils.widgets.text("extract_workers", "2", "4. PBF files processed in parallel")

# COMMAND ----------

import glob
import hashlib
import io
import json
import logging
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import unicodedata
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pajson
import pyarrow.parquet as pq

from pyspark.sql import functions as F

# ── Widgets ────────────────────────────────────────────────────────────────────
SINCE_DATE      = dbutils.widgets.get("since_date").strip()
COUNTRY_FILTER  = [x.strip().upper() for x in dbutils.widgets.get("countries").split(",") if x.strip()]
MODE            = dbutils.widgets.get("mode").strip().lower()
EXTRACT_WORKERS = max(1, int(dbutils.widgets.get("extract_workers").strip() or "2"))

if SINCE_DATE and not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", SINCE_DATE):
    raise ValueError(f"since_date must look like 2024-01-01T00:00:00Z (or be blank), got {SINCE_DATE!r}")
if MODE not in ("resume", "force"):
    raise ValueError(f"mode must be 'resume' or 'force', got {MODE!r}")
SINCE_EPOCH = (int(datetime.strptime(SINCE_DATE, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
               if SINCE_DATE else None)

# ── Configuration ──────────────────────────────────────────────────────────────
CFG = {
    # Inputs / outputs
    "source_dir":     "/Volumes/prd_mega/saiana95/vaiana95/OSM/geofabrik",
    "source_pattern": "*.osm.pbf",
    "catalog_schema": "prd_mega.saiana95",
    "csv_dir":        "/Volumes/prd_mega/saiana95/vaiana95/OSM/extraction/osm_road_csv",
    "staging_dir":    "/Volumes/prd_mega/saiana95/vaiana95/OSM/extraction/_staging_geofabrik",

    # World Bank boundaries. admin0 defines the countries; admin1/admin2 are optional (set to None to skip).
    "boundaries": {
        0: "/Volumes/prd_mega/saiana95/vaiana95/OSM/wb_boundaries/wb_admin0 (1).geojson",
        1: "/Volumes/prd_mega/saiana95/vaiana95/OSM/wb_boundaries/wb_admin1.geojson",
        2: "/Volumes/prd_mega/saiana95/vaiana95/OSM/wb_boundaries/wb_admin2.geojson",
    },
    # Attribute names in the GeoJSON files. None = detect automatically (the detected names are printed).
    "boundary_fields": {
        0: {"iso3": None, "iso2": None, "name": None},
        1: {"code": None, "name": None, "iso3": None},
        2: {"code": None, "name": None, "iso3": None},
    },
    "boundary_snap_deg": {0: 1e-5, 1: 1e-4, 2: 1e-4},   # vertex rounding (~1 m / ~11 m) to drop redundant detail
    "boundary_grid":     {0: 480, 1: 240, 2: 120},       # index resolution in cells per degree
    "snap_km": 2.0,   # roads with no vertex inside any country (piers, causeways, coastal generalisation) go to the
                      # nearest country within this distance (boundary_match = 'nearest'); 0 = never

    # Tables
    "master_table":         "osm_road_geofabrik",    # all countries, partitioned by country_iso3
    "log_table":            "osm_road_geofabrik_log",
    "table_suffix":         "_osm_road",
    "table_naming":         "name",    # "name" → table from the WB country name | "iso3" → e.g. bol_osm_road
    "country_objects":      "table",   # "table" | "view" (no second copy of the data) | "none"
    "create_empty_outputs": True,      # countries with 0 matching roads still get an empty table + header-only CSV

    # CSV
    "csv_export":            True,
    "csv_max_rows_per_file": 1_000_000,  # Excel's row limit → big countries get _part001, _part002, …; None = 1 file
    "csv_compression":       None,       # None or "gzip"

    # Driver-side processing
    "local_dir":        None,                  # None → /local_disk0/osm_geofabrik (or the system temp dir)
    "node_index":       "sparse_file_array",   # node-location store for osmium export; "flex_mem" = RAM (faster)
    "read_chunk_mb":    256,                   # size of each GeoJSON chunk parsed at once
    "keep_local_files": False,                 # keep the filtered .osm.pbf files on local disk after use

    # osmium-tool
    "osmium_path":      None,   # explicit path to an osmium binary, if you have one
    "tools_cache_dir":  "/Volumes/prd_mega/saiana95/vaiana95/OSM/tools",   # a working install is copied here and
                                                                            # reused by clusters without internet

    "allow_partial": False,   # publish even if some PBF files failed (their countries would be incomplete)
}
TABLE_FORMAT = "delta"
STAGING_VERSION = "wb-2"   # bump to invalidate staged roads when the staging format changes

HIGHWAY_TAGS = [
    "motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
    "secondary", "secondary_link", "tertiary", "tertiary_link", "residential", "living_street",
    "unclassified", "service", "pedestrian", "busway", "track", "path",
    "footway", "cycleway", "bridleway", "road",
]
EXPORT_TAGS = ["highway", "surface", "name", "ref", "lanes", "maxspeed", "oneway", "bridge", "tunnel", "access"]


def fq(name: str) -> str:
    return f"{CFG['catalog_schema']}.{name}"


MASTER_TABLE = fq(CFG["master_table"])
LOG_TABLE    = fq(CFG["log_table"])
DUP_TABLE    = fq("_osm_road_geofabrik_dups")   # scratch

STAGING       = CFG["staging_dir"].rstrip("/")
ROADS_STAGING = f"{STAGING}/roads"
MARKERS       = f"{STAGING}/_markers"
CSV_DIR       = CFG["csv_dir"].rstrip("/")


def _pick_local_dir() -> str:
    bases = [CFG["local_dir"]] if CFG["local_dir"] else [os.path.join(b, "osm_geofabrik")
                                                         for b in ("/local_disk0", tempfile.gettempdir())]
    for d in bases:
        try:
            os.makedirs(d, exist_ok=True)
            probe = os.path.join(d, ".probe")
            open(probe, "w").close()
            os.remove(probe)
            return d
        except OSError:
            continue
    raise RuntimeError("No writable local directory found.")


LOCAL_DIR = _pick_local_dir()
TOOLS_DIR = os.path.join(LOCAL_DIR, "tools")
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]

log = logging.getLogger("osm_geofabrik")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))
    log.addHandler(_h)

for d in (ROADS_STAGING, MARKERS, CSV_DIR):
    os.makedirs(d, exist_ok=True)
for leftover in (f"{STAGING}/boundaries", f"{STAGING}/country_boundaries.parquet"):   # from the OSM-boundary version
    if os.path.exists(leftover):
        shutil.rmtree(leftover, ignore_errors=True) if os.path.isdir(leftover) else os.remove(leftover)

try:   # on by default on serverless / shared clusters, where setting it may be refused
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
except Exception:
    pass

_free_gb = shutil.disk_usage(LOCAL_DIR).free / 1e9
print("✅ Configuration loaded")
print(f"   python / numpy / pandas / pyarrow : {platform.python_version()} / {np.__version__} / "
      f"{pd.__version__} / {pa.__version__}")
print(f"   since_date       : {SINCE_DATE or '(none — whole network)'}")
print(f"   countries        : {COUNTRY_FILTER or 'ALL'}")
print(f"   mode             : {MODE}")
print(f"   extract_workers  : {EXTRACT_WORKERS}")
print(f"   tables           : {CFG['catalog_schema']}.<country>{CFG['table_suffix']}  (+ {MASTER_TABLE})")
print(f"   csv              : {CSV_DIR}")
print(f"   local work dir   : {LOCAL_DIR}  ({_free_gb:,.0f} GB free)")
print(f"   run_id           : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tools: osmium, checkpoints, source files

# COMMAND ----------

def _osmium_version(path: str) -> Optional[str]:
    try:
        out = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=60).stdout
        m = re.search(r"osmium version (\S+)", out)
        return m.group(1) if m else None
    except (OSError, subprocess.SubprocessError):
        return None


def ensure_osmium() -> str:
    """Find osmium-tool; else restore the copy saved in the Volume; else install it (apt-get with root/sudo, or
    micromamba from conda-forge without root) and save a copy in the Volume for clusters without internet."""
    if CFG["osmium_path"]:
        if _osmium_version(CFG["osmium_path"]):
            return CFG["osmium_path"]
        raise RuntimeError(f"CFG['osmium_path'] = {CFG['osmium_path']} is not a working osmium binary.")
    env_dir = os.path.join(TOOLS_DIR, "env")
    conda_bin = os.path.join(env_dir, "bin", "osmium")
    for cand in (shutil.which("osmium"), conda_bin):
        if cand and os.path.exists(cand) and _osmium_version(cand):
            return cand

    arch = {"x86_64": "linux-64", "aarch64": "linux-aarch64"}.get(platform.machine(), platform.machine())
    cache = os.path.join(CFG["tools_cache_dir"], f"osmium-env-{arch}.tar.gz") if CFG["tools_cache_dir"] else None
    if cache and os.path.exists(cache):
        print(f"Restoring osmium from {cache} …")
        os.makedirs(TOOLS_DIR, exist_ok=True)
        with tarfile.open(cache, "r:gz") as tf:
            tf.extractall(TOOLS_DIR)
        if _osmium_version(conda_bin):
            return conda_bin
        print("   the saved copy does not run here; trying to install instead.")

    errors = []
    if os.geteuid() == 0 or subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0:
        sudo = [] if os.geteuid() == 0 else ["sudo", "-n"]
        env = dict(os.environ, DEBIAN_FRONTEND="noninteractive")
        print("Installing osmium-tool with apt-get …")
        subprocess.run(sudo + ["apt-get", "update", "-qq"], env=env, capture_output=True)
        r = subprocess.run(sudo + ["apt-get", "install", "-y", "-qq", "osmium-tool"], env=env,
                           capture_output=True, text=True)
        if r.returncode == 0 and shutil.which("osmium"):
            return shutil.which("osmium")
        errors.append(f"apt-get: {r.stderr.strip()[-300:]}")

    try:
        import requests
        os.makedirs(TOOLS_DIR, exist_ok=True)
        mm = os.path.join(TOOLS_DIR, "micromamba")
        if not os.path.exists(mm):
            print("Downloading micromamba …")
            resp = requests.get(f"https://micro.mamba.pm/api/micromamba/{arch}/latest", timeout=600)
            resp.raise_for_status()
            with tarfile.open(fileobj=io.BytesIO(resp.content), mode="r:bz2") as tf:
                with tf.extractfile(tf.getmember("bin/micromamba")) as src, open(mm, "wb") as dst:
                    shutil.copyfileobj(src, dst)
            os.chmod(mm, 0o755)
        print("Installing osmium-tool from conda-forge …")
        r = subprocess.run([mm, "create", "-y", "-q", "-r", os.path.join(TOOLS_DIR, "mamba"),
                            "-p", env_dir, "-c", "conda-forge", "osmium-tool"], capture_output=True, text=True)
        if r.returncode == 0 and _osmium_version(conda_bin):
            if cache:
                try:
                    os.makedirs(CFG["tools_cache_dir"], exist_ok=True)
                    tmp = os.path.join(LOCAL_DIR, os.path.basename(cache))
                    with tarfile.open(tmp, "w:gz") as tf:
                        tf.add(env_dir, arcname="env")
                    shutil.copyfile(tmp, cache)
                    os.remove(tmp)
                    print(f"   saved a copy to {cache}")
                except Exception as e:
                    print(f"   (could not save a copy to the Volume: {e})")
            return conda_bin
        errors.append(f"conda-forge: {r.stderr.strip()[-300:]}")
    except Exception as e:
        errors.append(f"conda-forge: {type(e).__name__}: {e}")
    raise RuntimeError(
        "osmium-tool is not available and could not be installed:\n  - " + "\n  - ".join(errors) +
        f"\nOptions: run this notebook once on a cluster with internet access (a copy is then saved to "
        f"{CFG['tools_cache_dir']}), ask your platform team to add 'osmium-tool' to the cluster (apt package or init "
        "script), or set CFG['osmium_path'] to an existing osmium binary.")


def run_cmd(cmd: list, what: str) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{what} failed (exit {r.returncode}): {(r.stderr or r.stdout).strip()[-2000:]}")
    return r.stdout


# ── Checkpoint markers (small JSON files in the staging folder) ───────────────
def _marker_path(kind: str, name: str) -> str:
    return f"{MARKERS}/{kind}__{name}.json"


def read_marker(kind: str, name: str) -> Optional[dict]:
    try:
        with open(_marker_path(kind, name)) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def write_marker(kind: str, name: str, payload: dict) -> None:
    with open(_marker_path(kind, name), "w") as fh:
        json.dump(dict(payload, written_at=datetime.now(timezone.utc).isoformat()), fh)


def rm_tree(path: str) -> None:
    try:
        dbutils.fs.rm(path, True)
    except Exception:
        shutil.rmtree(path, ignore_errors=True)


# ── Source files ───────────────────────────────────────────────────────────────
@dataclass
class Extract:
    name: str
    path: str
    size: int
    mtime: int
    snapshot: str

    @property
    def signature(self) -> str:
        return hashlib.sha1(f"{self.path}|{self.size}|{self.mtime}".encode()).hexdigest()[:12]


def discover_extracts() -> list:
    out = []
    for p in sorted(glob.glob(os.path.join(CFG["source_dir"], CFG["source_pattern"]))):
        name = re.sub(r"(-latest)?\.osm\.pbf$", "", os.path.basename(p))
        name = re.sub(r"[^A-Za-z0-9_-]+", "_", name)
        st = os.stat(p)
        snap = ""
        try:
            snap = run_cmd([OSMIUM, "fileinfo", "-g", "header.option.osmosis_replication_timestamp", p],
                           "fileinfo").strip()
        except Exception:
            pass
        if not snap:
            snap = datetime.fromtimestamp(st.st_mtime, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out.append(Extract(name, p, st.st_size, int(st.st_mtime), snap))
    if len({e.name for e in out}) != len(out):
        raise ValueError("Two source files map to the same extract name; rename one of them.")
    return sorted(out, key=lambda e: -e.size)   # biggest first


OSMIUM = ensure_osmium()
print(f"✅ osmium {_osmium_version(OSMIUM)} at {OSMIUM}")

EXTRACTS = discover_extracts()
if not EXTRACTS:
    raise RuntimeError(f"No {CFG['source_pattern']} files in {CFG['source_dir']}")
display(pd.DataFrame([{"extract": e.name, "file": os.path.basename(e.path), "size_gb": round(e.size / 1e9, 2),
                       "data_up_to": e.snapshot} for e in EXTRACTS]))
_need_gb = 0.7 * sum(e.size for e in EXTRACTS) / 1e9
if _free_gb < _need_gb:
    print(f"⚠️  Local disk has {_free_gb:,.0f} GB free; roughly {_need_gb:,.0f} GB may be needed at peak. "
          "Use a driver with a bigger local disk, or lower extract_workers.")

if MODE == "force":
    print("mode=force → discarding all checkpoints and staged data")
    rm_tree(MARKERS)
    rm_tree(ROADS_STAGING)
    for d in (ROADS_STAGING, MARKERS):
        os.makedirs(d, exist_ok=True)
    for f in glob.glob(os.path.join(LOCAL_DIR, "*.osm.pbf")) + glob.glob(os.path.join(LOCAL_DIR, "*.idx")):
        os.remove(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. World Bank boundaries → point-in-polygon index (numpy only)

# COMMAND ----------

_REF_DY = 0.2871            # each scanline sits at this fraction inside its row (never on a round coordinate)
_KM_PER_DEG_LAT = 110.574
_KM_PER_DEG_LON = 111.320   # at the equator


def iter_geojson_features(path: str):
    """Yield the features of a GeoJSON FeatureCollection one at a time (keeps memory low for big files)."""
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    m = re.search(r'"features"\s*:\s*\[', text)
    if not m:
        obj = json.loads(text)
        yield from (obj.get("features", []) if isinstance(obj, dict) else obj)
        return
    dec, pos, n = json.JSONDecoder(), m.end(), len(text)
    while True:
        while pos < n and text[pos] in " \t\r\n,":
            pos += 1
        if pos >= n or text[pos] == "]":
            return
        feat, pos = dec.raw_decode(text, pos)
        yield feat


def geometry_rings(geom: dict):
    """All rings (outer and holes) of a (Multi)Polygon / GeometryCollection as (n, 2) float arrays."""
    gtype = geom.get("type")
    if gtype == "GeometryCollection":
        for g in geom.get("geometries") or []:
            yield from geometry_rings(g)
        return
    polys = [geom.get("coordinates")] if gtype == "Polygon" else geom.get("coordinates") if gtype == "MultiPolygon" else []
    for poly in polys or []:
        for ring in poly or []:
            a = np.asarray(ring, dtype=np.float64)
            if a.ndim == 2 and a.shape[0] >= 3 and a.shape[1] >= 2:
                yield a[:, :2]


def _snap_ring(a: np.ndarray, q: Optional[float]):
    if q:
        a = np.round(a / q) * q
    keep = np.ones(len(a), dtype=bool)
    keep[1:] = np.any(a[1:] != a[:-1], axis=1)
    a = a[keep]
    if len(a) < 3:
        return None
    if a[0, 0] != a[-1, 0] or a[0, 1] != a[-1, 1]:
        a = np.vstack([a, a[:1]])
    return a if len(a) >= 4 else None


class PolygonIndex:
    """Exact point-in-polygon for many polygons at once, using only numpy.

    For every scanline row (`res` rows per degree) the index stores the x-intervals where each unit is inside,
    measured along a reference line in that row (even–odd rule, so holes and enclaves work). A point takes the unit
    of the interval it falls in on its row's reference line, then flips membership once for every boundary edge
    crossed by the short vertical step from the point to that line. Only edges registered in the point's grid cell
    can be crossed, so each lookup costs a binary search plus a handful of edge tests near boundaries.
    """

    def __init__(self, name: str, rings: list, ring_unit: list, n_units: int, res: int, dtype=np.float64):
        t0 = time.time()
        self.name, self.res, self.n_units = name, int(res), max(1, int(n_units))
        self.ncol, self.nrow = 360 * self.res, 180 * self.res
        lens = np.fromiter((len(r) for r in rings), dtype=np.int64, count=len(rings))
        pts = np.concatenate(rings).astype(dtype, copy=False) if rings else np.zeros((0, 2), dtype)
        last = np.cumsum(lens) - 1
        keep = np.ones(len(pts), dtype=bool)
        keep[last] = False
        i = np.flatnonzero(keep)
        ex1, ey1, ex2, ey2 = pts[i, 0], pts[i, 1], pts[i + 1, 0], pts[i + 1, 1]
        eu = np.repeat(np.asarray(ring_unit, dtype=np.int32), lens)[i]
        del pts, keep, i
        wrap = np.abs(ex2.astype(np.float64) - ex1) > 180    # edges jumping across the antimeridian
        ok = ((ex1 != ex2) | (ey1 != ey2)) & ~wrap
        self.antimeridian_edges = int(wrap.sum())
        self.ex1, self.ey1, self.ex2, self.ey2, self.eu = ex1[ok], ey1[ok], ex2[ok], ey2[ok], eu[ok]
        self.n_edges = len(self.eu)
        self._build_intervals()
        self._build_cells()
        self.build_seconds = time.time() - t0

    # ── grid helpers ──
    def _rows_of(self, y):
        return np.clip(np.floor((np.asarray(y, np.float64) + 90.0) * self.res), 0, self.nrow - 1).astype(np.int64)

    def _cols_of(self, x):
        return np.clip(np.floor((np.asarray(x, np.float64) + 180.0) * self.res), 0, self.ncol - 1).astype(np.int64)

    def _yref(self, rows):
        return -90.0 + (rows + _REF_DY) / self.res

    # ── build ──
    def _build_intervals(self):
        R = self.res
        y1 = self.ey1.astype(np.float64)
        y2 = self.ey2.astype(np.float64)
        r_lo = np.maximum(np.ceil((np.minimum(y1, y2) + 90.0) * R - _REF_DY), 0).astype(np.int64)
        r_hi = np.minimum(np.ceil((np.maximum(y1, y2) + 90.0) * R - _REF_DY) - 1, self.nrow - 1).astype(np.int64)
        n = np.maximum(r_hi - r_lo + 1, 0)
        e = np.repeat(np.arange(self.n_edges), n)
        rows = np.repeat(r_lo, n) + (np.arange(len(e)) - np.repeat(np.cumsum(n) - n, n))
        x1 = self.ex1[e].astype(np.float64)
        xc = x1 + (self._yref(rows) - y1[e]) * (self.ex2[e] - x1) / (y2[e] - y1[e])
        uc = self.eu[e]
        del e, x1
        order = np.lexsort((xc, uc, rows))
        rows, uc, xc = rows[order], uc[order], xc[order]
        g = rows * self.n_units + uc
        new = np.ones(len(g), dtype=bool)
        new[1:] = g[1:] != g[:-1]
        gstart = np.flatnonzero(new)
        gid = np.cumsum(new) - 1
        cnt = np.diff(np.append(gstart, len(g)))
        pos = np.arange(len(g)) - gstart[gid]
        self.odd_rows = int(np.sum(cnt % 2))       # rings that are not closed on some rows (bad geometry)
        s = np.flatnonzero((pos % 2 == 0) & (pos + 1 < cnt[gid]))
        iv_row, iv_x0, iv_x1, iv_u = rows[s], xc[s], xc[s + 1], uc[s]
        o = np.lexsort((iv_x0, iv_row))
        self.iv_row, self.iv_x0, self.iv_x1 = iv_row[o], iv_x0[o], iv_x1[o]
        self.iv_u = iv_u[o].astype(np.int32)
        self.iv_key = self.iv_row * 1000.0 + (self.iv_x0 + 180.0)       # used by nearest()
        # Elementary segments: cut every row at all interval ends, so overlapping units (slivers, duplicates,
        # enclaves drawn without a hole) each keep their own entry for the same stretch of the scanline.
        k0 = self.iv_key
        k1 = self.iv_row * 1000.0 + (self.iv_x1 + 180.0)
        ek, first = np.unique(np.concatenate([k0, k1]), return_index=True)
        ex = np.concatenate([self.iv_x0, self.iv_x1])[first]
        ia, ib = np.searchsorted(ek, k0), np.searchsorted(ek, k1)
        n = np.maximum(ib - ia, 0)
        seg = np.repeat(ia, n) + (np.arange(int(n.sum())) - np.repeat(np.cumsum(n) - n, n))
        su = np.repeat(self.iv_u, n)
        o = np.lexsort((su, seg))
        seg, su = seg[o], su[o]
        self.seg_key, self.seg_x1, self.seg_u = ek[seg], ex[seg + 1], su
        self.seg_row = np.repeat(self.iv_row, n)[o]
        self.overlaps = int(np.sum(seg[1:] == seg[:-1]))                  # stretches covered by >1 unit

    def _build_cells(self, batch: int = 20_000_000):
        R, ncol = self.res, self.ncol
        dx = self.ex2.astype(np.float64) - self.ex1
        dy = self.ey2.astype(np.float64) - self.ey1
        k = np.maximum(1, np.ceil(np.maximum(np.abs(dx), np.abs(dy)) * R * 2)).astype(np.int64)
        csum = np.cumsum(k)
        cell_parts, edge_parts, start = [], [], 0
        while start < self.n_edges:
            stop = max(start + 1, int(np.searchsorted(csum, (csum[start - 1] if start else 0) + batch, "right")))
            kk = k[start:stop]
            e = np.repeat(np.arange(start, stop), kk)
            j = (np.arange(len(e)) - np.repeat(np.cumsum(kk) - kk, kk)).astype(np.float64)
            ke = np.repeat(kk, kk).astype(np.float64)
            xa = self.ex1[e] + dx[e] * (j / ke)
            xb = self.ex1[e] + dx[e] * ((j + 1) / ke)
            ya = self.ey1[e] + dy[e] * (j / ke)
            yb = self.ey1[e] + dy[e] * ((j + 1) / ke)
            c0, c1 = self._cols_of(np.minimum(xa, xb)), self._cols_of(np.maximum(xa, xb))
            r0, r1 = self._rows_of(np.minimum(ya, yb)), self._rows_of(np.maximum(ya, yb))
            mc, mr = c1 != c0, r1 != r0
            both = mc & mr
            cells = np.concatenate([r0 * ncol + c0, r0[mc] * ncol + c1[mc], r1[mr] * ncol + c0[mr],
                                    r1[both] * ncol + c1[both]])
            edges = np.concatenate([e, e[mc], e[mr], e[both]])
            o = np.lexsort((edges, cells))
            cells, edges = cells[o], edges[o]
            first = np.ones(len(cells), dtype=bool)
            first[1:] = (cells[1:] != cells[:-1]) | (edges[1:] != edges[:-1])
            cell_parts.append(cells[first])
            edge_parts.append(edges[first])
            start = stop
        cells = np.concatenate(cell_parts) if cell_parts else np.zeros(0, np.int64)
        edges = np.concatenate(edge_parts) if edge_parts else np.zeros(0, np.int64)
        o = np.argsort(cells, kind="stable")
        self.reg_cell = cells[o]
        self.reg_edge = edges[o].astype(np.int32 if self.n_edges < 2**31 else np.int64)

    def summary(self) -> dict:
        return {"edges": self.n_edges, "intervals": len(self.iv_key), "edge_cells": len(self.reg_cell),
                "overlapping_intervals": self.overlaps, "unclosed_rows": self.odd_rows,
                "antimeridian_edges_dropped": self.antimeridian_edges, "build_s": round(self.build_seconds, 1)}

    # ── queries ──
    def query(self, x, y, batch: int = 20_000_000):
        """→ (point index, unit) for every unit that contains each point (sorted by point)."""
        x = np.asarray(x, np.float64)
        y = np.asarray(y, np.float64)
        U = self.n_units
        if len(x) == 0 or len(self.seg_key) == 0:
            return np.zeros(0, np.int64), np.zeros(0, np.int32)
        r, c = self._rows_of(y), self._cols_of(x)
        yr = self._yref(r)
        i = np.searchsorted(self.seg_key, r * 1000.0 + (x + 180.0), side="right") - 1
        ii = np.maximum(i, 0)
        base = np.flatnonzero((i >= 0) & (self.seg_row[ii] == r) & (x < self.seg_x1[ii]))
        last = ii[base]
        m = last - np.searchsorted(self.seg_key, self.seg_key[last], side="left") + 1   # units on this stretch
        bp = np.repeat(base, m)
        be = np.repeat(last - m + 1, m) + (np.arange(int(m.sum())) - np.repeat(np.cumsum(m) - m, m))
        base_keys = np.unique(bp * U + self.seg_u[be])

        cell = r * self.ncol + c
        lo = np.searchsorted(self.reg_cell, cell, "left")
        n = np.searchsorted(self.reg_cell, cell, "right") - lo
        cand = np.flatnonzero(n)
        toggles = []
        if len(cand):
            csum = np.cumsum(n[cand])
            s = 0
            while s < len(cand):
                t = max(s + 1, int(np.searchsorted(csum, (csum[s - 1] if s else 0) + batch, "right")))
                pts = cand[s:t]
                nn = n[pts]
                p = np.repeat(pts, nn)
                ed = self.reg_edge[np.repeat(lo[pts], nn) + (np.arange(len(p)) - np.repeat(np.cumsum(nn) - nn, nn))]
                xa, xb = self.ex1[ed].astype(np.float64), self.ex2[ed].astype(np.float64)
                px = x[p]
                m = (xa > px) != (xb > px)                      # edge spans the point's x (half-open)
                p, ed, xa, xb, px = p[m], ed[m], xa[m], xb[m], px[m]
                ya, yb = self.ey1[ed].astype(np.float64), self.ey2[ed].astype(np.float64)
                ye = ya + (px - xa) * (yb - ya) / (xb - xa)
                hit = (ye > y[p]) != (ye > yr[p])               # crossed between the point and the reference line
                toggles.append(p[hit] * U + self.eu[ed[hit]])
                s = t
        tog = np.concatenate(toggles) if toggles else np.zeros(0, np.int64)
        if len(tog):
            uk, cnt = np.unique(tog, return_counts=True)
            tog = uk[cnt % 2 == 1]
        keys = np.setxor1d(base_keys, tog, assume_unique=True)
        return keys // U, (keys % U).astype(np.int32)

    def nearest(self, x, y, max_km: float):
        """→ (unit or -1, distance in km) of the closest polygon found along nearby scanlines within max_km."""
        x = np.asarray(x, np.float64)
        y = np.asarray(y, np.float64)
        best_u = np.full(len(x), -1, np.int32)
        best_d = np.full(len(x), np.inf)
        niv = len(self.iv_key)
        if len(x) == 0 or not max_km or max_km <= 0 or niv == 0:
            return best_u, best_d
        k = int(math.ceil(max_km / (_KM_PER_DEG_LAT / self.res))) + 1
        r0 = self._rows_of(y)
        kx = _KM_PER_DEG_LON * np.maximum(np.cos(np.radians(y)), 1e-6)
        for dr in range(-k, k + 1):
            rr = r0 + dr
            dy = np.abs(self._yref(rr) - y) * _KM_PER_DEG_LAT
            valid = (rr >= 0) & (rr < self.nrow) & (dy <= max_km)
            if not valid.any():
                continue
            i = np.searchsorted(self.iv_key, rr * 1000.0 + (x + 180.0), side="right") - 1
            for cand, left in ((i, True), (i + 1, False)):
                cc = np.clip(cand, 0, niv - 1)
                ok = valid & (cand >= 0) & (cand < niv) & (self.iv_row[cc] == rr)
                dxd = np.maximum(x - self.iv_x1[cc], 0.0) if left else np.maximum(self.iv_x0[cc] - x, 0.0)
                d = np.hypot(dxd * kx, dy)
                better = ok & (d < best_d)
                best_d = np.where(better, d, best_d)
                best_u = np.where(better, self.iv_u[cc], best_u)
        return np.where(best_d <= max_km, best_u, -1).astype(np.int32), best_d


# ── Loading the WB GeoJSON files ───────────────────────────────────────────────
_BAD_CODES = {"", "-99", "-1", "NA", "N/A", "NULL", "NONE", "NAN"}
_FIELD_CANDIDATES = {
    "iso3":  ["ISO_A3", "WB_A3", "ISO3", "ISO3CD", "ISO_3", "ADM0_A3", "GID_0"],
    "iso2":  ["ISO_A2", "WB_A2", "ISO2", "ISO_2"],
    "name0": ["NAM_0", "WB_NAME", "NAME_EN", "ADM0_NAME", "ADM0NM", "COUNTRY", "NAME_0", "NAME"],
    "code1": ["ADM1CD_c", "ADM1CD", "ADM1_CODE", "ADM1_PCODE", "GID_1", "HASC_1"],
    "name1": ["NAM_1", "ADM1NM", "ADM1_NAME", "NAME_1", "ADM1_EN", "NAME"],
    "code2": ["ADM2CD_c", "ADM2CD", "ADM2_CODE", "ADM2_PCODE", "GID_2", "HASC_2"],
    "name2": ["NAM_2", "ADM2NM", "ADM2_NAME", "NAME_2", "ADM2_EN", "NAME"],
}


def _clean(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return None if s.upper() in _BAD_CODES else s


def _present(keys, candidates) -> list:
    low = {k.lower(): k for k in keys}
    return [low[c.lower()] for c in candidates if c.lower() in low]


def _slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


@dataclass
class AdminLevel:
    level: int
    path: str
    fields: dict
    units: list         # one dict per unit (key, name, iso3, code/iso2)
    index: PolygonIndex
    code: np.ndarray    # object arrays with a trailing None, so index -1 → None
    name: np.ndarray
    iso3: np.ndarray

    def lookup(self, x, y) -> np.ndarray:
        """One unit per point (inside, else nearest within snap_km, else -1)."""
        x = np.asarray(x, np.float64)
        y = np.asarray(y, np.float64)
        out = np.full(len(x), -1, np.int64)
        p, u = self.index.query(x, y)
        out[p[::-1]] = u[::-1]                   # if units overlap, the first one wins
        miss = np.flatnonzero(out < 0)
        if len(miss) and CFG["snap_km"] > 0:
            out[miss] = self.index.nearest(x[miss], y[miss], CFG["snap_km"])[0]
        return out


def load_admin_level(level: int, path: str) -> AdminLevel:
    t0 = time.time()
    override = CFG["boundary_fields"].get(level, {}) or {}
    q = CFG["boundary_snap_deg"].get(level)
    unit_of, units, rings, ring_unit = {}, [], [], []
    fields, n_feat, dropped = None, 0, 0
    for n_feat, feat in enumerate(iter_geojson_features(path), 1):
        props = feat.get("properties") or {}
        if fields is None:
            keys = set(props)

            def pick(role, cand_key):
                if override.get(role):
                    if override[role] not in keys:
                        raise ValueError(f"admin{level}: field {override[role]!r} not in {sorted(keys)}")
                    return [override[role]]
                return _present(keys, _FIELD_CANDIDATES[cand_key])

            if level == 0:
                fields = {"iso3": pick("iso3", "iso3"), "iso2": pick("iso2", "iso2"), "name": pick("name", "name0")}
                if not fields["iso3"] and not fields["name"]:
                    raise ValueError(f"admin0: no country code or name field found in {sorted(keys)}; "
                                     "set CFG['boundary_fields'][0].")
            else:
                fields = {"code": pick("code", f"code{level}"), "name": pick("name", f"name{level}"),
                          "iso3": pick("iso3", "iso3")}
            print(f"   admin{level} fields → " + ", ".join(f"{k}: {v[0] if v else '—'}" for k, v in fields.items())
                  + f"   (all fields: {sorted(keys)})")

        def first(role):
            for f in fields.get(role) or []:
                v = _clean(props.get(f))
                if v is not None:
                    return v
            return None

        name = first("name")
        if level == 0:
            iso3 = first("iso3")
            key = (iso3 or f"X_{_slug(name)[:24] or n_feat}").upper()
            meta = {"key": key, "iso3": iso3, "iso2": first("iso2"), "name": name or key}
        else:
            code, iso3 = first("code"), first("iso3")
            key = code or f"{iso3 or '?'}|{name or n_feat}"
            meta = {"key": key, "code": code or key, "name": name, "iso3": iso3}
        u = unit_of.get(key)
        if u is None:
            u = unit_of[key] = len(units)
            units.append(meta)
        for ring in geometry_rings(feat.get("geometry") or {}):
            ring = _snap_ring(ring, q)
            if ring is None:
                dropped += 1
                continue
            rings.append(ring)
            ring_unit.append(u)
    if not units:
        raise ValueError(f"No features in {path}")
    t_read = time.time() - t0
    idx = PolygonIndex(f"admin{level}", rings, ring_unit, len(units), CFG["boundary_grid"][level],
                       np.float64 if level == 0 else np.float32)
    def col(c):
        arr = np.empty(len(units) + 1, dtype=object)
        arr[:-1] = [u.get(c) for u in units]
        return arr                                  # trailing None: index -1 → None
    lvl = AdminLevel(level, path, {k: (v[0] if v else None) for k, v in fields.items()}, units, idx,
                     col("code" if level else "key"), col("name"), col("iso3"))
    print(f"   admin{level}: {n_feat:,} features → {len(units):,} units, {len(rings):,} rings "
          f"({dropped:,} tiny rings dropped), read {t_read:.0f}s, index {idx.summary()}")
    return lvl


print("✅ Boundary functions defined.")

# COMMAND ----------

t0 = time.time()
ADMIN = {}
for _lvl, _path in sorted(CFG["boundaries"].items()):
    if not _path:
        continue
    if not os.path.exists(_path):
        if _lvl == 0:
            raise FileNotFoundError(f"Country boundaries not found: {_path}")
        print(f"⚠️  {_path} not found — admin{_lvl} columns will be empty.")
        continue
    try:
        ADMIN[_lvl] = load_admin_level(_lvl, _path)
    except Exception as e:
        if _lvl == 0:
            raise
        print(f"⚠️  admin{_lvl} could not be loaded ({type(e).__name__}: {e}) — its columns will be empty.")
for _lvl, _a in ADMIN.items():
    _s = _a.index.summary()
    if _s["overlapping_intervals"] or _s["unclosed_rows"] or _s["antimeridian_edges_dropped"]:
        print(f"ℹ️  admin{_lvl} geometry notes: {_s['overlapping_intervals']:,} overlaps, "
              f"{_s['unclosed_rows']:,} unclosed ring rows, {_s['antimeridian_edges_dropped']:,} antimeridian edges")

# ── Country list (from admin0) and table names ────────────────────────────────
COUNTRIES = []
_names_seen = Counter()
for r in ADMIN[0].units:
    base = r["key"].lower() if CFG["table_naming"] == "iso3" else (_slug(r["name"]) or r["key"].lower())
    COUNTRIES.append({"key": r["key"], "iso3": r["iso3"], "iso2": r["iso2"], "name": r["name"], "base": base})
    _names_seen[base] += 1
for c in COUNTRIES:
    if _names_seen[c["base"]] > 1:
        c["base"] = f"{c['base']}_{c['key'].lower()}"
    c["table"] = fq(f"{c['base']}{CFG['table_suffix']}")
COUNTRIES.sort(key=lambda c: c["key"])
COUNTRY_BY_KEY = {c["key"]: c for c in COUNTRIES}
COUNTRY_KEYS = ADMIN[0].code                    # unit index → country key (object array, trailing None)

_alias = {c["key"]: c["key"] for c in COUNTRIES}
_alias.update({c["iso2"].upper(): c["key"] for c in COUNTRIES if c["iso2"]})
_unknown = [x for x in COUNTRY_FILTER if x not in _alias]
if _unknown:
    print(f"⚠️  Codes not found in {os.path.basename(CFG['boundaries'][0])}: {_unknown}")
TARGET_KEYS = sorted({_alias[x] for x in COUNTRY_FILTER if x in _alias}) if COUNTRY_FILTER else \
              [c["key"] for c in COUNTRIES]


def _file_sig(p: str) -> str:
    st = os.stat(p)
    return f"{p}|{st.st_size}|{int(st.st_mtime)}"


BOUNDARY_FINGERPRINT = hashlib.sha1(json.dumps({
    "files": {lvl: _file_sig(a.path) for lvl, a in ADMIN.items()},
    "fields": {lvl: a.fields for lvl, a in ADMIN.items()},
    "snap": CFG["boundary_snap_deg"], "grid": CFG["boundary_grid"], "snap_km": CFG["snap_km"],
    "version": STAGING_VERSION}, sort_keys=True, default=str).encode()).hexdigest()[:16]

print(f"✅ {len(COUNTRIES)} countries/territories from admin0, {len(TARGET_KEYS)} to publish; "
      f"admin levels loaded: {sorted(ADMIN)}; {(time.time() - t0) / 60:.1f} min (fingerprint {BOUNDARY_FINGERPRINT})")
display(pd.DataFrame(COUNTRIES)[["key", "iso2", "name", "table"]].head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Road functions — osmium filter/export → Arrow → length, country, admin units → staging Parquet

# COMMAND ----------

def filtered_path(ex: Extract) -> str:
    return os.path.join(LOCAL_DIR, f"{ex.name}.{ex.signature}.filtered.osm.pbf")


def ensure_filtered(ex: Extract) -> str:
    """Road ways (and their nodes) → small local PBF, written under a temp name and renamed when complete."""
    out = filtered_path(ex)
    if os.path.exists(out):
        return out
    tmp = out.replace(".osm.pbf", ".tmp.osm.pbf")
    t0 = time.time()
    run_cmd([OSMIUM, "tags-filter", ex.path, f"w/highway={','.join(HIGHWAY_TAGS)}", "-o", tmp, "-O"],
            f"[{ex.name}] osmium tags-filter")
    os.replace(tmp, out)
    log.info(f"[{ex.name}] filtered {ex.size / 1e9:.1f} GB → {os.path.getsize(out) / 1e9:.2f} GB "
             f"in {(time.time() - t0) / 60:.1f} min")
    return out


def export_config_path() -> str:
    path = os.path.join(LOCAL_DIR, "osmium_export_roads.json")
    with open(path, "w") as fh:
        json.dump({
            "attributes": {"type": False, "id": True, "version": True, "changeset": False,
                           "timestamp": True, "uid": False, "user": False, "way_nodes": False},
            "linear_tags": True,     # closed highway ways (roundabouts) stay lines…
            "area_tags": False,      # …only area=yes ways (pedestrian squares) become areas and are dropped
            "include_tags": EXPORT_TAGS,
        }, fh)
    return path


_JSON_SCHEMA = pa.schema([
    pa.field("geometry", pa.struct([pa.field("coordinates", pa.list_(pa.list_(pa.float64())))])),
    pa.field("properties", pa.struct(
        [pa.field("@id", pa.int64()), pa.field("@version", pa.int64()), pa.field("@timestamp", pa.int64())]
        + [pa.field(k, pa.string()) for k in EXPORT_TAGS])),
])
_HIGHWAY_SET = pa.array(HIGHWAY_TAGS, pa.string())

# Arrow parses JSON in blocks. Arrow versions shipped with Databricks runtimes (e.g. pyarrow 14/15) refuse a block
# holding more than 100,000 lines ("ArrowInvalid: Exceeded maximum rows"). The shortest feature osmium writes is
# ~130 bytes, so 8 MB blocks stay below ~65,000 lines, while the longest way (2,000 nodes ≈ 60 KB) still fits.
_JSON_BLOCK_BYTES = 8 << 20
_JSON_PARSE_OPTS = pajson.ParseOptions(explicit_schema=_JSON_SCHEMA, unexpected_field_behavior="ignore")


def read_geojson_lines(data: bytes, block_bytes: int = _JSON_BLOCK_BYTES) -> pa.Table:
    """Newline-delimited GeoJSON → Arrow table. If Arrow still objects (row cap, or a line longer than a block),
    the buffer is split at a line break / the block enlarged and the pieces are parsed separately."""
    try:
        return pajson.read_json(pa.BufferReader(data),
                                read_options=pajson.ReadOptions(use_threads=True, block_size=block_bytes),
                                parse_options=_JSON_PARSE_OPTS)
    except pa.ArrowInvalid as e:
        msg = str(e).lower()
        if "straddl" in msg and block_bytes < len(data):          # a single line longer than the block
            return read_geojson_lines(data, block_bytes * 4)
        if "maximum rows" in msg and data.count(b"\n") >= 2:     # too many lines in one block
            mid = data.rfind(b"\n", 0, len(data) // 2) + 1 or data.find(b"\n") + 1
            return pa.concat_tables([read_geojson_lines(data[:mid], block_bytes),
                                     read_geojson_lines(data[mid:], block_bytes)])
        raise


_WGS84_A = 6378137.0
_WGS84_E2 = (1 / 298.257223563) * (2 - 1 / 298.257223563)


def segment_lengths_m(lon1, lat1, lon2, lat2):
    """WGS84 length of short segments from the local radii of curvature at mid-latitude
    (vs. exact geodesics: < 1e-7 relative error for 1 km segments, < 1e-4 for 50 km)."""
    phi = np.radians((lat1 + lat2) * 0.5)
    s = np.sin(phi)
    w = np.sqrt(1.0 - _WGS84_E2 * s * s)
    n = _WGS84_A / w
    m = _WGS84_A * (1.0 - _WGS84_E2) / (w * w * w)
    dlam = np.radians((lon2 - lon1 + 180.0) % 360.0 - 180.0)
    return np.hypot(n * np.cos(phi) * dlam, m * np.radians(lat2 - lat1))


_MATCH = np.array(["inside", "nearest", None], dtype=object)


def process_chunk(data: bytes, stats: Counter) -> Optional[pa.Table]:
    """One block of GeoJSON lines → Arrow table with one row per (way, country) and admin1/admin2 of the road."""
    if b"\x1e" in data:                       # RFC 8142 record separators
        data = data.replace(b"\x1e", b"")
    t = read_geojson_lines(data)
    stats["features"] += t.num_rows
    if t.num_rows == 0:
        return None
    props = t.column("properties").combine_chunks()
    tb = pa.table(dict(zip([f.name for f in props.type], props.flatten())))
    tb = tb.append_column("coords", t.column("geometry").combine_chunks().flatten()[0])
    n_pts = pc.fill_null(pc.list_value_length(tb["coords"]), 0)
    keep = pc.and_(pc.fill_null(pc.is_in(tb["highway"], value_set=_HIGHWAY_SET), False),
                   pc.greater_equal(n_pts, 2))
    tb = tb.filter(keep).combine_chunks()
    n = tb.num_rows
    if n == 0:
        return None
    coords = tb["coords"].chunk(0)
    counts = pc.list_value_length(coords).to_numpy(zero_copy_only=False).astype(np.int64)
    xy = np.ascontiguousarray(coords.flatten().flatten().to_numpy(zero_copy_only=False), dtype=np.float64)
    xy = xy.reshape(-1, 2)
    way_of_pt = np.repeat(np.arange(n), counts)
    first_pt = np.cumsum(counts) - counts
    mid_pt = first_pt + counts // 2

    # Length
    same = way_of_pt[1:] == way_of_pt[:-1]
    seg = segment_lengths_m(xy[:-1, 0], xy[:-1, 1], xy[1:, 0], xy[1:, 1])
    length_m = np.round(np.bincount(way_of_pt[1:][same], weights=seg[same], minlength=n), 2)

    # Countries: every vertex is tested, so a road crossing a border is assigned to each country it enters.
    A0 = ADMIN[0]
    U0 = A0.index.n_units
    vp, vu = A0.index.query(xy[:, 0], xy[:, 1])
    vw = way_of_pt[vp]
    key = vw * U0 + vu
    o = np.lexsort((np.abs(vp - mid_pt[vw]), key))      # per (way, country): vertex closest to the way's middle
    ks = key[o]
    first = np.ones(len(ks), dtype=bool)
    first[1:] = ks[1:] != ks[:-1]
    sel = o[first]
    r_way, r_unit, r_rep = vw[sel], vu[sel].astype(np.int64), vp[sel]
    r_match = np.zeros(len(sel), dtype=np.int8)
    has = np.zeros(n, dtype=bool)
    has[r_way] = True
    lost = np.flatnonzero(~has)
    if len(lost):
        near = A0.index.nearest(xy[mid_pt[lost], 0], xy[mid_pt[lost], 1], CFG["snap_km"])[0].astype(np.int64)
        r_way = np.concatenate([r_way, lost])
        r_unit = np.concatenate([r_unit, near])
        r_rep = np.concatenate([r_rep, mid_pt[lost]])
        r_match = np.concatenate([r_match, np.where(near >= 0, 1, 2).astype(np.int8)])
    o = np.lexsort((r_unit, r_way))
    r_way, r_unit, r_rep, r_match = r_way[o], r_unit[o], r_rep[o], r_match[o]
    stats["ways"] += n
    stats["rows"] += len(r_way)
    stats["nearest"] += int(np.sum(r_match == 1))
    stats["unassigned"] += int(np.sum(r_match == 2))

    # Province / district of each row: looked up at a vertex of the road that lies in that row's country
    admin_cols = {}
    for lvl in (1, 2):
        A = ADMIN.get(lvl)
        u = np.full(len(r_way), -1, dtype=np.int64)
        if A is not None:
            q = np.flatnonzero(r_unit >= 0)
            got = A.lookup(xy[r_rep[q], 0], xy[r_rep[q], 1])
            a_iso = A.iso3[got]
            c_iso = A0.iso3[r_unit[q]]
            bad = (got >= 0) & np.array([a is not None and c is not None and a != c for a, c in zip(a_iso, c_iso)],
                                         dtype=bool)
            stats[f"admin{lvl}_other_country"] += int(bad.sum())
            got[bad] = -1
            u[q] = got
            stats[f"admin{lvl}_missing"] += int(np.sum(u[q] < 0))
            admin_cols[f"admin{lvl}_code"] = A.code[u]
            admin_cols[f"admin{lvl}_name"] = A.name[u]
        else:
            admin_cols[f"admin{lvl}_code"] = np.full(len(r_way), None, dtype=object)
            admin_cols[f"admin{lvl}_name"] = np.full(len(r_way), None, dtype=object)

    take = pa.array(r_way)
    out = (tb.select(["@id", "@version", "@timestamp"] + EXPORT_TAGS)
             .take(take)
             .rename_columns(["osm_id", "osm_version", "osm_ts"] + EXPORT_TAGS))
    stats["no_timestamp"] += int(pc.sum(pc.fill_null(pc.less_equal(out["osm_ts"], 0), True)
                                        .cast(pa.int64())).as_py() or 0)
    offsets = np.zeros(n + 1, dtype=np.int32)
    offsets[1:] = np.cumsum(counts) * 2
    coords_flat = pa.ListArray.from_arrays(pa.array(offsets, pa.int32()), pa.array(xy.ravel(), pa.float64()))
    out = out.append_column("n_points", pa.array(counts[r_way].astype(np.int32)))
    out = out.append_column("length_m", pa.array(length_m[r_way]))
    out = out.append_column("coords", coords_flat.take(take))
    out = out.append_column("country_iso3", pa.array(A0.code[r_unit], pa.string()))
    out = out.append_column("boundary_match", pa.array(_MATCH[r_match], pa.string()))
    for name, arr in admin_cols.items():
        out = out.append_column(name, pa.array(arr, pa.string()))
    return out


def process_extract(ex: Extract, export_cfg: str) -> dict:
    """Roads of one PBF file → staging Parquet (skipped when its checkpoint matches)."""
    m = read_marker("roads", ex.name)
    if m and m.get("signature") == ex.signature and m.get("boundary_fingerprint") == BOUNDARY_FINGERPRINT:
        return {"extract": ex.name, "status": "skipped (done earlier)", **m.get("stats", {})}
    t0 = time.time()
    filtered = ensure_filtered(ex)
    out_dir = f"{ROADS_STAGING}/extract={ex.name}"
    rm_tree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    index = CFG["node_index"]
    idx_file = None
    if index.endswith("_file_array"):
        idx_file = os.path.join(LOCAL_DIR, f"{ex.name}.nodes.idx")
        index = f"{index},{idx_file}"
    cmd = [OSMIUM, "export", filtered, "-c", export_cfg, "-f", "geojsonseq",
           "--geometry-types=linestring", "-i", index, "-o", "-"]
    err_path = os.path.join(LOCAL_DIR, f"{ex.name}.export.stderr")
    chunk_bytes = int(CFG["read_chunk_mb"]) << 20
    stats, part = Counter(), 0

    def flush(tbl):
        nonlocal part
        if tbl is None or tbl.num_rows == 0:
            return
        local = os.path.join(LOCAL_DIR, f"{ex.name}.part.parquet")
        pq.write_table(tbl, local, compression="zstd")
        shutil.copyfile(local, f"{out_dir}/part-{part:05d}.parquet")
        os.remove(local)
        part += 1

    with open(err_path, "wb") as err:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err, bufsize=1 << 20)
        try:
            rest, last_log = b"", time.time()
            while True:
                chunk = proc.stdout.read(chunk_bytes)
                if not chunk:
                    break
                data = rest + chunk
                cut = data.rfind(b"\n") + 1
                rest = data[cut:]
                if cut:
                    flush(process_chunk(data[:cut], stats))
                if time.time() - last_log > 300:
                    log.info(f"[{ex.name}] {stats['ways']:,} roads so far ({(time.time() - t0) / 60:.0f} min)")
                    last_log = time.time()
            if rest.strip():
                flush(process_chunk(rest + b"\n", stats))
            rc = proc.wait()
        except BaseException:
            proc.kill()
            proc.wait()
            raise
    if rc != 0:
        with open(err_path, "rb") as fh:
            tail = fh.read()[-2000:].decode("utf-8", "ignore")
        raise RuntimeError(f"[{ex.name}] osmium export failed (exit {rc}): {tail}")
    for f in (idx_file, err_path):
        if f and os.path.exists(f):
            os.remove(f)
    if not CFG["keep_local_files"] and os.path.exists(filtered):
        os.remove(filtered)
    stats = dict(stats, parquet_files=part)
    write_marker("roads", ex.name, {"signature": ex.signature, "boundary_fingerprint": BOUNDARY_FINGERPRINT,
                                    "snapshot": ex.snapshot, "stats": stats})
    return {"extract": ex.name, "status": f"done in {(time.time() - t0) / 60:.1f} min", **stats}


def run_parallel(fn, extracts: list, label: str) -> list:
    results, failures = [], {}
    with ThreadPoolExecutor(max_workers=EXTRACT_WORKERS, thread_name_prefix=label) as pool:
        futs = {pool.submit(fn, ex): ex for ex in extracts}
        for fut in as_completed(futs):
            ex = futs[fut]
            try:
                res = fut.result()
                results.append(res)
                log.info(f"[{label}] ✅ {ex.name}: {res['status']}")
            except Exception as e:
                failures[ex.name] = f"{type(e).__name__}: {str(e)[:1500]}"
                results.append({"extract": ex.name, "status": "FAILED", "error": failures[ex.name]})
                log.error(f"[{label}] ❌ {ex.name}: {failures[ex.name][:500]}")
    display(pd.DataFrame(results))
    if failures and not CFG["allow_partial"]:
        raise RuntimeError(f"{label} failed for {sorted(failures)} — fix the cause and re-run with mode=resume "
                           "(finished files are skipped).")
    return results


print("✅ Road functions defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run — roads of every PBF file → staging Parquet

# COMMAND ----------

EXPORT_CFG = export_config_path()
ROAD_RESULTS = run_parallel(lambda ex: process_extract(ex, EXPORT_CFG), EXTRACTS, "roads")
ROAD_EXTRACTS = [ex for ex in EXTRACTS
                 if (read_marker("roads", ex.name) or {}).get("boundary_fingerprint") == BOUNDARY_FINGERPRINT]

_tot = Counter()
for ex in ROAD_EXTRACTS:
    _tot.update({k: v for k, v in read_marker("roads", ex.name).get("stats", {}).items() if isinstance(v, int)})
print(f"Roads staged: {_tot['ways']:,} ways → {_tot['rows']:,} rows; {_tot['nearest']:,} assigned to the nearest "
      f"country within {CFG['snap_km']} km, {_tot['unassigned']:,} outside every country")
if SINCE_EPOCH and _tot["ways"] and _tot["no_timestamp"] >= 0.5 * _tot["rows"]:
    raise RuntimeError("Most roads have no timestamp in these files, so since_date cannot be applied. "
                       "Leave since_date blank, or use files that keep timestamps.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Spark — de-duplicate across files, derive attributes and WKT, write the all-countries table

# COMMAND ----------

_PAVED = ["paved", "asphalt", "concrete", "concrete:plates", "concrete:lanes", "paving_stones", "sett",
          "unhewn_cobblestone", "cobblestone", "cobblestone:flattened", "metal", "wood", "tartan", "artificial_turf"]
_UNPAVED = ["unpaved", "compacted", "fine_gravel", "gravel", "shells", "rock", "pebblestone", "ground", "dirt",
            "earth", "grass", "grass_paver", "mud", "sand", "woodchips", "snow", "ice", "salt", "clay",
            "caliche", "laterite"]
_INF_PAVED = ["motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link", "secondary",
              "secondary_link", "tertiary", "tertiary_link", "residential", "living_street", "unclassified",
              "service", "pedestrian", "busway"]
_INF_UNPAVED = ["track", "path", "footway", "bridleway", "cycleway"]
_CLASS_MAP = {
    "motorway": "motorway", "motorway_link": "motorway", "trunk": "trunk", "trunk_link": "trunk",
    "primary": "primary", "primary_link": "primary", "secondary": "secondary", "secondary_link": "secondary",
    "tertiary": "tertiary", "tertiary_link": "tertiary", "residential": "residential",
    "living_street": "residential", "unclassified": "unclassified", "road": "unclassified",
    "service": "service", "busway": "service", "pedestrian": "pedestrian", "track": "track",
    "path": "path", "footway": "path", "cycleway": "path", "bridleway": "path",
}

FINAL_COLUMNS = [
    "osm_id", "osm_version", "osm_timestamp", "country_iso3", "country_iso2", "country_name", "boundary_match",
    "admin1_code", "admin1_name", "admin2_code", "admin2_name",
    "highway", "highway_class", "surface_tag", "surface_type", "road_name", "ref", "lanes",
    "maxspeed_raw", "maxspeed_kmh", "oneway", "bridge", "tunnel", "access",
    "length_m", "length_km", "geometry_wkt", "since_date", "source_extract", "osm_snapshot",
    "run_id", "extracted_at",
]

# WKT built in Spark from the staged coordinate list [x0, y0, x1, y1, …]; 7 decimals (OSM precision), zeros trimmed
_FMT = r"regexp_replace(format_string('%.7f', {v}), '\\.?0+$', '')"
_WKT_SQL = ("concat('LINESTRING (', array_join(transform(sequence(0, CAST(size(coords) / 2 AS INT) - 1), "
            "i -> concat(" + _FMT.format(v="coords[2 * i]") + ", ' ', " + _FMT.format(v="coords[2 * i + 1]") +
            ")), ', '), ')')")


def _flag(col: str):
    v = F.lower(F.trim(F.col(col)))
    return F.when(F.col(col).isNull(), F.lit(False)).otherwise(~v.isin("no", "false", "0", ""))


def shape_roads(df, snapshots: dict):
    hw = F.lower(F.trim(F.col("highway")))
    sf = F.lower(F.trim(F.col("surface")))
    ms = F.trim(F.col("maxspeed"))
    num = F.expr("try_cast(regexp_extract(trim(maxspeed), '^([0-9.]+)', 1) AS DOUBLE)")
    class_map = F.create_map(*[F.lit(x) for kv in _CLASS_MAP.items() for x in kv])
    snap_map = F.create_map(*[F.lit(x) for kv in snapshots.items() for x in kv])
    meta = spark.createDataFrame([(c["key"], c["iso2"], c["name"]) for c in COUNTRIES],
                                 "country_iso3 STRING, country_iso2 STRING, country_name STRING")
    out = (df.join(F.broadcast(meta), "country_iso3", "left")
             .select(
                 F.col("osm_id").cast("bigint").alias("osm_id"),
                 F.col("osm_version").cast("bigint").alias("osm_version"),
                 F.when(F.col("osm_ts") > 0, F.timestamp_seconds(F.col("osm_ts"))).alias("osm_timestamp"),
                 "country_iso3", "country_iso2", "country_name", "boundary_match",
                 "admin1_code", "admin1_name", "admin2_code", "admin2_name",
                 F.col("highway"),
                 F.coalesce(F.element_at(class_map, hw), F.lit("other")).alias("highway_class"),
                 F.col("surface").alias("surface_tag"),
                 F.when(sf.isin(_PAVED), "paved").when(sf.isin(_UNPAVED), "unpaved")
                  .when(hw.isin(_INF_PAVED), "paved").when(hw.isin(_INF_UNPAVED), "unpaved")
                  .otherwise("unknown").alias("surface_type"),
                 F.col("name").alias("road_name"),
                 F.col("ref"),
                 F.expr("try_cast(lanes AS INT)").alias("lanes"),
                 F.col("maxspeed").alias("maxspeed_raw"),
                 F.when(ms.rlike(r"(?i)^[0-9.]+\s*mph$"), F.round(num * 1.60934, 1))
                  .when(ms.rlike(r"(?i)^[0-9.]+\s*knots?$"), F.round(num * 1.852, 1))
                  .when(ms.rlike(r"(?i)^[0-9.]+\s*(km/h|kmh|kph)?$"), num).alias("maxspeed_kmh"),
                 F.coalesce(F.col("oneway"), F.lit("no")).alias("oneway"),
                 _flag("bridge").alias("bridge"),
                 _flag("tunnel").alias("tunnel"),
                 F.col("access"),
                 F.col("length_m").cast("double").alias("length_m"),
                 F.round(F.col("length_m") / 1000.0, 4).alias("length_km"),
                 F.expr(_WKT_SQL).alias("geometry_wkt"),
                 F.lit(SINCE_DATE or None).cast("string").alias("since_date"),
                 F.col("extract").alias("source_extract"),
                 F.element_at(snap_map, F.col("extract")).alias("osm_snapshot"),
                 F.lit(RUN_ID).alias("run_id"),
                 F.current_timestamp().alias("extracted_at")))
    return out.select(*FINAL_COLUMNS)


def build_master_table(extracts: list) -> None:
    paths = [f"{ROADS_STAGING}/extract={ex.name}" for ex in extracts]
    raw = spark.read.option("basePath", ROADS_STAGING).parquet(*paths)

    # Continent files overlap along their edges, so some ways are in two files. Keep one copy of each way,
    # from the file with the newest version (then the longest geometry).
    dups = (raw.groupBy("osm_id")
               .agg(F.min("extract").alias("_mn"), F.max("extract").alias("_mx"),
                    F.max_by("extract", F.struct("osm_version", "n_points", "extract")).alias("_best"))
               .where("_mn <> _mx")
               .select("osm_id", "_best"))
    dups.write.format(TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(DUP_TABLE)
    dups = spark.table(DUP_TABLE)
    n_dup = dups.count()
    print(f"Ways present in more than one file: {n_dup:,}")
    dups_j = F.broadcast(dups) if n_dup <= 5_000_000 else dups
    roads = raw.join(dups_j, "osm_id", "left").where(F.col("_best").isNull() | (F.col("extract") == F.col("_best")))

    if SINCE_EPOCH:
        roads = roads.where(F.col("osm_ts") >= F.lit(SINCE_EPOCH))
    if COUNTRY_FILTER:
        roads = roads.where(F.col("country_iso3").isin(TARGET_KEYS))

    final = shape_roads(roads, {ex.name: ex.snapshot for ex in extracts})
    writer = final.write.format(TABLE_FORMAT).partitionBy("country_iso3")
    if COUNTRY_FILTER and spark.catalog.tableExists(MASTER_TABLE):
        in_list = ", ".join(f"'{k}'" for k in TARGET_KEYS)
        writer.mode("overwrite").option("replaceWhere", f"country_iso3 IN ({in_list})").saveAsTable(MASTER_TABLE)
    else:
        writer.mode("overwrite").option("overwriteSchema", "true").saveAsTable(MASTER_TABLE)
    try:
        spark.sql(f"DROP TABLE IF EXISTS {DUP_TABLE}")
    except Exception:
        pass


t0 = time.time()
build_master_table(ROAD_EXTRACTS)
COUNTS = {r["country_iso3"]: (r["n"], r["km"]) for r in
          spark.table(MASTER_TABLE).groupBy("country_iso3")
               .agg(F.count("*").alias("n"), F.round(F.sum("length_km"), 1).alias("km")).collect()}
print(f"✅ {MASTER_TABLE}: {sum(v[0] for k, v in COUNTS.items() if k):,} rows in "
      f"{sum(1 for k in COUNTS if k)} countries ({(time.time() - t0) / 60:.1f} min)")
if None in COUNTS:
    print(f"   + {COUNTS[None][0]:,} rows outside every country (country_iso3 IS NULL)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. One table (or view) per country

# COMMAND ----------

PUBLISH = [k for k in TARGET_KEYS if COUNTS.get(k, (0, 0))[0] > 0 or CFG["create_empty_outputs"]]


def publish_country_object(key: str) -> str:
    c = COUNTRY_BY_KEY[key]
    t = c["table"]
    body = f"SELECT * FROM {MASTER_TABLE} WHERE country_iso3 = '{key}'"
    if CFG["country_objects"] == "table":
        spark.sql(f"CREATE OR REPLACE TABLE {t} AS {body}")
    elif CFG["country_objects"] == "view":
        spark.sql(f"CREATE OR REPLACE VIEW {t} AS {body}")
    else:
        return ""
    try:
        label = (c["name"] or key).replace("'", "")
        what = "TABLE" if CFG["country_objects"] == "table" else "VIEW"
        spark.sql(f"COMMENT ON {what} {t} IS 'OSM highway ways in {label} ({key}; World Bank boundaries) from "
                  f"Geofabrik extracts{' edited since ' + SINCE_DATE if SINCE_DATE else ''}. run_id={RUN_ID}'")
    except Exception:
        pass
    return t


OBJ_ERRORS = {}
if CFG["country_objects"] in ("table", "view"):
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="publish") as pool:
        futs = {pool.submit(publish_country_object, k): k for k in PUBLISH}
        for fut in as_completed(futs):
            try:
                fut.result()
            except Exception as e:
                OBJ_ERRORS[futs[fut]] = f"{type(e).__name__}: {str(e)[:500]}"
    print(f"✅ {len(PUBLISH) - len(OBJ_ERRORS)} country {CFG['country_objects']}s in {CFG['catalog_schema']} "
          f"({(time.time() - t0) / 60:.1f} min)" + (f"; failed: {sorted(OBJ_ERRORS)}" if OBJ_ERRORS else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CSV export — all countries in one Spark job, then renamed to `<country>_osm_road.csv`

# COMMAND ----------

def _csv_pattern(base: str):
    return re.compile(rf"^{re.escape(base)}(_part\d{{3}})?\.csv(\.gz)?$")


def _strip_dbfs(path: str) -> str:
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path


def export_all_csv(keys: list) -> dict:
    """→ {country key: [csv paths]}. Rows are bucketed so each file stays under csv_max_rows_per_file."""
    lim = CFG["csv_max_rows_per_file"]
    ext = ".csv.gz" if CFG["csv_compression"] == "gzip" else ".csv"
    with_rows = [k for k in keys if COUNTS.get(k, (0, 0))[0] > 0]
    files = {}
    tmp = f"{CSV_DIR}/_tmp_{RUN_ID}"
    try:
        if with_rows:
            buckets = {k: (max(1, math.ceil(COUNTS[k][0] / (0.9 * lim))) if lim else 1) for k in with_rows}
            nb = spark.createDataFrame(list(buckets.items()), "country_iso3 STRING, _nb INT")
            data = (spark.table(MASTER_TABLE).where(F.col("country_iso3").isin(with_rows))
                         .join(F.broadcast(nb), "country_iso3")
                         .withColumn("_b", F.pmod(F.xxhash64("osm_id"), F.col("_nb")))
                         .withColumn("_key", F.col("country_iso3"))
                         .repartition(sum(buckets.values()), "_key", "_b")
                         .select(*FINAL_COLUMNS, "_key"))
            w = (data.write.mode("overwrite").partitionBy("_key")
                     .option("header", "true").option("quote", '"').option("escape", '"')
                     .option("encoding", "UTF-8").option("timeZone", "UTC")
                     .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            if lim:
                w = w.option("maxRecordsPerFile", int(lim))
            if CFG["csv_compression"]:
                w = w.option("compression", CFG["csv_compression"])
            w.csv(tmp)

        existing = dbutils.fs.ls(CSV_DIR)

        def finalize(key: str) -> list:
            base = COUNTRY_BY_KEY[key]["table"].split(".")[-1]
            pat = _csv_pattern(base)
            parts = []
            if key in with_rows:
                parts = sorted(e.path for e in dbutils.fs.ls(f"{tmp}/_key={key}") if e.name.startswith("part-"))
            for e in existing:
                if pat.match(e.name):
                    dbutils.fs.rm(e.path)
            out = []
            if not parts:
                dest = f"{CSV_DIR}/{base}.csv"
                dbutils.fs.put(dest, ",".join(FINAL_COLUMNS) + "\n", True)
                out.append(dest)
            elif len(parts) == 1:
                dest = f"{CSV_DIR}/{base}{ext}"
                dbutils.fs.mv(parts[0], dest)
                out.append(dest)
            else:
                for i, p in enumerate(parts, 1):
                    dest = f"{CSV_DIR}/{base}_part{i:03d}{ext}"
                    dbutils.fs.mv(p, dest)
                    out.append(dest)
            return [_strip_dbfs(x) for x in out]

        with ThreadPoolExecutor(max_workers=16, thread_name_prefix="csv") as pool:
            futs = {pool.submit(finalize, k): k for k in keys}
            for fut in as_completed(futs):
                k = futs[fut]
                try:
                    files[k] = fut.result()
                except Exception as e:
                    files[k] = []
                    log.error(f"[{k}] CSV finalize failed: {type(e).__name__}: {str(e)[:300]}")
    finally:
        rm_tree(tmp)
    return files


CSV_FILES = {}
if CFG["csv_export"]:
    t0 = time.time()
    CSV_FILES = export_all_csv(PUBLISH)
    print(f"✅ CSVs for {sum(1 for v in CSV_FILES.values() if v)} countries in {CSV_DIR} "
          f"({(time.time() - t0) / 60:.1f} min)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Run summary

# COMMAND ----------

def _status(key: str) -> str:
    if key in OBJ_ERRORS or (CFG["csv_export"] and key in PUBLISH and not CSV_FILES.get(key)):
        return "FAILED"
    return "SUCCESS" if COUNTS.get(key, (0, 0))[0] > 0 else "EMPTY"


_now = datetime.now(timezone.utc)
_bfile = os.path.basename(CFG["boundaries"][0])
log_rows = []
for k in TARGET_KEYS:
    c = COUNTRY_BY_KEY[k]
    log_rows.append((RUN_ID, k, c["iso2"], c["name"],
                     c["table"] if k in PUBLISH and CFG["country_objects"] != "none" else None,
                     SINCE_DATE or None, _status(k), int(COUNTS.get(k, (0, 0))[0]),
                     float(COUNTS.get(k, (0, 0.0))[1] or 0.0),
                     ", ".join(CSV_FILES.get(k, [])) or None, _bfile, OBJ_ERRORS.get(k),
                     ",".join(ex.name for ex in ROAD_EXTRACTS), _now))
LOG_SCHEMA = ("run_id STRING, country_iso3 STRING, country_iso2 STRING, country_name STRING, table_name STRING, "
              "since_date STRING, status STRING, row_count BIGINT, total_km DOUBLE, csv_files STRING, "
              "boundary_source STRING, error STRING, extracts STRING, finished_at TIMESTAMP")
(spark.createDataFrame(log_rows, LOG_SCHEMA)
      .write.format(TABLE_FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(LOG_TABLE))

summary = spark.table(LOG_TABLE).where(F.col("run_id") == RUN_ID).orderBy("status", "country_iso3")
display(summary)
print(pd.Series([r[6] for r in log_rows]).value_counts().to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Quick checks (optional)

# COMMAND ----------

display(spark.sql(f"""
    SELECT country_iso3, country_name, boundary_match, highway_class, surface_type,
           COUNT(*) AS road_count, ROUND(SUM(length_km), 1) AS total_km,
           ROUND(AVG(CASE WHEN admin1_code IS NULL THEN 0 ELSE 1 END), 3) AS share_with_admin1
    FROM {MASTER_TABLE}
    WHERE country_iso3 IS NOT NULL
    GROUP BY ALL
    ORDER BY country_iso3, boundary_match, highway_class, surface_type
"""))
