# Databricks notebook source
# MAGIC %md
# MAGIC # OSM roads: coverage in km per country and road kind
# MAGIC
# MAGIC Turns every `prd_mega.sgpbpi163.osm_geofabrik_<iso2>` table into one row of a country-level CSV:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/prd_mega/sgpbpi163/vgpbpi163/OSM/geofabrik_country/00_countrywise_road_coverage_analysis.csv
# MAGIC
# MAGIC country_iso2, country_name, total_coverage_km, primary_km, secondary_km, tertiary_km, <every other kind>_km ...
# MAGIC ```
# MAGIC
# MAGIC For every table the notebook:
# MAGIC 1. loads it as a Spark DataFrame. Countries that Geofabrik ships inside a multi-country extract (GCC states,
# MAGIC    Malaysia/Singapore/Brunei, ...) are cut back to their World Bank boundary with the **same rule as the zip
# MAGIC    builder** (`02_osm_roads_build_zips.py`), so the km figures match the rows people download,
# MAGIC 2. adds the column `kms`: the geodesic length of each road line on the WGS84 ellipsoid, in kilometres,
# MAGIC 3. groups the roads by `kind` and sums `kms`,
# MAGIC 4. keeps the small per-country result and carries on with the next country. A failed country is reported and
# MAGIC    left out of the CSV, it never stops the loop.
# MAGIC
# MAGIC After the loop the results are put together as one Spark DataFrame:
# MAGIC * every `kind` found in **any** country gets its own column, a country without that kind gets `0`,
# MAGIC * `primary_km`, `secondary_km` and `tertiary_km` always exist (right after the total), even if no country has them,
# MAGIC * `total_coverage_km` is the sum over **all** kinds (links, tracks, footways, `unknown`, ... included),
# MAGIC * a final check proves that no kind was dropped (total = sum of the columns) and that the CSV on the Volume
# MAGIC   reads back with the same rows and columns.
# MAGIC
# MAGIC **Notes**
# MAGIC * `kind` is normalised before grouping: trimmed, lower-cased, every run of characters that is not a letter or digit
# MAGIC   becomes `_` (`Tertiary-Link`, `tertiary link`, `tertiary_link` -> `tertiary_link`). Empty or NULL `kind` -> `unknown`.
# MAGIC * A road whose geometry is NULL, unreadable or not a line has no length: it is left out of the km sums and counted in
# MAGIC   the run summary (`bad_geometry`).
# MAGIC * For clipped countries a road is kept **in full** when it touches the boundary, as in the zips. A road that crosses
# MAGIC   a border is therefore counted for both neighbours.
# MAGIC * Every country is one Spark job, so the big ones (US, India, China, Indonesia, Russia) take the longest.
# MAGIC   Smoke test first with `ONLY_ISO2 = ["LA", "BT"]`.

# COMMAND ----------

# MAGIC %pip install "numpy<2" shapely pyproj

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import re
import shutil
import time
from functools import reduce
from operator import add
from typing import Iterator

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DecimalType, DoubleType

# COMMAND ----------

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

CATALOG = "prd_mega"
SCHEMA = "sgpbpi163"
TABLE_PREFIX = "osm_geofabrik_"           # tables are <prefix><iso2>, e.g. osm_geofabrik_la
SKIP_TABLES = {"osm_geofabrik_run_log"}    # also matches the prefix, but it is the run log

OUT_DIR = "/Volumes/prd_mega/sgpbpi163/vgpbpi163/OSM/geofabrik_country"
OUT_NAME = "00_countrywise_road_coverage_analysis.csv"
OUT_CSV = f"{OUT_DIR}/{OUT_NAME}"
STAGING_DIR = f"{OUT_DIR}/_staging_coverage"   # Spark writes the single CSV part here; removed afterwards

# Same clipping rule as 02_osm_roads_build_zips.py: keep the values identical so the km match the zips.
CLIP_SHARED_EXTRACTS = True
BOUNDARY_TABLE = "prd_mega.sgpbpi163.cbd_world_boundaries_dissolved"   # columns iso_a3, geometry_json (GeoJSON)
CLIP_BUFFER_DEG = 0.002                    # ~200 m
BOUNDARY_SIMPLIFY_DEG = 0.0005             # ~50 m
# Unlike the zip builder (which can publish a file with a note), a CSV row has no place for a warning, and a
# neighbouring country's roads would silently inflate the km. So by default a country whose boundary is missing is
# skipped and reported. "keep_unclipped" = use the whole shared extract anyway.
ON_MISSING_BOUNDARY = "fail"

PRIORITY_KINDS = ["primary", "secondary", "tertiary"]   # always present, right after the total
UNKNOWN_KIND = "unknown"                   # label for NULL / empty kind
KM_DECIMALS = 3                            # 0.001 km = 1 m
ID_COLS = ["country_iso2", "country_name"]
TOTAL_COL = "total_coverage_km"

# Smoke test: e.g. ["LA", "BT"]. Empty list = all tables.
# WARNING: the CSV is overwritten each run, so a smoke test replaces the full file with just these countries.
ONLY_ISO2 = []

# COMMAND ----------

# -----------------------------------------------------------------------------
# Country names (same list as the extraction notebook), ISO-2 -> name
# -----------------------------------------------------------------------------

NAMES = {
    "AW": "Aruba",
    "AF": "Afghanistan",
    "AO": "Angola",
    "AI": "Anguilla",
    "AL": "Albania",
    "AD": "Andorra",
    "AN": "Netherlands Antilles",
    "AE": "United Arab Emirates",
    "AR": "Argentina",
    "AM": "Armenia",
    "AS": "American Samoa",
    "AG": "Antigua and Barbuda",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BI": "Burundi",
    "BE": "Belgium",
    "BJ": "Benin",
    "BF": "Burkina Faso",
    "BD": "Bangladesh",
    "BG": "Bulgaria",
    "BH": "Bahrain",
    "BS": "Bahamas",
    "BA": "Bosnia and Herzegovina",
    "BY": "Belarus",
    "BZ": "Belize",
    "BM": "Bermuda",
    "BO": "Bolivia",
    "BR": "Brazil",
    "BB": "Barbados",
    "BN": "Brunei",
    "BT": "Bhutan",
    "BW": "Botswana",
    "CF": "Central African Republic",
    "CA": "Canada",
    "CC": "Cocos (Keeling) Islands",
    "CH": "Switzerland",
    "CL": "Chile",
    "CN": "China",
    "CI": "Cote d'Ivoire",
    "CM": "Cameroon",
    "CD": "Congo, Democratic Republic",
    "CG": "Congo, Republic",
    "CK": "Cook Islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CV": "Cape Verde",
    "CR": "Costa Rica",
    "CU": "Cuba",
    "CX": "Christmas Island",
    "KY": "Cayman Islands",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DE": "Germany",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DK": "Denmark",
    "DO": "Dominican Republic",
    "DZ": "Algeria",
    "EC": "Ecuador",
    "EG": "Egypt",
    "ER": "Eritrea",
    "EH": "Western Sahara",
    "ES": "Spain",
    "EE": "Estonia",
    "ET": "Ethiopia",
    "FI": "Finland",
    "FJ": "Fiji",
    "FK": "Falkland Islands",
    "FR": "France",
    "FO": "Faroe Islands",
    "FM": "Micronesia",
    "GA": "Gabon",
    "GB": "United Kingdom",
    "GE": "Georgia",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GN": "Guinea",
    "GP": "Guadeloupe",
    "GM": "Gambia",
    "GW": "Guinea-Bissau",
    "GQ": "Equatorial Guinea",
    "GR": "Greece",
    "GD": "Grenada",
    "GL": "Greenland",
    "GT": "Guatemala",
    "GF": "French Guiana",
    "GU": "Guam",
    "GY": "Guyana",
    "HK": "Hong Kong",
    "HN": "Honduras",
    "HR": "Croatia",
    "HT": "Haiti",
    "HU": "Hungary",
    "ID": "Indonesia",
    "IN": "India",
    "IO": "British Indian Ocean Territory",
    "IE": "Ireland",
    "IR": "Iran",
    "IQ": "Iraq",
    "IS": "Iceland",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JO": "Jordan",
    "JP": "Japan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KG": "Kyrgyzstan",
    "KH": "Cambodia",
    "KI": "Kiribati",
    "KN": "Saint Kitts and Nevis",
    "KR": "South Korea",
    "KW": "Kuwait",
    "LA": "Laos",
    "LB": "Lebanon",
    "LR": "Liberia",
    "LY": "Libya",
    "LC": "Saint Lucia",
    "LI": "Liechtenstein",
    "LK": "Sri Lanka",
    "LS": "Lesotho",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "LV": "Latvia",
    "MO": "Macao",
    "MA": "Morocco",
    "MC": "Monaco",
    "MD": "Moldova",
    "MG": "Madagascar",
    "MV": "Maldives",
    "MX": "Mexico",
    "MH": "Marshall Islands",
    "MK": "North Macedonia",
    "ML": "Mali",
    "MT": "Malta",
    "MM": "Myanmar",
    "MN": "Mongolia",
    "MP": "Northern Mariana Islands",
    "MZ": "Mozambique",
    "MR": "Mauritania",
    "MS": "Montserrat",
    "MQ": "Martinique",
    "MU": "Mauritius",
    "MW": "Malawi",
    "MY": "Malaysia",
    "YT": "Mayotte",
    "NA": "Namibia",
    "NC": "New Caledonia",
    "NE": "Niger",
    "NF": "Norfolk Island",
    "NG": "Nigeria",
    "NI": "Nicaragua",
    "NU": "Niue",
    "NL": "Netherlands",
    "NO": "Norway",
    "NP": "Nepal",
    "NR": "Nauru",
    "NZ": "New Zealand",
    "OM": "Oman",
    "PK": "Pakistan",
    "PA": "Panama",
    "PN": "Pitcairn Islands",
    "PE": "Peru",
    "PH": "Philippines",
    "PW": "Palau",
    "PG": "Papua New Guinea",
    "PL": "Poland",
    "PR": "Puerto Rico",
    "KP": "North Korea",
    "PT": "Portugal",
    "PY": "Paraguay",
    "PS": "Palestine",
    "PF": "French Polynesia",
    "QA": "Qatar",
    "RE": "Reunion",
    "RO": "Romania",
    "RU": "Russia",
    "RW": "Rwanda",
    "SA": "Saudi Arabia",
    "CS": "Serbia and Montenegro",
    "SD": "Sudan",
    "SN": "Senegal",
    "SG": "Singapore",
    "SH": "Saint Helena",
    "SJ": "Svalbard and Jan Mayen",
    "SB": "Solomon Islands",
    "SL": "Sierra Leone",
    "SV": "El Salvador",
    "SM": "San Marino",
    "SO": "Somalia",
    "PM": "Saint Pierre and Miquelon",
    "ST": "Sao Tome and Principe",
    "SR": "Suriname",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SE": "Sweden",
    "SZ": "Eswatini",
    "SC": "Seychelles",
    "SY": "Syria",
    "TC": "Turks and Caicos Islands",
    "TD": "Chad",
    "TG": "Togo",
    "TH": "Thailand",
    "TJ": "Tajikistan",
    "TK": "Tokelau",
    "TM": "Turkmenistan",
    "TL": "Timor-Leste",
    "TO": "Tonga",
    "TT": "Trinidad and Tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TV": "Tuvalu",
    "TW": "Taiwan",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "UA": "Ukraine",
    "UM": "US Minor Outlying Islands",
    "UY": "Uruguay",
    "US": "United States",
    "UZ": "Uzbekistan",
    "VA": "Vatican City",
    "VC": "Saint Vincent and the Grenadines",
    "VE": "Venezuela",
    "VG": "British Virgin Islands",
    "VI": "US Virgin Islands",
    "VN": "Vietnam",
    "VU": "Vanuatu",
    "WF": "Wallis and Futuna",
    "WS": "Samoa",
    "YE": "Yemen",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

# Countries Geofabrik only publishes inside a multi-country extract are cut back to their own boundary:
# ISO-2 -> iso_a3 in BOUNDARY_TABLE. Identical to 02_osm_roads_build_zips.py.
CLIP_TO_COUNTRY = {
    "AE": "ARE", "BH": "BHR", "KW": "KWT", "OM": "OMN", "QA": "QAT", "SA": "SAU",   # gcc-states
    "MY": "MYS", "SG": "SGP", "BN": "BRN",                                          # malaysia-singapore-brunei
    "HT": "HTI", "DO": "DOM",                                                       # haiti-and-domrep
    "IL": "ISR", "PS": "PSE",                                                       # israel-and-palestine
    "SN": "SEN", "GM": "GMB",                                                       # senegal-and-gambia
    "AS": "ASM", "GU": "GUM", "MP": "MNP",                                          # american-oceania
    "IE": "IRL",                                                                    # + Northern Ireland
    "ID": "IDN",                                                                    # + Timor-Leste
}

# COMMAND ----------

# -----------------------------------------------------------------------------
# Helpers: tables, boundary clip
# -----------------------------------------------------------------------------

_ISO2_SUFFIX = re.compile(r"^[a-z]{2}$")


def discover_tables():
    """[(iso2, full table name)] for every osm_geofabrik_<iso2> table, sorted by code."""
    found = []
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect():
        name = row["tableName"]
        if name in SKIP_TABLES or not name.startswith(TABLE_PREFIX) or row["isTemporary"]:
            continue
        suffix = name[len(TABLE_PREFIX):]
        if _ISO2_SUFFIX.match(suffix):            # leaves out e.g. osm_geofabrik_de__new staging tables
            found.append((suffix.upper(), f"{CATALOG}.{SCHEMA}.{name}"))
    return sorted(found)


def boundary_wkb(iso3):
    """The country's boundary from BOUNDARY_TABLE, buffered and simplified, as WKB (or None if missing)."""
    import json
    import shapely
    from shapely.geometry import shape

    rows = (spark.table(BOUNDARY_TABLE)
            .where(F.col("iso_a3") == iso3)
            .where(F.col("geometry_json").isNotNull())
            .select("geometry_json")
            .collect())
    geoms = []
    for row in rows:
        gj = json.loads(row["geometry_json"])
        if gj.get("type") == "Feature":
            gj = gj.get("geometry")
        if gj:
            geoms.append(shapely.make_valid(shape(gj)))
    if not geoms:
        return None
    area = shapely.union_all(geoms)
    if CLIP_BUFFER_DEG > 0:
        area = area.buffer(CLIP_BUFFER_DEG)
    if BOUNDARY_SIMPLIFY_DEG > 0:
        area = area.simplify(BOUNDARY_SIMPLIFY_DEG, preserve_topology=True)
    if area.is_empty:
        return None
    return shapely.to_wkb(area)


def clip_to_boundary(df, area_wkb):
    """Rows whose geometry (WKT) touches the area. Runs on the executors with shapely."""

    @F.pandas_udf(BooleanType())
    def touches(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        import shapely
        area = shapely.from_wkb(area_wkb)
        shapely.prepare(area)
        for wkt in batches:
            lines = shapely.from_wkt(wkt.to_numpy(dtype=object), on_invalid="ignore")
            yield pd.Series(shapely.intersects(lines, area), index=wkt.index)

    return df.where(touches(F.col("geometry")))

# COMMAND ----------

# -----------------------------------------------------------------------------
# Helpers: road length in km
# -----------------------------------------------------------------------------


def _km_from_wkt(wkt):
    """Geodesic length (WGS84 ellipsoid) in km of every WKT geometry in a pandas Series.

    WKT coordinates are (lon lat). Every line part is measured segment by segment; the parts of a MULTILINESTRING
    are NOT joined to each other. NaN (becomes NULL in Spark) = no usable line geometry: NULL, empty, unreadable WKT,
    a point/polygon, or a coordinate outside the valid lon/lat range.
    """
    import numpy as np
    import shapely
    from pyproj import Geod

    n = len(wkt)
    out = np.full(n, np.nan)
    geoms = shapely.from_wkt(wkt.to_numpy(dtype=object, na_value=None), on_invalid="ignore")
    present = np.flatnonzero(~shapely.is_missing(geoms))
    if present.size == 0:
        return pd.Series(out, index=wkt.index)

    parts, owner = shapely.get_parts(geoms[present], return_index=True)
    owner = present[owner]                                   # position of the row each part belongs to
    keep = np.isin(shapely.get_type_id(parts), (1, 2)) & ~shapely.is_empty(parts)   # LineString, LinearRing
    parts, owner = parts[keep], owner[keep]
    if parts.size == 0:
        return pd.Series(out, index=wkt.index)

    has_line = np.zeros(n, dtype=bool)
    has_line[owner] = True

    coords, part_idx = shapely.get_coordinates(parts, return_index=True)
    same_part = part_idx[1:] == part_idx[:-1]                # consecutive vertices of the same line part
    lon1, lat1 = coords[:-1, 0][same_part], coords[:-1, 1][same_part]
    lon2, lat2 = coords[1:, 0][same_part], coords[1:, 1][same_part]
    seg_owner = owner[part_idx[:-1][same_part]]

    metres = np.zeros(n)
    bad = np.zeros(n, dtype=bool)
    if lon1.size:
        _, _, dist = Geod(ellps="WGS84").inv(lon1, lat1, lon2, lat2)
        dist = np.asarray(dist, dtype="float64")
        finite = np.isfinite(dist)
        metres = np.bincount(seg_owner, weights=np.where(finite, dist, 0.0), minlength=n)
        bad = np.bincount(seg_owner, weights=(~finite).astype("float64"), minlength=n) > 0
    out[has_line] = metres[has_line] / 1000.0
    out[bad] = np.nan
    return pd.Series(out, index=wkt.index)


@F.pandas_udf(DoubleType())
def line_length_km(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for wkt in batches:
        yield _km_from_wkt(wkt)


def clean_kind(col):
    """Normalised kind: trimmed, lower case, non letters/digits -> '_'; NULL or empty -> UNKNOWN_KIND."""
    cleaned = F.regexp_replace(F.lower(F.trim(col.cast("string"))), r"[^\p{L}\p{N}]+", "_")
    cleaned = F.regexp_replace(cleaned, r"^_+|_+$", "")
    return F.when(cleaned.isNull() | (cleaned == ""), F.lit(UNKNOWN_KIND)).otherwise(cleaned)

# COMMAND ----------

# -----------------------------------------------------------------------------
# One country: table -> SDF with kms -> group by kind
# -----------------------------------------------------------------------------


def road_km_by_kind(iso2, table, clip_iso3):
    """Returns (rows, clipped). rows = one Row per raw `kind` value: kind_raw, km, segments, bad_geometry."""
    df = spark.table(table)                                          # 1. the table as a Spark DataFrame
    lower_cols = {c.lower() for c in df.columns}
    missing = [c for c in ("kind", "geometry") if c not in lower_cols]
    if missing:
        raise RuntimeError(f"{table} has no {', '.join(missing)} column")

    clipped = False
    if clip_iso3:
        area = boundary_wkb(clip_iso3)
        if area is not None:
            df = clip_to_boundary(df, area)
            clipped = True
            print(f"  clipping the shared extract to the {clip_iso3} boundary")
        elif ON_MISSING_BOUNDARY == "fail":
            raise RuntimeError(f"No boundary for {clip_iso3} in {BOUNDARY_TABLE}")
        else:
            print(f"  WARNING: no boundary for {clip_iso3} in {BOUNDARY_TABLE}; using the whole shared extract, "
                  "so the km include neighbouring countries")

    df = (df.withColumn("_len_km", line_length_km(F.col("geometry")))      # 2. the new column kms
            .withColumn("kms", F.nanvl(F.col("_len_km"), F.lit(None).cast("double")))   # NaN -> NULL, never a NaN sum
            .drop("_len_km"))

    by_kind = (df.groupBy(F.col("kind").cast("string").alias("kind_raw"))   # 3. group by kind (NULL is its own group)
                 .agg(F.sum("kms").alias("km"),
                      F.count(F.lit(1)).alias("segments"),
                      F.sum(F.when(F.col("kms").isNull(), 1).otherwise(0)).alias("bad_geometry")))
    return by_kind.collect(), clipped                                # a handful of rows per country

# COMMAND ----------

# -----------------------------------------------------------------------------
# Loop over the country tables (never stops on an error)
# -----------------------------------------------------------------------------

tables = discover_tables()
print(f"{len(tables)} country tables found in {CATALOG}.{SCHEMA}")

raw_records = []       # (country_iso2, kind_raw, km, segments, bad_geometry)
ok_countries = []      # (country_iso2, country_name) of every country that was processed
summary = []           # (iso2, name, status, detail)
unclipped = []
t_all = time.time()

for iso2, table in tables:
    if ONLY_ISO2 and iso2 not in {c.upper() for c in ONLY_ISO2}:
        continue
    name = NAMES.get(iso2)
    if name is None:
        print(f"  WARNING: no country name for {iso2}, using the code")
        name = iso2
    print(f"\n{'-' * 70}\n{name} ({iso2})\n  table: {table}")
    started = time.time()
    try:
        clip_iso3 = CLIP_TO_COUNTRY.get(iso2) if CLIP_SHARED_EXTRACTS else None
        rows, clipped = road_km_by_kind(iso2, table, clip_iso3)
        if clip_iso3 and not clipped:
            unclipped.append(iso2)

        for r in rows:
            raw_records.append((iso2, r["kind_raw"], float(r["km"] or 0.0), int(r["segments"]), int(r["bad_geometry"])))
        ok_countries.append((iso2, name))

        total_km = sum(float(r["km"] or 0.0) for r in rows)
        segments = sum(int(r["segments"]) for r in rows)
        bad = sum(int(r["bad_geometry"]) for r in rows)
        if segments == 0:
            print("  WARNING: no roads (empty table, or nothing inside the boundary); reported as 0 km")
        print(f"  {segments:,} roads, {total_km:,.1f} km, {len(rows)} raw kind value(s), "
              f"{bad:,} without usable geometry, {time.time() - started:,.0f}s")
        summary.append((iso2, name, "success", f"{total_km:,.1f} km, {segments:,} roads, {bad:,} bad geometry"))
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"[:600]
        print(f"  FAILED: {reason}")
        summary.append((iso2, name, "failure", reason))

print(f"\nLoop finished in {(time.time() - t_all) / 60:,.1f} min")
if not ok_countries:
    raise RuntimeError("No country could be processed, nothing to write")

# COMMAND ----------

# -----------------------------------------------------------------------------
# All kinds that exist (the variations of `kind`)
# -----------------------------------------------------------------------------

RAW_SCHEMA = "country_iso2 string, kind_raw string, km double, segments long, bad_geometry long"
long_raw = spark.createDataFrame(raw_records, RAW_SCHEMA)

# Variations: every spelling found in the data and the clean kind it ends up in.
variants = (long_raw.withColumn("kind", clean_kind(F.col("kind_raw")))
            .groupBy("kind")
            .agg(F.countDistinct("country_iso2").alias("countries"),
                 F.round(F.sum("km"), 1).alias("total_km"),
                 F.sort_array(F.collect_set(F.coalesce(F.col("kind_raw"), F.lit("<NULL>")))).alias("spellings_in_source"))
            .orderBy(F.desc("total_km"), "kind"))
variants.show(500, truncate=False)

# Long table: one row per country and clean kind
long_sdf = (long_raw.withColumn("kind", clean_kind(F.col("kind_raw")))
            .groupBy("country_iso2", "kind")
            .agg(F.sum("km").alias("km"),
                 F.sum("segments").alias("segments"),
                 F.sum("bad_geometry").alias("bad_geometry")))

found_kinds = [r["kind"] for r in (long_sdf.groupBy("kind").agg(F.sum("km").alias("km"))
                                   .orderBy(F.desc("km"), "kind").collect())]
# primary, secondary, tertiary first (even when nobody has them), then every other kind, biggest first
ordered_kinds = PRIORITY_KINDS + [k for k in found_kinds if k not in PRIORITY_KINDS]
km_cols = [f"{k}_km" for k in ordered_kinds]
print(f"{len(found_kinds)} kinds found: {', '.join(found_kinds)}")
print(f"kinds that no country has (written as 0): {[k for k in PRIORITY_KINDS if k not in found_kinds] or 'none'}")

all_names = ID_COLS + [TOTAL_COL] + km_cols
if len(set(n.lower() for n in all_names)) != len(all_names):
    raise RuntimeError(f"Output column names are not unique: {all_names}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# Final Spark DataFrame: one row per country, one column per kind
# -----------------------------------------------------------------------------

countries_sdf = spark.createDataFrame(ok_countries, "country_iso2 string, country_name string")

per_kind = (long_sdf.groupBy("country_iso2")
            .agg(*[F.sum(F.when(F.col("kind") == F.lit(k), F.col("km")).otherwise(F.lit(0.0))).alias(f"{k}_km")
                   for k in ordered_kinds]))

wide = (countries_sdf.join(per_kind, "country_iso2", "left")      # countries without any road still get a row
        .na.fill(0.0, subset=km_cols)                              # a kind a country does not have -> 0
        .withColumn(TOTAL_COL, reduce(add, [F.col(c) for c in km_cols])))   # total = sum of ALL kinds

km_type = DecimalType(20, KM_DECIMALS)   # plain decimals in the CSV (a double would print 1.2E7 for 12,000,000 km)
final_sdf = (wide.select(*ID_COLS,
                         *[F.col(c).cast(km_type).alias(c) for c in [TOTAL_COL] + km_cols])
             .orderBy("country_iso2"))

# Checks: no kind dropped, one row per country, no NULLs
direct = long_sdf.groupBy("country_iso2").agg(F.sum("km").alias("direct_km"))
max_diff = (wide.join(direct, "country_iso2", "left")
            .select(F.max(F.abs(F.col(TOTAL_COL) - F.coalesce(F.col("direct_km"), F.lit(0.0)))).alias("d"))
            .first()["d"])
if max_diff > 1e-3:
    raise RuntimeError(f"Total does not equal the sum of the kinds (max difference {max_diff} km)")
n_rows = final_sdf.count()
if n_rows != len(ok_countries) or final_sdf.select("country_iso2").distinct().count() != n_rows:
    raise RuntimeError("Final table does not have exactly one row per country")
n_nulls = final_sdf.select(*[F.sum(F.col(c).isNull().cast("int")) for c in final_sdf.columns]).first()
if any(v for v in n_nulls):
    raise RuntimeError(f"Final table has NULLs: {dict(zip(final_sdf.columns, n_nulls))}")

print(f"{n_rows} countries x {len(final_sdf.columns)} columns")
print(final_sdf.columns)
display(final_sdf)

# COMMAND ----------

# -----------------------------------------------------------------------------
# Write the single CSV to the Volume
# -----------------------------------------------------------------------------


def copy_to_volume(local_path, final_path):
    """Copy under a .part name, then rename, so nobody sees a half-written file."""
    tmp = final_path + ".part"
    shutil.copyfile(local_path, tmp)
    try:
        os.replace(tmp, final_path)
    except OSError:
        shutil.move(tmp, final_path)
    if os.path.getsize(final_path) != os.path.getsize(local_path):
        raise RuntimeError(f"Size check failed after copying to {final_path}")


os.makedirs(OUT_DIR, exist_ok=True)
dbutils.fs.rm(STAGING_DIR, True)
try:
    (final_sdf.coalesce(1)                      # ~250 rows: one part file, row order kept
     .write.mode("overwrite")
     .option("header", "true")
     .option("quote", '"')
     .option("escape", '"')                     # RFC 4180 ("" inside quotes), e.g. "Congo, Democratic Republic"
     .option("encoding", "UTF-8")
     .option("lineSep", "\n")
     .csv(STAGING_DIR))
    parts = [f for f in os.listdir(STAGING_DIR) if f.startswith("part-") and f.endswith(".csv")]
    if len(parts) != 1:
        raise RuntimeError(f"Expected one CSV part in {STAGING_DIR}, found {parts}")
    copy_to_volume(os.path.join(STAGING_DIR, parts[0]), OUT_CSV)
finally:
    dbutils.fs.rm(STAGING_DIR, True)
print(f"Written: {OUT_CSV} ({os.path.getsize(OUT_CSV) / 1024:,.1f} KB)")

# COMMAND ----------

# -----------------------------------------------------------------------------
# Read the file back and compare, then the run summary
# -----------------------------------------------------------------------------

back = spark.read.option("header", "true").csv(OUT_CSV)
if back.columns != final_sdf.columns:
    raise RuntimeError("CSV header differs from the DataFrame")
if back.count() != n_rows:
    raise RuntimeError(f"CSV has {back.count()} rows, expected {n_rows}")
file_total = back.select(F.sum(F.col(TOTAL_COL).cast("double"))).first()[0]
sdf_total = final_sdf.select(F.sum(F.col(TOTAL_COL).cast("double"))).first()[0]
if abs(file_total - sdf_total) > 0.01:
    raise RuntimeError(f"CSV total {file_total} differs from DataFrame total {sdf_total}")
print(f"CSV check passed: {n_rows} rows, {len(back.columns)} columns, {sdf_total:,.1f} km in total")

n_ok = sum(1 for s in summary if s[2] == "success")
failed = [s for s in summary if s[2] == "failure"]
print("=" * 70)
print("OSM ROAD COVERAGE COMPLETE")
print("=" * 70)
print(f"  countries in the CSV: {n_ok} | failed (not in the CSV): {len(failed)}")
for iso2, name, _status, detail in failed:
    print(f"  FAILED  {iso2}  {name}: {detail[:200]}")
if unclipped:
    print(f"  shared extracts used WITHOUT clipping (km include neighbouring countries): {', '.join(sorted(unclipped))}")
