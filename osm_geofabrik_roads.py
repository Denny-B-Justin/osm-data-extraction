# Databricks notebook source
# MAGIC %md
# MAGIC # OSM roads by country (Geofabrik) -> Unity Catalog tables
# MAGIC
# MAGIC For every country in `COUNTRIES` the notebook:
# MAGIC 1. skips it if `prd_mega.saiana95.osm_geofabrik_<iso2>` already exists (nothing is logged),
# MAGIC 2. otherwise downloads the Geofabrik `.osm.pbf` into the Volume,
# MAGIC 3. extracts the roads in one pyosmium pass and appends them to the UC table in chunks of 200,000 rows,
# MAGIC 4. logs success / failure to `prd_mega.saiana95.osm_geofabrik_run_log`.
# MAGIC
# MAGIC The loop never stops on an error: a failed country is logged and the next one starts.

# COMMAND ----------

# MAGIC %pip install osmium

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# =============================================================================
# OSM ROADS - Geofabrik country extracts -> one UC table per country
# =============================================================================
# Reads  : download.geofabrik.de  (one country .osm.pbf, cached in a Volume)
# Writes : prd_mega.saiana95.osm_geofabrik_<iso2>   road segments, WKT geometry
#          prd_mega.saiana95.osm_geofabrik_run_log  one row per success / failure
#
# Roads only (highway=* in OSM_ROAD_HIGHWAY_VALUES). Spark only: no geopandas /
# shapely - each LINESTRING is built as WKT text while parsing, and every chunk
# of rows becomes a Spark DataFrame appended to the UC table.
# =============================================================================

import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

import osmium
import requests
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

CATALOG = "prd_mega"
SCHEMA = "sgpbpi163"
TABLE_PREFIX = "osm_geofabrik_"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.osm_geofabrik_run_log"

# One flat folder: <geofabrik name>-latest.osm.pbf for every country.
VOLUME_DIR = "/Volumes/prd_mega/sgpbpi163/vgpbpi163/OSM/geofabrik_country/"

GEOFABRIK_BASE = "https://download.geofabrik.de"
GEOFABRIK_INDEX = f"{GEOFABRIK_BASE}/index-v1-nogeom.json"

# Re-download even if the PBF is already in the Volume ("-latest" is rebuilt daily).
FORCE_DOWNLOAD = False

# Rows buffered before they are flushed to the UC table.
CHUNK_ROWS = 200_000

# PBFs above this size use a disk-backed node-location index instead of RAM.
BIG_PBF_BYTES = 500 * 1024 ** 2

# Parsing is done from local disk (much faster than the Volume mount).
_local_base = "/local_disk0" if os.path.isdir("/local_disk0") else "/tmp"
LOCAL_DIR = os.path.join(_local_base, "osm_tmp")

# Smoke test: list a few ISO-2 codes here, e.g. ["BT", "LA"]. Empty list = all countries.
ONLY_ISO2 = []

SOURCE_YEAR = datetime.now().year

OSM_ROAD_HIGHWAY_VALUES = [
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "unclassified", "residential", "service", "motorway_link",
    "trunk_link", "primary_link", "secondary_link", "tertiary_link",
    "living_street", "track", "road",
]
ROAD_SET = set(OSM_ROAD_HIGHWAY_VALUES)

# Columns of every country table (geometry = WKT LINESTRING, EPSG:4326).
ROAD_SCHEMA = StructType([
    StructField("osm_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("kind", StringType(), True),
    StructField("geometry", StringType(), True),
    StructField("source_year", LongType(), True),
])

LOG_SCHEMA = StructType([
    StructField("run_ts", TimestampType(), True),
    StructField("iso2", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("status", StringType(), True),       # success / failure
    StructField("uc_table", StringType(), True),     # filled on success
    StructField("row_count", LongType(), True),      # filled on success
    StructField("reason", StringType(), True),       # filled on failure
    StructField("note", StringType(), True),         # e.g. shared-extract warning
])

# COMMAND ----------

# -----------------------------------------------------------------------------
# STEP 1 - Countries and their Geofabrik location
# -----------------------------------------------------------------------------
# (ISO-2, country name, Geofabrik folder, Geofabrik file name)
#   URL = https://download.geofabrik.de/<folder>/<file name>-latest.osm.pbf
#   folder "" = top level (Russia). None / None = no entry of its own in the
#   static list; resolve_pbf() then searches Geofabrik's index by ISO-2 code and
#   logs a failure if Geofabrik really has no extract.

COUNTRIES = [
    ("AW",  "Aruba",                              None,                     None),
    ("AF",  "Afghanistan",                        "asia",                   "afghanistan"),
    ("AO",  "Angola",                             "africa",                 "angola"),
    ("AI",  "Anguilla",                           None,                     None),
    ("AL",  "Albania",                            "europe",                 "albania"),
    ("AD",  "Andorra",                            "europe",                 "andorra"),
    ("AN",  "Netherlands Antilles",               None,                     None),
    ("AE",  "United Arab Emirates",               "asia",                   "gcc-states"),
    ("AR",  "Argentina",                          "south-america",          "argentina"),
    ("AM",  "Armenia",                            "asia",                   "armenia"),
    ("AS",  "American Samoa",                     "australia-oceania",      "american-oceania"),
    ("AG",  "Antigua and Barbuda",                None,                     None),
    ("AU",  "Australia",                          "australia-oceania",      "australia"),
    ("AT",  "Austria",                            "europe",                 "austria"),
    ("AZ",  "Azerbaijan",                         "asia",                   "azerbaijan"),
    ("BI",  "Burundi",                            "africa",                 "burundi"),
    ("BE",  "Belgium",                            "europe",                 "belgium"),
    ("BJ",  "Benin",                              "africa",                 "benin"),
    ("BF",  "Burkina Faso",                       "africa",                 "burkina-faso"),
    ("BD",  "Bangladesh",                         "asia",                   "bangladesh"),
    ("BG",  "Bulgaria",                           "europe",                 "bulgaria"),
    ("BH",  "Bahrain",                            "asia",                   "gcc-states"),
    ("BS",  "Bahamas",                            "central-america",        "bahamas"),
    ("BA",  "Bosnia and Herzegovina",             "europe",                 "bosnia-herzegovina"),
    ("BY",  "Belarus",                            "europe",                 "belarus"),
    ("BZ",  "Belize",                             "central-america",        "belize"),
    ("BM",  "Bermuda",                            None,                     None),
    ("BO",  "Bolivia",                            "south-america",          "bolivia"),
    ("BR",  "Brazil",                             "south-america",          "brazil"),
    ("BB",  "Barbados",                           None,                     None),
    ("BN",  "Brunei",                             "asia",                   "malaysia-singapore-brunei"),
    ("BT",  "Bhutan",                             "asia",                   "bhutan"),
    ("BW",  "Botswana",                           "africa",                 "botswana"),
    ("CF",  "Central African Republic",           "africa",                 "central-african-republic"),
    ("CA",  "Canada",                             "north-america",          "canada"),
    ("CC",  "Cocos (Keeling) Islands",            None,                     None),
    ("CH",  "Switzerland",                        "europe",                 "switzerland"),
    ("CL",  "Chile",                              "south-america",          "chile"),
    ("CN",  "China",                              "asia",                   "china"),
    ("CI",  "Cote d'Ivoire",                      "africa",                 "ivory-coast"),
    ("CM",  "Cameroon",                           "africa",                 "cameroon"),
    ("CD",  "Congo, Democratic Republic",         "africa",                 "congo-democratic-republic"),
    ("CG",  "Congo, Republic",                    "africa",                 "congo-brazzaville"),
    ("CK",  "Cook Islands",                       "australia-oceania",      "cook-islands"),
    ("CO",  "Colombia",                           "south-america",          "colombia"),
    ("KM",  "Comoros",                            "africa",                 "comores"),
    ("CV",  "Cape Verde",                         "africa",                 "cape-verde"),
    ("CR",  "Costa Rica",                         "central-america",        "costa-rica"),
    ("CU",  "Cuba",                               "central-america",        "cuba"),
    ("CX",  "Christmas Island",                   None,                     None),
    ("KY",  "Cayman Islands",                     None,                     None),
    ("CY",  "Cyprus",                             "europe",                 "cyprus"),
    ("CZ",  "Czech Republic",                     "europe",                 "czech-republic"),
    ("DE",  "Germany",                            "europe",                 "germany"),
    ("DJ",  "Djibouti",                           "africa",                 "djibouti"),
    ("DM",  "Dominica",                           None,                     None),
    ("DK",  "Denmark",                            "europe",                 "denmark"),
    ("DO",  "Dominican Republic",                 "central-america",        "haiti-and-domrep"),
    ("DZ",  "Algeria",                            "africa",                 "algeria"),
    ("EC",  "Ecuador",                            "south-america",          "ecuador"),
    ("EG",  "Egypt",                              "africa",                 "egypt"),
    ("ER",  "Eritrea",                            "africa",                 "eritrea"),
    ("EH",  "Western Sahara",                     None,                     None),
    ("ES",  "Spain",                              "europe",                 "spain"),
    ("EE",  "Estonia",                            "europe",                 "estonia"),
    ("ET",  "Ethiopia",                           "africa",                 "ethiopia"),
    ("FI",  "Finland",                            "europe",                 "finland"),
    ("FJ",  "Fiji",                               "australia-oceania",      "fiji"),
    ("FK",  "Falkland Islands",                   None,                     None),
    ("FR",  "France",                             "europe",                 "france"),
    ("FO",  "Faroe Islands",                      "europe",                 "faroe-islands"),
    ("FM",  "Micronesia",                         "australia-oceania",      "micronesia"),
    ("GA",  "Gabon",                              "africa",                 "gabon"),
    ("GB",  "United Kingdom",                     "europe",                 "united-kingdom"),
    ("GE",  "Georgia",                            "europe",                 "georgia"),
    ("GH",  "Ghana",                              "africa",                 "ghana"),
    ("GI",  "Gibraltar",                          None,                     None),
    ("GN",  "Guinea",                             "africa",                 "guinea"),
    ("GP",  "Guadeloupe",                         "europe/france",          "guadeloupe"),
    ("GM",  "Gambia",                             "africa",                 "senegal-and-gambia"),
    ("GW",  "Guinea-Bissau",                      "africa",                 "guinea-bissau"),
    ("GQ",  "Equatorial Guinea",                  "africa",                 "equatorial-guinea"),
    ("GR",  "Greece",                             "europe",                 "greece"),
    ("GD",  "Grenada",                            None,                     None),
    ("GL",  "Greenland",                          "north-america",          "greenland"),
    ("GT",  "Guatemala",                          "central-america",        "guatemala"),
    ("GF",  "French Guiana",                      "europe/france",          "guyane"),
    ("GU",  "Guam",                               "australia-oceania",      "american-oceania"),
    ("GY",  "Guyana",                             "south-america",          "guyana"),
    ("HK",  "Hong Kong",                          "asia/china",             "hong-kong"),
    ("HN",  "Honduras",                           "central-america",        "honduras"),
    ("HR",  "Croatia",                            "europe",                 "croatia"),
    ("HT",  "Haiti",                              "central-america",        "haiti-and-domrep"),
    ("HU",  "Hungary",                            "europe",                 "hungary"),
    ("ID",  "Indonesia",                          "asia",                   "indonesia"),
    ("IN",  "India",                              "asia",                   "india"),
    ("IO",  "British Indian Ocean Territory",     None,                     None),
    ("IE",  "Ireland",                            "europe",                 "ireland-and-northern-ireland"),
    ("IR",  "Iran",                               "asia",                   "iran"),
    ("IQ",  "Iraq",                               "asia",                   "iraq"),
    ("IS",  "Iceland",                            "europe",                 "iceland"),
    ("IL",  "Israel",                             "asia",                   "israel-and-palestine"),
    ("IT",  "Italy",                              "europe",                 "italy"),
    ("JM",  "Jamaica",                            "central-america",        "jamaica"),
    ("JO",  "Jordan",                             "asia",                   "jordan"),
    ("JP",  "Japan",                              "asia",                   "japan"),
    ("KZ",  "Kazakhstan",                         "asia",                   "kazakhstan"),
    ("KE",  "Kenya",                              "africa",                 "kenya"),
    ("KG",  "Kyrgyzstan",                         "asia",                   "kyrgyzstan"),
    ("KH",  "Cambodia",                           "asia",                   "cambodia"),
    ("KI",  "Kiribati",                           "australia-oceania",      "kiribati"),
    ("KN",  "Saint Kitts and Nevis",              None,                     None),
    ("KR",  "South Korea",                        "asia",                   "south-korea"),
    ("KW",  "Kuwait",                             "asia",                   "gcc-states"),
    ("LA",  "Laos",                               "asia",                   "laos"),
    ("LB",  "Lebanon",                            "asia",                   "lebanon"),
    ("LR",  "Liberia",                            "africa",                 "liberia"),
    ("LY",  "Libya",                              "africa",                 "libya"),
    ("LC",  "Saint Lucia",                        None,                     None),
    ("LI",  "Liechtenstein",                      "europe",                 "liechtenstein"),
    ("LK",  "Sri Lanka",                          "asia",                   "sri-lanka"),
    ("LS",  "Lesotho",                            "africa",                 "lesotho"),
    ("LT",  "Lithuania",                          "europe",                 "lithuania"),
    ("LU",  "Luxembourg",                         "europe",                 "luxembourg"),
    ("LV",  "Latvia",                             "europe",                 "latvia"),
    ("MO",  "Macao",                              "asia/china",             "macau"),
    ("MA",  "Morocco",                            "africa",                 "morocco"),
    ("MC",  "Monaco",                             "europe",                 "monaco"),
    ("MD",  "Moldova",                            "europe",                 "moldova"),
    ("MG",  "Madagascar",                         "africa",                 "madagascar"),
    ("MV",  "Maldives",                           "asia",                   "maldives"),
    ("MX",  "Mexico",                             "north-america",          "mexico"),
    ("MH",  "Marshall Islands",                   "australia-oceania",      "marshall-islands"),
    ("MK",  "North Macedonia",                    "europe",                 "macedonia"),
    ("ML",  "Mali",                               "africa",                 "mali"),
    ("MT",  "Malta",                              "europe",                 "malta"),
    ("MM",  "Myanmar",                            "asia",                   "myanmar"),
    ("MN",  "Mongolia",                           "asia",                   "mongolia"),
    ("MP",  "Northern Mariana Islands",           "australia-oceania",      "american-oceania"),
    ("MZ",  "Mozambique",                         "africa",                 "mozambique"),
    ("MR",  "Mauritania",                         "africa",                 "mauritania"),
    ("MS",  "Montserrat",                         None,                     None),
    ("MQ",  "Martinique",                         "europe/france",          "martinique"),
    ("MU",  "Mauritius",                          "africa",                 "mauritius"),
    ("MW",  "Malawi",                             "africa",                 "malawi"),
    ("MY",  "Malaysia",                           "asia",                   "malaysia-singapore-brunei"),
    ("YT",  "Mayotte",                            "europe/france",          "mayotte"),
    ("NA",  "Namibia",                            "africa",                 "namibia"),
    ("NC",  "New Caledonia",                      "australia-oceania",      "new-caledonia"),
    ("NE",  "Niger",                              "africa",                 "niger"),
    ("NF",  "Norfolk Island",                     None,                     None),
    ("NG",  "Nigeria",                            "africa",                 "nigeria"),
    ("NI",  "Nicaragua",                          "central-america",        "nicaragua"),
    ("NU",  "Niue",                               "australia-oceania",      "niue"),
    ("NL",  "Netherlands",                        "europe",                 "netherlands"),
    ("NO",  "Norway",                             "europe",                 "norway"),
    ("NP",  "Nepal",                              "asia",                   "nepal"),
    ("NR",  "Nauru",                              "australia-oceania",      "nauru"),
    ("NZ",  "New Zealand",                        "australia-oceania",      "new-zealand"),
    ("OM",  "Oman",                               "asia",                   "gcc-states"),
    ("PK",  "Pakistan",                           "asia",                   "pakistan"),
    ("PA",  "Panama",                             "central-america",        "panama"),
    ("PN",  "Pitcairn Islands",                   "australia-oceania",      "pitcairn-islands"),
    ("PE",  "Peru",                               "south-america",          "peru"),
    ("PH",  "Philippines",                        "asia",                   "philippines"),
    ("PW",  "Palau",                              "australia-oceania",      "palau"),
    ("PG",  "Papua New Guinea",                   "australia-oceania",      "papua-new-guinea"),
    ("PL",  "Poland",                             "europe",                 "poland"),
    ("PR",  "Puerto Rico",                        "north-america/us",       "puerto-rico"),
    ("KP",  "North Korea",                        "asia",                   "north-korea"),
    ("PT",  "Portugal",                           "europe",                 "portugal"),
    ("PY",  "Paraguay",                           "south-america",          "paraguay"),
    ("PS",  "Palestine",                          "asia",                   "israel-and-palestine"),
    ("PF",  "French Polynesia",                   "australia-oceania",      "polynesie-francaise"),
    ("QA",  "Qatar",                              "asia",                   "gcc-states"),
    ("RE",  "Reunion",                            "europe/france",          "reunion"),
    ("RO",  "Romania",                            "europe",                 "romania"),
    ("RU",  "Russia",                             "",                       "russia"),
    ("RW",  "Rwanda",                             "africa",                 "rwanda"),
    ("SA",  "Saudi Arabia",                       "asia",                   "gcc-states"),
    ("CS",  "Serbia and Montenegro",              None,                     None),
    ("SD",  "Sudan",                              "africa",                 "sudan"),
    ("SN",  "Senegal",                            "africa",                 "senegal-and-gambia"),
    ("SG",  "Singapore",                          "asia",                   "malaysia-singapore-brunei"),
    ("SH",  "Saint Helena",                       "africa",                 "saint-helena-ascension-and-tristan-da-cunha"),
    ("SJ",  "Svalbard and Jan Mayen",             None,                     None),
    ("SB",  "Solomon Islands",                    "australia-oceania",      "solomon-islands"),
    ("SL",  "Sierra Leone",                       "africa",                 "sierra-leone"),
    ("SV",  "El Salvador",                        "central-america",        "el-salvador"),
    ("SM",  "San Marino",                         None,                     None),
    ("SO",  "Somalia",                            "africa",                 "somalia"),
    ("PM",  "Saint Pierre and Miquelon",          None,                     None),
    ("ST",  "Sao Tome and Principe",              "africa",                 "sao-tome-and-principe"),
    ("SR",  "Suriname",                           "south-america",          "suriname"),
    ("SK",  "Slovakia",                           "europe",                 "slovakia"),
    ("SI",  "Slovenia",                           "europe",                 "slovenia"),
    ("SE",  "Sweden",                             "europe",                 "sweden"),
    ("SZ",  "Eswatini",                           "africa",                 "swaziland"),
    ("SC",  "Seychelles",                         "africa",                 "seychelles"),
    ("SY",  "Syria",                              "asia",                   "syria"),
    ("TC",  "Turks and Caicos Islands",           None,                     None),
    ("TD",  "Chad",                               "africa",                 "chad"),
    ("TG",  "Togo",                               "africa",                 "togo"),
    ("TH",  "Thailand",                           "asia",                   "thailand"),
    ("TJ",  "Tajikistan",                         "asia",                   "tajikistan"),
    ("TK",  "Tokelau",                            "australia-oceania",      "tokelau"),
    ("TM",  "Turkmenistan",                       "asia",                   "turkmenistan"),
    ("TL",  "Timor-Leste",                        "asia",                   "east-timor"),
    ("TO",  "Tonga",                              "australia-oceania",      "tonga"),
    ("TT",  "Trinidad and Tobago",                None,                     None),
    ("TN",  "Tunisia",                            "africa",                 "tunisia"),
    ("TR",  "Turkey",                             "europe",                 "turkey"),
    ("TV",  "Tuvalu",                             "australia-oceania",      "tuvalu"),
    ("TW",  "Taiwan",                             "asia",                   "taiwan"),
    ("TZ",  "Tanzania",                           "africa",                 "tanzania"),
    ("UG",  "Uganda",                             "africa",                 "uganda"),
    ("UA",  "Ukraine",                            "europe",                 "ukraine"),
    ("UM",  "US Minor Outlying Islands",          None,                     None),
    ("UY",  "Uruguay",                            "south-america",          "uruguay"),
    ("US",  "United States",                      "north-america",          "us"),
    ("UZ",  "Uzbekistan",                         "asia",                   "uzbekistan"),
    ("VA",  "Vatican City",                       None,                     None),
    ("VC",  "Saint Vincent and the Grenadines",   None,                     None),
    ("VE",  "Venezuela",                          "south-america",          "venezuela"),
    ("VG",  "British Virgin Islands",             None,                     None),
    ("VI",  "US Virgin Islands",                  "north-america/us",       "us-virgin-islands"),
    ("VN",  "Vietnam",                            "asia",                   "vietnam"),
    ("VU",  "Vanuatu",                            "australia-oceania",      "vanuatu"),
    ("WF",  "Wallis and Futuna",                  "australia-oceania",      "wallis-et-futuna"),
    ("WS",  "Samoa",                              "australia-oceania",      "samoa"),
    ("YE",  "Yemen",                              "asia",                   "yemen"),
    ("ZA",  "South Africa",                       "africa",                 "south-africa"),
    ("ZM",  "Zambia",                             "africa",                 "zambia"),
    ("ZW",  "Zimbabwe",                           "africa",                 "zimbabwe"),
]

# Geofabrik extracts that cover more than one code. The whole extract is written
# to the table of every code that uses it, so those tables also hold the
# neighbouring territories' roads (this is added to the log as a note).
SHARED_EXTRACTS = {
    "gcc-states": "AE, BH, KW, OM, QA, SA",
    "malaysia-singapore-brunei": "MY, SG, BN",
    "haiti-and-domrep": "HT, DO",
    "israel-and-palestine": "IL, PS",
    "senegal-and-gambia": "SN, GM",
    "american-oceania": "AS, GU, MP and other US Pacific territories",
    "ireland-and-northern-ireland": "IE and Northern Ireland (GB)",
    "indonesia": "ID and TL (East Timor)",
}

# ISO codes that no longer exist, so there is nothing to download.
OBSOLETE_CODES = {
    "AN": "Netherlands Antilles, dissolved in 2010",
    "CS": "Serbia and Montenegro, dissolved in 2006",
}

# COMMAND ----------

# -----------------------------------------------------------------------------
# STEP 2 - UC table name for every country
# -----------------------------------------------------------------------------

TABLE_NAMES = {
    iso2: f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}{iso2.lower()}"
    for iso2, _name, _folder, _slug in COUNTRIES
}

print(f"{len(COUNTRIES)} countries, {len(TABLE_NAMES)} potential tables")

# COMMAND ----------

# -----------------------------------------------------------------------------
# Helpers: run log, table check, Geofabrik URL, download
# -----------------------------------------------------------------------------


def log_result(iso2, name, status, uc_table=None, row_count=None, reason=None, note=None):
    """Append one row to the run-log table. Never raises, so the loop cannot break here."""
    try:
        row = (datetime.now(timezone.utc), iso2, name, status, uc_table, row_count, reason, note)
        spark.createDataFrame([row], schema=LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)
    except Exception as exc:
        print(f"  WARNING: could not write the run log for {iso2}: {exc}")


def table_exists(table):
    return spark.catalog.tableExists(table)


_index_cache = []  # Geofabrik's index, downloaded at most once


def resolve_pbf(iso2, name, folder, slug):
    """Return (pbf_url, note). Raises RuntimeError when Geofabrik has no extract for the code."""
    if iso2 in OBSOLETE_CODES:
        raise RuntimeError(
            f"Obsolete ISO code ({OBSOLETE_CODES[iso2]}); Geofabrik has no extract for it"
        )

    if slug:
        url = f"{GEOFABRIK_BASE}/{folder + '/' if folder else ''}{slug}-latest.osm.pbf"
        covers = SHARED_EXTRACTS.get(slug)
        note = f"Shared extract, table also holds roads of: {covers}" if covers else None
        return url, note

    # No static path: look the code up in Geofabrik's own index.
    try:
        if not _index_cache:
            response = requests.get(GEOFABRIK_INDEX, timeout=120)
            response.raise_for_status()
            _index_cache.extend(f["properties"] for f in response.json()["features"])
    except Exception as exc:
        raise RuntimeError(
            f"No static Geofabrik path for {name} ({iso2}) and the index could not be read "
            f"({GEOFABRIK_INDEX}): {exc}"
        )

    matches = [
        r for r in _index_cache
        if iso2 in (r.get("iso3166-1:alpha2") or []) and (r.get("urls") or {}).get("pbf")
    ]
    if not matches:
        raise RuntimeError(
            f"No Geofabrik extract exists for {name} ({iso2}); checked {GEOFABRIK_INDEX}"
        )
    best = min(matches, key=lambda r: len(r["iso3166-1:alpha2"]))  # prefer single-country
    codes = best["iso3166-1:alpha2"]
    note = "Found via Geofabrik index"
    if len(codes) > 1:
        note += f"; shared extract, table also holds roads of: {', '.join(codes)}"
    return best["urls"]["pbf"], note


def _size(path):
    return f"{os.path.getsize(path) / 1024 ** 2:,.0f} MB"


def ensure_pbf(url):
    """Make sure the PBF is in the Volume and return a local-disk copy to parse."""
    filename = url.rsplit("/", 1)[-1]
    os.makedirs(VOLUME_DIR, exist_ok=True)
    os.makedirs(LOCAL_DIR, exist_ok=True)
    volume_path = os.path.join(VOLUME_DIR, filename)
    local_path = os.path.join(LOCAL_DIR, filename)

    if os.path.isfile(volume_path) and os.path.getsize(volume_path) > 0 and not FORCE_DOWNLOAD:
        print(f"  already in volume, skipping download: {volume_path} ({_size(volume_path)})")
        print("  copying to local disk ...")
        shutil.copyfile(volume_path, local_path)
        return local_path

    print(f"  downloading {url}")
    if os.path.exists(local_path):  # never resume onto an older snapshot
        os.remove(local_path)
    result = subprocess.run(
        ["wget", "-c", "-nv", "--tries=5", "--timeout=60", "--waitretry=15",
         "-O", local_path, url],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not os.path.isfile(local_path) or os.path.getsize(local_path) == 0:
        raise RuntimeError(
            f"Download failed (wget exit {result.returncode}) for {url} - {result.stderr.strip()[-300:]}"
        )
    print(f"  downloaded {_size(local_path)}")

    # Copy under a .part name, then rename, so a half-copied file is never reused.
    tmp = volume_path + ".part"
    shutil.copyfile(local_path, tmp)
    try:
        os.replace(tmp, volume_path)
    except OSError:
        shutil.move(tmp, volume_path)
    print(f"  stored in volume: {volume_path}")
    return local_path

# COMMAND ----------

# -----------------------------------------------------------------------------
# Road writer (chunked flush to UC) and PBF parser
# -----------------------------------------------------------------------------


class RoadWriter:
    """Buffers road rows and appends them to the country's UC table every CHUNK_ROWS rows."""

    def __init__(self, table):
        self.table = table
        self.rows = []
        self.count = 0  # rows already written to the table

    def add(self, row):
        self.rows.append(row)
        if len(self.rows) >= CHUNK_ROWS:
            self.flush()

    def flush(self):
        if not self.rows:
            return
        sdf = spark.createDataFrame(self.rows, schema=ROAD_SCHEMA)
        sdf.coalesce(1).write.mode("append").saveAsTable(self.table)
        self.count += len(self.rows)
        self.rows = []
        print(f"    {self.count:,} rows written")


def to_wkt(coords):
    return "LINESTRING (" + ", ".join(f"{lon:.7f} {lat:.7f}" for lon, lat in coords) + ")"


class RoadHandler(osmium.SimpleHandler):
    """Keeps every way with highway=* in ROAD_SET and hands it to the writer."""

    def __init__(self, writer):
        super().__init__()
        self.writer = writer

    def way(self, w):
        highway = w.tags.get("highway")
        if highway not in ROAD_SET:
            return
        coords = [(nd.lon, nd.lat) for nd in w.nodes if nd.location.valid()]
        if len(coords) < 2:  # fewer than two nodes cannot form a line
            return
        self.writer.add((w.id, w.tags.get("name"), highway, to_wkt(coords), SOURCE_YEAR))


def extract_roads(pbf_path, writer):
    """One pass over the PBF feeding the writer."""
    index_file = None
    if os.path.getsize(pbf_path) < BIG_PBF_BYTES:
        index = "flex_mem"
    else:
        index_file = os.path.join(LOCAL_DIR, "node_locations.idx")
        if os.path.exists(index_file):  # leftover from an interrupted run
            os.remove(index_file)
        index = f"sparse_file_array,{index_file}"

    print(f"  parsing {os.path.basename(pbf_path)} (node index: {index.split(',')[0]}) ...")
    started = time.time()
    try:
        RoadHandler(writer).apply_file(
            pbf_path,
            locations=True,
            idx=index,
            filters=[osmium.filter.KeyFilter("highway")],
        )
    finally:
        if index_file and os.path.exists(index_file):
            os.remove(index_file)
    writer.flush()  # last, partly filled chunk
    print(f"  parsed in {time.time() - started:,.0f}s")

# COMMAND ----------

# -----------------------------------------------------------------------------
# STEP 3-8 - Loop over the countries (never stops on an error)
# -----------------------------------------------------------------------------

shutil.rmtree(LOCAL_DIR, ignore_errors=True)  # clear leftovers from an earlier session
summary = []  # (iso2, country, status, detail)

for iso2, name, folder, slug in COUNTRIES:
    if ONLY_ISO2 and iso2 not in ONLY_ISO2:
        continue

    table = TABLE_NAMES[iso2]
    print(f"\n{'-' * 70}\n{name} ({iso2})\n  table: {table}")

    url = None
    writer = None
    try:
        # 3. table already there -> skip, no log
        if table_exists(table):
            print("  exists, skipping")
            summary.append((iso2, name, "skipped", table))
            continue

        # 4-5. download the .osm.pbf, extract the roads in chunks
        url, note = resolve_pbf(iso2, name, folder, slug)
        local_pbf = ensure_pbf(url)
        writer = RoadWriter(table)
        extract_roads(local_pbf, writer)

        # a. no roads = failure
        if writer.count == 0:
            raise RuntimeError(
                f"No road features (highway={'|'.join(OSM_ROAD_HIGHWAY_VALUES)}) found in {url}"
            )

        # 6-7. done, log it
        log_result(iso2, name, "success", uc_table=table, row_count=writer.count, note=note)
        summary.append((iso2, name, "success", f"{writer.count:,} rows -> {table}"))
        print(f"  SUCCESS: {writer.count:,} rows -> {table}")

    except Exception as exc:
        # b, d. any problem (bad link, parse error, Spark error ...) is logged and the loop goes on
        reason = f"{type(exc).__name__}: {exc}"[:1500]
        if url and url not in reason:
            reason += f" | extract: {url}"
        print(f"  FAILED: {reason}")

        # Drop a half-written table so a re-run does not mistake it for a finished one.
        if writer is not None and writer.count > 0:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table}")
            except Exception as drop_exc:
                print(f"  WARNING: could not drop partial table {table}: {drop_exc}")

        log_result(iso2, name, "failure", reason=reason)
        summary.append((iso2, name, "failure", reason))

    # finally:
    #     shutil.rmtree(LOCAL_DIR, ignore_errors=True)  # free the local disk after every country

# COMMAND ----------

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

n_ok = sum(1 for s in summary if s[2] == "success")
n_fail = sum(1 for s in summary if s[2] == "failure")
n_skip = sum(1 for s in summary if s[2] == "skipped")

print("=" * 70)
print("OSM ROADS RUN COMPLETE")
print("=" * 70)
print(f"  success: {n_ok} | failed: {n_fail} | skipped (table exists): {n_skip}")

for iso2, name, status, detail in summary:
    if status == "failure":
        print(f"  FAILED  {iso2}  {name}: {detail[:160]}")

print(f"\nFull log: {LOG_TABLE}")
