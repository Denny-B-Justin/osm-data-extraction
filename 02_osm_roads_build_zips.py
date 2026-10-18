# Databricks notebook source
# MAGIC %md
# MAGIC # OSM roads: one zip per country in the UC Volume
# MAGIC
# MAGIC Turns every `prd_mega.sgpbpi163.osm_geofabrik_<iso2>` table into a download for the web app:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/prd_mega/sgpbpi163/vgpbpi163/OSM/osm_road_all_country/
# MAGIC     vn_roads.zip        road_openstreetmap_vn_part00001.csv ... (at most 200,000 rows each) + README.txt
# MAGIC     ...
# MAGIC     manifest.json       what is available: name, rows, parts, size, columns, dates
# MAGIC ```
# MAGIC
# MAGIC For every table the notebook:
# MAGIC 1. skips it when the zip is up to date (same table version and same build settings as last time),
# MAGIC 2. for countries that Geofabrik ships inside a multi-country extract (GCC states, Malaysia/Singapore/Brunei, ...),
# MAGIC    keeps only the roads that touch the country's World Bank boundary,
# MAGIC 3. writes the rows as CSV parts with Spark (`maxRecordsPerFile` = 200,000, rows spread evenly and in no order),
# MAGIC 4. zips the parts on local disk with a README, checks the row count of every part, and copies the zip to the Volume,
# MAGIC 5. updates `manifest.json` after every country, so the app sees each new file as soon as it is ready.
# MAGIC
# MAGIC A failed country is reported and the loop continues; its previous zip (if any) stays published.
# MAGIC Run it after `osm_geofabrik_roads`, by hand or as the next task of the same job.

# COMMAND ----------

# MAGIC %pip install "numpy<2" shapely

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import math
import os
import re
import shutil
import time
import zipfile
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

# COMMAND ----------

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------

CATALOG = "prd_mega"
SCHEMA = "sgpbpi163"
TABLE_PREFIX = "osm_geofabrik_"          # tables are <prefix><iso2>, e.g. osm_geofabrik_vn
SKIP_TABLES = {"osm_geofabrik_run_log"}   # also matches the prefix, but it is the run log

OUT_DIR = "/Volumes/prd_mega/sgpbpi163/vgpbpi163/OSM/osm_road_all_country"
STAGING_DIR = f"{OUT_DIR}/_staging"       # Spark writes the CSV parts here; removed after each country
MANIFEST_NAME = "manifest.json"

ROWS_PER_PART = 200_000                   # hard limit per CSV file
TARGET_ROWS_PER_PART = 195_000            # sets the number of parts; the head-room keeps every part under the limit
ZIP_COMPRESSION_LEVEL = 6

# Countries that Geofabrik only publishes inside a multi-country extract are cut back to their own
# boundary: a road is kept when it touches the (slightly buffered) World Bank boundary.
CLIP_SHARED_EXTRACTS = True
BOUNDARY_TABLE = "prd_mega.sgpbpi163.cbd_world_boundaries_dissolved"   # columns iso_a3, geometry_json (GeoJSON)
CLIP_BUFFER_DEG = 0.002                   # ~200 m, keeps coastal roads and causeways the boundary line just misses
BOUNDARY_SIMPLIFY_DEG = 0.0005            # ~50 m, keeps the polygon small enough to ship to the executors
# When a boundary is missing: "keep_unclipped" publishes the whole shared extract (the app's documentation
# then says the file includes neighbouring roads); "fail" skips the country instead.
ON_MISSING_BOUNDARY = "keep_unclipped"

FORCE_REBUILD = False                     # True = rebuild every zip, even if up to date
ONLY_ISO2 = []                            # smoke test: e.g. ["BT", "VN"]. Empty list = all tables
PRUNE_MISSING_ZIPS = True                 # drop manifest entries whose zip is no longer in the Volume

BUILD_VERSION = 1                         # bump when the zip layout or README changes: forces a rebuild

_local_base = "/local_disk0" if os.path.isdir("/local_disk0") else "/tmp"
LOCAL_DIR = os.path.join(_local_base, "osm_zip_tmp")
READ_CHUNK = 8 * 1024 * 1024

# COMMAND ----------

# -----------------------------------------------------------------------------
# Countries (same list as the extraction notebook) and the shared extracts
# -----------------------------------------------------------------------------
# (ISO-2, country name, Geofabrik folder, Geofabrik file name)

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
NAMES = {iso2: name for iso2, name, _folder, _slug in COUNTRIES}
EXTRACT_OF = {iso2: slug for iso2, _name, _folder, slug in COUNTRIES}

# Geofabrik extracts that cover more than one country or territory.
SHARED_EXTRACTS = {
    "gcc-states", "malaysia-singapore-brunei", "haiti-and-domrep", "israel-and-palestine",
    "senegal-and-gambia", "american-oceania", "ireland-and-northern-ireland", "indonesia",
}

# Countries cut back to their own boundary: ISO-2 -> iso_a3 in BOUNDARY_TABLE.
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
# Helpers: tables, manifest, README
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


def table_version(table):
    """(fingerprint, last-modified date). The fingerprint changes whenever the table is rewritten."""
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
    modified = detail.get("lastModified")
    fingerprint = f"{detail.get('id')}|{modified}"
    return fingerprint, (modified.date().isoformat() if modified else None)


def build_settings(clip_iso3):
    """Everything that changes the zip besides the data; a change forces a rebuild."""
    return json.dumps({
        "build_version": BUILD_VERSION,
        "rows_per_part": ROWS_PER_PART,
        "clip": [clip_iso3, BOUNDARY_TABLE, CLIP_BUFFER_DEG, BOUNDARY_SIMPLIFY_DEG] if clip_iso3 else None,
    }, sort_keys=True)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def manifest_path():
    return os.path.join(OUT_DIR, MANIFEST_NAME)


def load_manifest():
    try:
        with open(manifest_path(), encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data.get("countries"), dict):
            return data
        print("  WARNING: manifest.json has no 'countries'; starting a new one")
    except FileNotFoundError:
        pass
    return {"manifest_version": 1, "countries": {}}


def copy_to_volume(local_path, final_path):
    """Copy under a .part name, then rename, so the app never sees a half-written file."""
    tmp = final_path + ".part"
    shutil.copyfile(local_path, tmp)
    try:
        os.replace(tmp, final_path)
    except OSError:
        shutil.move(tmp, final_path)
    if os.path.getsize(final_path) != os.path.getsize(local_path):
        raise RuntimeError(f"Size check failed after copying to {final_path}")


def save_manifest(manifest):
    manifest["manifest_version"] = 1
    manifest["updated_at"] = utc_now()
    manifest["rows_per_part"] = ROWS_PER_PART
    manifest["countries"] = dict(sorted(manifest["countries"].items()))
    local = os.path.join(LOCAL_DIR, MANIFEST_NAME)
    with open(local, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=1, ensure_ascii=False)
    copy_to_volume(local, manifest_path())


def readme_text(name, iso2, rows, parts, extracted_on, extract, clipped):
    if parts == 1:
        split = f"This CSV file holds all {rows:,} road segments for {name} ({iso2})."
    else:
        split = (f"These {parts} CSV files together hold all {rows:,} road segments for {name} ({iso2}). "
                 f"Rows were split into files of at most {ROWS_PER_PART:,} in no particular order, so append "
                 "all parts before using the data.")
    lines = [
        f"OpenStreetMap roads - {name} ({iso2})",
        "",
        split,
        "Road data © OpenStreetMap contributors, available under the Open Database License (ODbL): "
        f"https://www.openstreetmap.org/copyright. Extracted from Geofabrik (download.geofabrik.de) on {extracted_on}.",
    ]
    if clipped:
        lines.append(f"Geofabrik publishes {name} inside its combined '{extract}' extract; roads were kept where "
                     "they touch the country's World Bank official boundary.")
    return "\r\n".join(lines) + "\r\n"

# COMMAND ----------

# -----------------------------------------------------------------------------
# Helpers: clip to boundary, CSV parts, zip
# -----------------------------------------------------------------------------


def boundary_wkb(iso3):
    """The country's boundary from BOUNDARY_TABLE, buffered and simplified, as WKB (or None if missing)."""
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


def one_line_per_row(df):
    """Line breaks inside text values would break CSV row counting in some tools: replace them with a space."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType) and field.name != "geometry":
            df = df.withColumn(field.name, F.regexp_replace(F.col(field.name), r"[\r\n]+", " "))
    return df


def write_csv_parts(df, rows, staging):
    """Spread the rows evenly over ceil(rows / TARGET) partitions and write one CSV per partition."""
    num_parts = max(1, math.ceil(rows / TARGET_ROWS_PER_PART))
    (df.repartition(num_parts)
       .write.mode("overwrite")
       .option("header", "true")
       .option("quote", '"')
       .option("escape", '"')             # RFC 4180 ("" inside quotes), not Spark's default backslash
       .option("encoding", "UTF-8")
       .option("lineSep", "\n")
       .option("maxRecordsPerFile", ROWS_PER_PART)
       .csv(staging))
    parts = sorted(os.path.join(staging, f) for f in os.listdir(staging)
                   if f.startswith("part-") and f.endswith(".csv"))
    return [p for p in parts if has_rows(p)]


def has_rows(path):
    """False for a file that only holds the header line."""
    with open(path, "rb") as fh:
        first = fh.readline()
    return os.path.getsize(path) > len(first)


def zip_parts(part_paths, zip_path, iso2):
    """Copy the parts into the zip under their final names, counting data rows as they stream through."""
    counts = []
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=ZIP_COMPRESSION_LEVEL, allowZip64=True) as zf:
        for i, src in enumerate(part_paths, start=1):
            arcname = f"road_openstreetmap_{iso2.lower()}_part{i:05d}.csv"
            newlines, last = 0, b"\n"
            with open(src, "rb") as fin, zf.open(arcname, "w", force_zip64=True) as fout:
                while True:
                    buf = fin.read(READ_CHUNK)
                    if not buf:
                        break
                    newlines += buf.count(b"\n")
                    last = buf[-1:]
                    fout.write(buf)
            lines = newlines + (0 if last == b"\n" else 1)
            counts.append(lines - 1)                      # minus the header
    return counts


def add_readme(zip_path, text):
    with zipfile.ZipFile(zip_path, "a", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.writestr("README.txt", text.encode("utf-8"))


def build_country(iso2, name, table, extracted_on, clip_iso3):
    """Build <iso2>_roads.zip, publish it to the Volume and return its manifest entry."""
    started = time.time()
    df = spark.table(table)
    columns = df.columns
    if "geometry" not in columns:
        raise RuntimeError(f"{table} has no geometry column")

    extract = EXTRACT_OF.get(iso2)
    shared = extract in SHARED_EXTRACTS
    clipped = False
    if clip_iso3:
        area = boundary_wkb(clip_iso3)
        if area is not None:
            df = clip_to_boundary(df, area)
            clipped = True
            print(f"  clipping the shared '{extract}' extract to the {clip_iso3} boundary")
        elif ON_MISSING_BOUNDARY == "fail":
            raise RuntimeError(f"No boundary for {clip_iso3} in {BOUNDARY_TABLE}")
        else:
            print(f"  WARNING: no boundary for {clip_iso3} in {BOUNDARY_TABLE}; publishing the whole "
                  f"'{extract}' extract (the app will say it includes neighbouring roads)")

    df = one_line_per_row(df)
    rows = df.count()
    if rows == 0:
        raise RuntimeError(f"No rows in {table}" + (" after clipping" if clipped else ""))
    print(f"  {rows:,} rows")

    staging = f"{STAGING_DIR}/{iso2.lower()}"
    dbutils.fs.rm(staging, True)
    part_paths = write_csv_parts(df, rows, staging)
    print(f"  wrote {len(part_paths)} CSV part(s)")

    file_name = f"{iso2.lower()}_roads.zip"
    local_zip = os.path.join(LOCAL_DIR, file_name)
    if os.path.exists(local_zip):
        os.remove(local_zip)
    counts = zip_parts(part_paths, local_zip, iso2)
    if sum(counts) != rows:
        raise RuntimeError(f"Row check failed: table has {rows:,} rows, CSV parts have {sum(counts):,}")
    if max(counts) > ROWS_PER_PART:
        raise RuntimeError(f"A part has {max(counts):,} rows, more than {ROWS_PER_PART:,}")
    add_readme(local_zip, readme_text(name, iso2, rows, len(counts), extracted_on, extract, clipped))

    final = os.path.join(OUT_DIR, file_name)
    copy_to_volume(local_zip, final)
    size = os.path.getsize(final)
    print(f"  published {final} ({size / 1024 ** 2:,.1f} MB) in {time.time() - started:,.0f}s")

    return {
        "iso2": iso2,
        "name": name,
        "file": file_name,
        "size_bytes": size,
        "rows": rows,
        "parts": len(counts),
        "columns": columns,
        "table": table,
        "extracted_at": extracted_on,
        "built_at": utc_now(),
        "extract": extract,
        "shared_extract": shared,
        "clipped": clipped,
    }

# COMMAND ----------

# -----------------------------------------------------------------------------
# Build the zips (never stops on an error)
# -----------------------------------------------------------------------------

os.makedirs(OUT_DIR, exist_ok=True)
shutil.rmtree(LOCAL_DIR, ignore_errors=True)
os.makedirs(LOCAL_DIR, exist_ok=True)

manifest = load_manifest()
tables = discover_tables()
print(f"{len(tables)} country tables found in {CATALOG}.{SCHEMA}")

if PRUNE_MISSING_ZIPS:
    gone = [c for c, e in manifest["countries"].items()
            if not os.path.isfile(os.path.join(OUT_DIR, e.get("file", "")))]
    for code in gone:
        print(f"  {code}: zip no longer in the Volume, removed from the manifest")
        del manifest["countries"][code]
    if gone:
        save_manifest(manifest)

summary = []  # (iso2, name, status, detail)

for iso2, table in tables:
    if ONLY_ISO2 and iso2 not in ONLY_ISO2:
        continue
    name = NAMES.get(iso2, iso2)
    print(f"\n{'-' * 70}\n{name} ({iso2})\n  table: {table}")
    try:
        fingerprint, extracted_on = table_version(table)
        clip_iso3 = CLIP_TO_COUNTRY.get(iso2) if CLIP_SHARED_EXTRACTS else None
        settings = build_settings(clip_iso3)
        old = manifest["countries"].get(iso2)
        zip_file = os.path.join(OUT_DIR, f"{iso2.lower()}_roads.zip")
        up_to_date = (
            not FORCE_REBUILD and old is not None
            and old.get("table_fingerprint") == fingerprint
            and old.get("build_settings") == settings
            and os.path.isfile(zip_file) and os.path.getsize(zip_file) == old.get("size_bytes")
        )
        if up_to_date:
            print("  zip is up to date, skipping")
            summary.append((iso2, name, "skipped", ""))
            continue

        entry = build_country(iso2, name, table, extracted_on, clip_iso3)
        entry["table_fingerprint"] = fingerprint
        entry["build_settings"] = settings
        manifest["countries"][iso2] = entry
        save_manifest(manifest)
        summary.append((iso2, name, "success", f"{entry['rows']:,} rows, {entry['parts']} part(s)"))

    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"[:600]
        print(f"  FAILED: {reason}")
        summary.append((iso2, name, "failure", reason))

    finally:
        try:
            dbutils.fs.rm(f"{STAGING_DIR}/{iso2.lower()}", True)
        except Exception as cleanup_exc:
            print(f"  WARNING: could not remove the staging folder: {cleanup_exc}")
        for leftover in os.listdir(LOCAL_DIR):
            if leftover.endswith(".zip"):
                os.remove(os.path.join(LOCAL_DIR, leftover))

try:
    dbutils.fs.rm(STAGING_DIR, True)
except Exception:
    pass

# COMMAND ----------

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

n_ok = sum(1 for s in summary if s[2] == "success")
n_fail = sum(1 for s in summary if s[2] == "failure")
n_skip = sum(1 for s in summary if s[2] == "skipped")
countries = manifest["countries"]

print("=" * 70)
print("OSM ROAD ZIPS COMPLETE")
print("=" * 70)
print(f"  built: {n_ok} | failed: {n_fail} | up to date: {n_skip}")
for iso2, name, status, detail in summary:
    if status == "failure":
        print(f"  FAILED  {iso2}  {name}: {detail[:200]}")
print(f"\nManifest: {manifest_path()}")
print(f"  {len(countries)} countries, {sum(e['rows'] for e in countries.values()):,} road segments, "
      f"{sum(e['size_bytes'] for e in countries.values()) / 1024 ** 3:,.2f} GB")
not_clipped = sorted(c for c, e in countries.items() if e.get("shared_extract") and not e.get("clipped"))
if not_clipped:
    print(f"  shared extracts published unclipped: {', '.join(not_clipped)}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# Check the published files (reads only each zip's directory, not the data)
# -----------------------------------------------------------------------------

problems = []
for code, entry in sorted(countries.items()):
    path = os.path.join(OUT_DIR, entry["file"])
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
        csvs = [n for n in names if n.endswith(".csv")]
        expected = [f"road_openstreetmap_{code.lower()}_part{i:05d}.csv" for i in range(1, entry["parts"] + 1)]
        if csvs != expected or "README.txt" not in names:
            problems.append(f"{code}: unexpected contents {names[:5]}")
        if os.path.getsize(path) != entry["size_bytes"]:
            problems.append(f"{code}: size differs from the manifest")
    except Exception as exc:
        problems.append(f"{code}: {type(exc).__name__}: {exc}")

print(f"{len(countries) - len(problems)} of {len(countries)} zips look right")
for p in problems:
    print("  PROBLEM", p)
