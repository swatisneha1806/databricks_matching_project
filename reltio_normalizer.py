"""Data normalization pipeline for the Reltio matching service."""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Dict, Optional, Tuple

from pyspark.sql import SparkSession

try:
    from .match_config import STATE_MAP
    from .matching_utils import fix_encoding, cleanse_supplier_name
except ImportError:
    from match_config import STATE_MAP
    from matching_utils import fix_encoding, cleanse_supplier_name

# Fields used to build the cache-lookup hash (order matters for JSON serialisation)
MATCH_KEY_FIELDS: tuple[str, ...] = ("supplier_name", "street_address", "city", "state", "postal_code", "country")

# State values that are invalid / unknown placeholders or country codes used as state
INVALID_STATE_VALUES: frozenset[str] = frozenset({"UNK", "UNKNOWN", "NULL", "BOG", "000", "N.L", "Q R", "XX", "ZAF"})

# Known dummy postal codes used as placeholders in source data
DUMMY_POSTAL_CODES: frozenset[str] = frozenset({"12212", "90021", "852", "853"})


class CountryLookup:
    """Resolution flow inside normalize_country(): raw input  →  alias_map  →  country_name  →  name_iso_map  →  iso2"""

    def __init__(self, name_iso_map: Dict[str, str], alias_map: Dict[str, str]) -> None:
        self.name_iso_map = name_iso_map
        self.alias_map = alias_map

    def resolve(self, raw: str) -> Tuple[str, str]:
        """Return (country_name, iso2) for any raw country input."""
        if not raw:
            return "", ""

        raw_upper = raw.strip().upper()

        # Step 1: alias → canonical country_name
        country_name = self.alias_map.get(raw_upper)

        # Step 2: if alias hit, look up iso2; otherwise try name_iso_map directly
        if country_name:
            iso2 = self.name_iso_map.get(country_name.upper(), "")
            return country_name, iso2

        # Direct name lookup (handles inputs that are already the canonical name)
        iso2 = self.name_iso_map.get(raw_upper)
        if iso2 is not None:
            return raw.strip(), iso2

        return raw.strip(), ""

    def is_known_country_key(self, value: str) -> bool:
        """Return True if value is a recognised country alias or name (used to reject country codes as postal codes)."""
        v = value.strip().upper()
        return v in self.alias_map or v in self.name_iso_map

    def __bool__(self) -> bool:
        return bool(self.name_iso_map)


def _load_name_iso_map(spark: SparkSession, table_name: str) -> Dict[str, str]:
    """Load country_map table → {UPPER(country_name): iso2}."""
    df = spark.read.table(table_name)
    missing = {"country_name", "country_code_2alpha"} - set(df.columns)
    if missing:
        raise ValueError(f"country_map table '{table_name}' is missing columns: {missing}")

    rows = df.select("country_name", "country_code_2alpha").dropna().collect()
    result: Dict[str, str] = {}
    for r in rows:
        name = str(r["country_name"]).strip()
        iso2 = str(r["country_code_2alpha"]).strip()
        if name and iso2:
            result[name.upper()] = iso2
    logging.info(f"Loaded {len(result)} entries from country_map table '{table_name}'")
    return result


def _load_alias_map(spark: SparkSession, table_name: str) -> Dict[str, str]:
    """Load country_alias_map table → {UPPER(alias): country_name}."""
    df = spark.read.table(table_name)
    missing = {"alias", "country_name"} - set(df.columns)
    if missing:
        raise ValueError(f"country_alias_map table '{table_name}' is missing columns: {missing}")

    rows = df.select("alias", "country_name").dropna().collect()
    result: Dict[str, str] = {}
    for r in rows:
        alias = str(r["alias"]).strip()
        name = str(r["country_name"]).strip()
        if alias and name:
            result[alias.upper()] = name
    logging.info(f"Loaded {len(result)} entries from country_alias_map table '{table_name}'")
    return result


def load_country_lookup(spark: SparkSession, cnty_map_tbl_name: str, cnty_alias_map_tbl_name: str) -> CountryLookup:
    """Load country reference data from two optional Spark tables at startup."""
    alias_map: Dict[str, str] = {}
    if cnty_alias_map_tbl_name:
        try:
            alias_map = _load_alias_map(spark, cnty_alias_map_tbl_name)
        except Exception as e:
            logging.warning(f"Could not load country_alias_map table '{cnty_alias_map_tbl_name}': {e}. Using static COUNTRY_MAP as alias fallback.")

    name_iso_map: Dict[str, str] = {}
    if cnty_map_tbl_name:
        try:
            name_iso_map = _load_name_iso_map(spark, cnty_map_tbl_name)
        except Exception as e:
            logging.warning(f"Could not load country_map table '{cnty_map_tbl_name}': {e}. ISO-2 codes will not be resolved.")

    return CountryLookup(name_iso_map, alias_map)


def normalize_state(state: str, cntry: str) -> str:
    """Normalise state input to standard abbreviation.

    For US 2-letter codes, prefixes "USA-" (e.g. "CA" → "USA-CA").Returns original string if no mapping applies.
    """
    if not state:
        return ""

    state_upper = str(state).strip().upper()

    if len(state_upper) == 2 and state_upper.isalpha() and cntry == "USA":
        return f"USA-{state_upper}"

    if len(state_upper) == 2 and state_upper.isalpha():
        return state_upper

    return str(state).strip()


def normalize_country(country: str, country_lookup: CountryLookup) -> Tuple[str, str]:
    return country_lookup.resolve(country)


def is_valid_postal_code(postal_code: str, country_lookup: Optional[CountryLookup] = None) -> bool:
    """Return True if postal_code looks like a real postal code.

    Rejects: null strings, too-short values, known dummy codes, all-zero or
    all-same-digit strings, phone-number-length strings, country codes / names
    used as postal codes, and strings with excessive leading zeros.
    """
    if not postal_code:
        return False
    pc = postal_code.strip()
    digits = re.sub(r'\D', '', pc)

    is_country_key = country_lookup.is_known_country_key(pc)

    return not any([
        pc.lower() in ("null", "none", "n/a", "na", "-"),  # null-like strings
        len(pc) < 3,  # too short
        pc in DUMMY_POSTAL_CODES,  # known placeholders
        bool(digits) and all(d == '0' for d in digits),  # all zeros
        pc.isdigit() and len(set(pc)) == 1,  # repeated single digit
        pc.isdigit() and len(pc) > 10,  # phone-number length
        len(digits) > 6 and digits.startswith('000'),  # excessive leading zeros
        is_country_key,  # country code used as postal
    ])


def normalize_input_row(row: Dict, country_lookup: CountryLookup = None) -> Dict:
    """Normalise a single raw input record.

    Produces ``norm_*`` prefixed fields consumed by the filter builder and scorer:
        norm_supplier_name, norm_street_address, norm_city, norm_postal_code,
        norm_country, norm_state, country_iso2, full_form_state,
        cleansed_supplier_name

    Args:
        row:            Raw input record dict.
        country_lookup: Pre-built CountryLookup from load_country_lookup.
                        Pass None to rely solely on the static COUNTRY_MAP.
    """
    if country_lookup is None:
        country_lookup = CountryLookup({}, {})

    def _clean(v) -> str:
        """Strip control characters, collapse whitespace, fix encoding, truncate."""
        if not v:
            return ""
        cleaned = re.sub(r'[\r\n\t\x00-\x1f\x7f]', ' ', str(v))
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        cleaned = fix_encoding(cleaned)
        return cleaned[:200].strip() if len(cleaned) > 200 else cleaned

    normalized = row.copy()

    # Clean core string fields
    for field in ("supplier_name", "street_address", "city", "postal_code"):
        if field in normalized and normalized[field]:
            normalized[f"norm_{field}"] = _clean(normalized[field])

    # Reject dummy / invalid postal codes
    if "norm_postal_code" in normalized and not is_valid_postal_code(normalized["norm_postal_code"], country_lookup):
        normalized["norm_postal_code"] = ""

    # Cleansed supplier name (suffix-stripped, camelCase split, lowercase)
    if "supplier_name" in normalized:
        normalized["cleansed_supplier_name"] = cleanse_supplier_name(normalized.get("norm_supplier_name", ""))

    # Country → norm_country (display name) + country_iso2 (alpha-2)
    if "country" in normalized:
        norm_country, country_iso2 = normalize_country(_clean(normalized["country"]), country_lookup)
        normalized["norm_country"] = norm_country
        normalized["country_iso2"] = country_iso2

    # State → norm_state + full_form_state
    if "state" in normalized:
        normalized["norm_state"] = normalize_state(_clean(normalized["state"]), normalized.get("norm_country", normalized.get("country", "")))
        normalized["full_form_state"] = STATE_MAP.get(str(normalized["state"]).strip().upper(), str(normalized["state"]).strip())

    return normalized


def build_request_hash(row: Dict) -> str:
    """Build a deterministic SHA-256 hash over the match-key fields for cache lookup."""
    canon = {k: str(row.get(k, "")).strip().lower() for k in MATCH_KEY_FIELDS}
    canonical_json = json.dumps(canon, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
