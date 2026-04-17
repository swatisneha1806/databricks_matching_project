from __future__ import annotations

import json
import logging
import os
import re
import time
import warnings
from argparse import Namespace
from typing import Tuple, Optional, Dict

import requests

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

try:
    from .match_config import SEARCH_FILTERS, BROAD_RULES
    from .matching_utils import compute_name_similarity, compute_address_similarity, check_city_match, check_postal_match, calculate_boost, escape_reltio_value
    from .reltio_normalizer import build_request_hash, normalize_state, CountryLookup, normalize_input_row, load_country_lookup
    from .reltio_auth import ReltioTokenManager
except ImportError:
    from match_config import SEARCH_FILTERS, BROAD_RULES
    from matching_utils import compute_name_similarity, compute_address_similarity, check_city_match, check_postal_match, calculate_boost, escape_reltio_value
    from reltio_normalizer import build_request_hash, normalize_state, CountryLookup, normalize_input_row, load_country_lookup
    from reltio_auth import ReltioTokenManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

warnings.filterwarnings("ignore")
REQUIRED_COLS = ["supplier_id", "supplier_name", "state", "city", "postal_code", "country", "street_address", "phone_number"]
SELECT_ATTRIBUTES = (
    "uri,attributes.MMHID,attributes.DoingBusinessAsName,attributes.Name,attributes.URL,attributes.Industry,attributes.MCC_Code,"
    "attributes.SIC,attributes.NAICS,attributes.AggrMerchID,attributes.AggrMerchName,attributes.ParentAggrMerchID,"
    "attributes.ParentAggregateMerchantName,attributes.CLEARING_LAST_SEEN_DATE,attributes.AUTH_LAST_SEEN_DATE,"
    "attributes.ZI_C_LAST_UPDATED_DATE,attributes.Address.AddressLine1,attributes.Address.City,attributes.Address.StateProvince,"
    "attributes.Address.PostalCode,attributes.Address.Country,attributes.Address.RegionName,attributes.Address.FinanceRegioncode,"
    "attributes.Address.ISO3166-3"
)


def _load_cached_matches(spark: SparkSession, args: Namespace, request_hashes: List[str]) -> Dict[str, Dict]:
    """
    Load cached match results from audit table.
    Returns latest cached match rows keyed by request_hash (within expiry window).
    """
    if not request_hashes:
        return {}

    if not args.reltio_match_audit_tbl:
        logging.info("No cache table configured, skipping cache lookup")
        return {}

    try:
        # Read cache table
        cached_df = spark.read.table(args.reltio_match_audit_tbl)
        # Filter by request hashes and expiry date
        req_df = spark.createDataFrame([(x,) for x in request_hashes], "request_hash STRING")
        recent_cached_df = cached_df.join(F.broadcast(req_df), ['request_hash'], "inner").where(
            F.col("audit_ts") >= F.date_sub(F.current_date(), args.match_cache_expiry))
        if not recent_cached_df.head(1):
            return {}

        # Get latest record for each request_hash
        w = Window.partitionBy("request_hash").orderBy(F.desc("audit_ts"))
        latest = recent_cached_df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

        return {r.request_hash: r.asDict() for r in latest.collect()}
    except Exception as e:
        logging.info(f"Cache lookup failed: {e}")
        return {}


def _entity_from_row(row: Dict) -> Dict:
    """Build entity from row. Values are already cleaned and normalized by _normalize_input_row."""
    crosswalk_value = row.get("supplier_id", "1_1")

    return {
        "type": "configuration/entityTypes/Organization",
        "attributes": {
            "DoingBusinessAsName": [{"value": row.get("supplier_name", "")}],
            "Name": [{"value": row.get("supplier_name", "")}],
            "MMHID": [{"value": "MMHID"}],
            "Address": [{
                "value": {
                    "AddressLine1": [{"value": row.get("street_address", "")}],
                    "Country": [{"value": row.get("country", "")}],
                    "City": [{"value": row.get("city", "")}],
                    "StateProvince": [{"value": row.get("state", "")}],
                    "PostalCode": [{"value": {"PostalCode": [{"value": row.get("postal_code", "")}]}}]
                },
                "refEntity": {
                    "crosswalks": [{
                        "type": "configuration/sources/MMH",
                        "value": crosswalk_value
                    }]
                }
            }]
        },
        "crosswalks": [{
            "type": "configuration/sources/MMH",
            "value": crosswalk_value
        }]
    }


def _extract_attr(attrs: dict, path: list[str]) -> str:
    cur = attrs
    for key in path:
        if isinstance(cur, list):
            cur = cur[0] if cur else {}
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key, {})
    if isinstance(cur, list):
        cur = cur[0] if cur else {}
    if isinstance(cur, dict) and "value" in cur:
        cur = cur["value"]
    return cur if isinstance(cur, str) else ""


def _extract_sic8_code(attrs: dict) -> str:
    """Extract SIC8 code from SIC attribute array."""
    sic_list = attrs.get("SIC", [])
    if not isinstance(sic_list, list):
        return ""

    # Filter for SIC8 type entries
    for sic_entry in sic_list:
        if not isinstance(sic_entry, dict):
            continue

        value = sic_entry.get("value", {})
        if not isinstance(value, dict):
            continue

        # Check if Type contains 'SIC8'
        type_list = value.get("Type", [])
        if isinstance(type_list, list) and type_list:
            type_value = type_list[0].get("value", "") if isinstance(type_list[0], dict) else ""
            if type_value == "SIC8":
                # Extract lookupCode from Code array
                code_list = value.get("Code", [])
                if isinstance(code_list, list) and code_list:
                    return code_list[0].get("lookupCode", "") if isinstance(code_list[0], dict) else ""

    return ""


def _get_best_rule(relevance: dict) -> tuple[float, str]:
    best_score, best_rule = 0.0, ""
    if isinstance(relevance, dict):
        for rule, rule_dict in relevance.items():
            if isinstance(rule_dict, dict):
                first_uri = next(iter(rule_dict), None)
                if first_uri:
                    score = float(rule_dict.get(first_uri, 0.0))
                    if score > best_score:
                        best_score, best_rule = score, rule
    return best_score, best_rule


def _extract_entity_attributes(attrs: dict) -> dict:
    """Extract common entity attributes from Reltio response."""
    return {
        "matched_mmhid": _extract_attr(attrs, ["MMHID"]),
        "matched_dba_name": _extract_attr(attrs, ["DoingBusinessAsName"]),
        "matched_mmh_name": _extract_attr(attrs, ["Name"]),
        "matched_street_address": _extract_attr(attrs, ["Address", "value", "AddressLine1"]),
        "matched_city": _extract_attr(attrs, ["Address", "value", "City"]),
        "matched_state": normalize_state(_extract_attr(attrs, ["Address", "value", "StateProvince"]),
                                         _extract_attr(attrs, ["Address", "value", "Country"])),
        "matched_postal_code": _extract_attr(attrs, ["Address", "value", "PostalCode", "value", "PostalCode"]),
        "matched_country": _extract_attr(attrs, ["Address", "value", "Country"]),
        "matched_industry": _extract_attr(attrs, ["Industry", "value", "Code", "value"]),
        "matched_url": _extract_attr(attrs, ["URL", "value", "URLValue", "value"]),
        "matched_mmc_code": _extract_attr(attrs, ["MCC_Code", "value", "Code", "lookupCode"]),
        "matched_sic": _extract_sic8_code(attrs),
        "matched_naics": _extract_attr(attrs, ["NAICS", "value", "Code", "lookupCode"]),
        "matched_clearing_last_seen_date": _extract_attr(attrs, ["CLEARING_LAST_SEEN_DATE", "value"]),
        "matched_auth_last_seen_date": _extract_attr(attrs, ["AUTH_LAST_SEEN_DATE", "value"]),
        "matched_zi_c_last_updated_date": _extract_attr(attrs, ["ZI_C_LAST_UPDATED_DATE", "value"]),
        "matched_agg_merch_id": _extract_attr(attrs, ["AggrMerchID"]),
        "matched_agg_merch_name": _extract_attr(attrs, ["AggrMerchName"]),
        "matched_parent_agg_merch_id": _extract_attr(attrs, ["ParentAggrMerchID"]),
        "matched_parent_agg_merch_name": _extract_attr(attrs, ["ParentAggregateMerchantName"]),
        "matched_region_code": _extract_attr(attrs, ["Address", "value", "FinanceRegioncode", "lookupRawValue"]),
        "matched_region_name": _extract_attr(attrs, ["Address", "value", "RegionName", "value"]),
        "matched_country_cd": (_extract_attr(attrs, ["Address", "value", "ISO3166-3"]) or _extract_attr(attrs, ["Address", "value", "Country", "lookupCode"]))
    }


def _parse_match_resp(resp_obj: dict, src_row: dict) -> dict:
    resp_obj = resp_obj if isinstance(resp_obj, dict) else {}
    best_score, best_rule = _get_best_rule(resp_obj.get("relevance", {}))
    obj_section = resp_obj.get("object", {})
    candidates = obj_section.get(best_rule, []) if best_rule else []
    entity = candidates[0] if candidates and isinstance(candidates[0], dict) else {}
    attrs = entity.get("attributes", {}) if isinstance(entity, dict) else {}

    entity_id = (entity.get("uri", "") or "").split("/")[-1] if entity.get("uri") else ""
    successful = "Y" if entity_id else "N"

    return {
        "supplier_id": src_row.get("supplier_id", ""),
        "entity_id": entity_id,
        **_extract_entity_attributes(attrs),
        "matched_score": best_score,
        "matched_rule": best_rule,
        "successful": successful,
        "match_response_time": 0.0,  # Will be updated by caller if needed
        "match_request": "",
        "match_response": "",
        "request_hash": src_row.get("request_hash", "")
    }


def _build_api_headers(token: str) -> Dict[str, str]:
    """Build common headers for Match and Search API calls."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "accept": "application/json"
    }


def _call_match_api(args: Namespace, token_manager: ReltioTokenManager, batch_rows: List[Dict]) -> List[Dict]:
    """Call Reltio Match API for a batch."""
    with requests.Session() as session:
        payload = [_entity_from_row(r) for r in batch_rows]
        headers = _build_api_headers(token_manager.get_token())
        params = {
            "options": "ovOnly", "cleanse": "true", "activeness": "active", "max": "1",
            "select": SELECT_ATTRIBUTES
        }

        for attempt in range(1, args.match_max_retries + 1):
            try:
                resp = session.post(args.reltio_match_url, headers=headers, params=params,
                                    data=json.dumps(payload, separators=(",", ":")),
                                    timeout=(args.match_connect_timeout, args.match_read_timeout))

                if resp.status_code == 401:
                    headers["Authorization"] = f"Bearer {token_manager.force_refresh()}"
                    resp = session.post(args.reltio_match_url, headers=headers, params=params,
                                        data=json.dumps(payload, separators=(",", ":")),
                                        timeout=(args.match_connect_timeout, args.match_read_timeout))

                resp.raise_for_status()
                js = resp.json() if resp.content else []
                return [_parse_match_resp(resp_obj, src_row) for src_row, resp_obj in zip(batch_rows, js)]

            except Exception as e:
                if attempt < args.match_max_retries:
                    time.sleep(args.match_backoff_secs * (2 ** (attempt - 1)))
                    continue
                raise RuntimeError(f"Match API failed after {args.match_max_retries} attempts: {e}") from e


def _search_entities(args: Namespace, token_manager: ReltioTokenManager,
                     headers: Dict, filter_str: str, session: requests.Session = None) -> List[Dict]:
    """Call Reltio Search API with a filter. Optionally use a session for connection reuse."""

    params = {
        "filter": filter_str,
        "select": SELECT_ATTRIBUTES,
        "sort": "attributes.Address.AddressLine1",
        "order": "desc", "options": "ovOnly", "cleanse": "true", "activeness": "active", "max": 10
    }

    # Use provided session or create a new request
    requester = session if session else requests

    try:
        r = requester.get(args.reltio_search_url, headers=headers, params=params, timeout=(args.match_connect_timeout, args.match_read_timeout))
        if r.status_code == 401:
            headers["Authorization"] = f"Bearer {token_manager.force_refresh()}"
            r = requester.get(args.reltio_search_url, headers=headers, params=params, timeout=(args.match_connect_timeout, args.match_read_timeout))
        r.raise_for_status()
        data = r.json()
        return data.get("entities", []) if isinstance(data, dict) else data
    except Exception as e:
        logging.error(f"Search API error: {e}")
        return []


def _parse_search_entity(entity: Dict) -> Dict:
    """Parse a single entity from search response using same extraction logic as match response."""
    attrs = entity.get("attributes", {}) or {}
    entity_id = entity.get("uri", "").split("/")[-1] if entity.get("uri") else ""

    return {"entity_id": entity_id, **_extract_entity_attributes(attrs)}


def _score_match(input_row: Dict, entity_data: Dict, filter_config) -> float:
    (name_strong_threshold, name_partial_threshold, addr_strong_threshold) = (0.90, 0.80, 0.80)

    # Extract and normalize input fields
    # Uses norm_ prefixed fields populated by _normalize_input_row
    input_name = input_row.get("norm_supplier_name", "").lower()
    input_street = input_row.get("norm_street_address", "").lower()
    input_city = input_row.get("norm_city", "").lower()
    input_zip = input_row.get("norm_postal_code", "").lower()
    country = input_row.get("norm_country", "")

    # Extract and normalize entity fields
    entity_name = entity_data.get("matched_mmh_name", "").lower()
    entity_dba = entity_data.get("matched_dba_name", "").lower()
    entity_address = entity_data.get("matched_street_address", "").lower()
    entity_city = entity_data.get("matched_city", "").lower()
    entity_zip = entity_data.get("matched_postal_code", "").lower()

    # STEP 1: Compute similarity signals
    name_sim = compute_name_similarity(input_name, entity_name, entity_dba)
    addr_sim = compute_address_similarity(input_street, entity_address)

    # STEP 2: Check geo agreements
    city_match = check_city_match(input_city, entity_city)
    postal_match = check_postal_match(input_zip, entity_zip, country)
    state_match = (input_row.get("full_form_state", "").lower() == entity_data.get("matched_state", "").lower())
    geo_match = postal_match or city_match

    # STEP 3: Determine boolean flags
    name_strong = name_sim >= name_strong_threshold
    name_partial = name_sim >= name_partial_threshold
    addr_strong = addr_sim >= addr_strong_threshold

    # STEP 4: Rule-based scoring
    if name_strong and addr_strong and geo_match:
        name_boost = calculate_boost(name_sim, name_strong_threshold, 0.05)
        addr_boost = calculate_boost(addr_sim, addr_strong_threshold, 0.03)
        return min(0.85 + name_boost + addr_boost, 0.99)

    if name_strong and geo_match:
        name_boost = calculate_boost(name_sim, name_strong_threshold, 0.05)
        return min(0.80 + name_boost, 0.90)

    if name_partial and addr_strong and geo_match:
        name_boost = calculate_boost(name_sim, name_partial_threshold, 0.10)
        return min(0.80 + name_boost, 0.90)

    if name_partial and geo_match:
        name_boost = calculate_boost(name_sim, name_partial_threshold, 0.15)
        return min(0.75 + name_boost, 0.88)

    if name_strong and state_match:
        name_boost = calculate_boost(name_sim, name_strong_threshold, 0.10)
        return min(0.80 + name_boost, 0.85)

    # Strong name similarity alone (>=0.90) – e.g. chain stores at different locations,
    # or same entity found by a broad rule without geo data.  Return HIGH confidence.
    if name_strong:
        name_boost = calculate_boost(name_sim, name_strong_threshold, 0.05)
        return min(0.80 + name_boost, 0.88)

    if filter_config.get("name") in BROAD_RULES and name_sim >= 0.95:
        return 0.80

    if name_partial:
        name_boost = calculate_boost(name_sim, name_partial_threshold, 0.15)
        return min(0.65 + name_boost, 0.78)

    if filter_config.get("name") in BROAD_RULES:
        return name_sim

    return 0.0


def _build_search_filter(row: Dict, filter_config) -> str:
    """
    Build search filter using template from filter configuration.
    Values are already cleaned and normalized.

    Args:
        row: Input row with supplier data
        filter_config: Either a filter name string (e.g., "Exact_Match") or a dict with "name" and "template"

    Returns:
        Filter string for Reltio search API
    """
    # Use norm_ prefixed fields to ensure correct casing e.g. 'Romania' not 'ROMANIA'
    name = escape_reltio_value(row.get("norm_supplier_name", ""))
    city = escape_reltio_value(row.get("norm_city", ""))
    state = escape_reltio_value(row.get("norm_state", ""))
    country = escape_reltio_value(row.get("norm_country", ""))
    postal_code = escape_reltio_value(row.get("norm_postal_code", ""))
    street_address = escape_reltio_value(row.get("norm_street_address", ""))
    country_iso2 = escape_reltio_value(row.get("country_iso2", ""))

    # Handle both string filter name and dict filter config
    if isinstance(filter_config, str):
        filter_obj = next((f for f in SEARCH_FILTERS if f["name"] == filter_config), None)
        if not filter_obj:
            raise ValueError(f"Unknown filter type: {filter_config}")
        template = filter_obj["template"]
    elif isinstance(filter_config, dict):
        template = filter_config["template"]
    else:
        raise ValueError(f"filter_config must be a string or dict, got {type(filter_config)}")

    # Format template with actual values
    filter_clause = template.format(name=name, city=city, state=state, country=country,
                                    postal_code=postal_code, street_address=street_address, country_iso2=country_iso2)

    return f"(equals(type,'configuration/entityTypes/Organization') and {filter_clause})"


def _get_filter_name(filter_config) -> str:
    """Extract filter name from filter configuration."""
    if isinstance(filter_config, str):
        return filter_config
    elif isinstance(filter_config, dict):
        return filter_config.get("name", "Custom_Match")
    else:
        return "Unknown_Match"


def _score_and_select_best_match(entities: List[Dict], row: Dict, filter_config) -> Tuple[Optional[float], Optional[Dict]]:
    """
    Score all entities and return the best match with its score.
    Uses rule-based scoring for all matches.

    Args:
        entities: List of entity dictionaries from search results
        row: Input row data

    Returns:
        Tuple of (best_score, best_match) or (None, None) if no valid matches
    """
    scored_matches = []

    for entity in entities:
        parsed = _parse_search_entity(entity)
        match_score = _score_match(row, parsed, filter_config)

        # Only consider matches with positive scores
        if match_score > 0:
            scored_matches.append((match_score, parsed))

    if scored_matches:
        scored_matches.sort(reverse=True, key=lambda x: x[0])
        return scored_matches[0]  # Returns (best_score, best_match)

    return None, None


def _try_search_with_name(args: Namespace, token_manager: ReltioTokenManager, headers: Dict, row: Dict, filter_config,
                          score_threshold: float, rule_suffix: str = "", session: requests.Session = None) -> Optional[Dict]:
    """
    Helper function to search and score a single name variant.

    Args:
        session: Optional requests.Session for HTTP connection reuse

    Returns:
        Match dict with metadata or None if no valid match found
    """
    filter_str = _build_search_filter(row, filter_config)
    entities = _search_entities(args, token_manager, headers, filter_str, session)

    if not entities:
        return None

    best_score, best_match = _score_and_select_best_match(entities, row, filter_config)

    if not best_match or best_score < score_threshold:
        return None

    best_match.update({
        "supplier_id": row["supplier_id"],
        "matched_score": best_score,
        "matched_rule": _get_filter_name(filter_config) + rule_suffix,
        "successful": "Y",
        "match_response_time": 0.0,
        "match_request": "",
        "match_response": "",
        "request_hash": row.get("request_hash", "")
    })
    return best_match


def _try_fallback_names(args: Namespace, token_manager: ReltioTokenManager, headers: Dict, row: Dict, filter_config,
                        score_threshold: float, session: requests.Session) -> Optional[Dict]:
    """
    Try fallback name variations (DBA parts and cleansed name).

    Returns:
        Match dict if found, None otherwise
    """
    supplier_name = row.get("supplier_name", "")

    # Define all fallback variations to try
    fallback_variations = []

    # Try DBA parts if name contains 'dba'
    if "dba" in supplier_name.lower():
        parts = re.split(r'(?i)dba', supplier_name)

        # Add first part (before DBA)
        if parts[0].strip():
            fallback_variations.append((parts[0].strip(), "_DBA_Part1"))

        # Add second part (after DBA)
        if len(parts) > 1 and parts[1].strip():
            fallback_variations.append((parts[1].strip(), "_DBA_Part2"))

    # Add cleansed supplier name
    if row.get("cleansed_supplier_name"):
        fallback_variations.append((row["cleansed_supplier_name"], "_CleansedName"))

    # Try each variation
    for name_variant, suffix in fallback_variations:
        modified_row = row.copy()
        modified_row["norm_supplier_name"] = name_variant
        result = _try_search_with_name(args, token_manager, headers, modified_row, filter_config, score_threshold, suffix, session)
        if result:
            return result

    return None


def _call_search_api(args: Namespace, token_manager: ReltioTokenManager,
                     row: Dict, filter_config, session: requests.Session = None) -> Optional[Dict]:
    """
    Search API for a single record with a specific filter configuration.
    Uses rule-based scoring uniformly for all filters.

    For names containing 'DBA', implements progressive fallback:
    1. Try full name first
    2. If no match, try part before DBA (split[0])
    3. If still no match, try part after DBA (split[1])
    4. If still no match, try cleansed name

    Args:
        filter_config: Either a filter name string or a dict with "name", "template", and "score_threshold"
        session: Optional requests.Session for HTTP connection reuse

    Returns:
        Dict with match results or None if no match found or score below threshold
    """
    headers = _build_api_headers(token_manager.get_token())
    score_threshold = filter_config.get("score_threshold", 0.0) if isinstance(filter_config, dict) else 0.0

    local_session = session or requests.Session()

    try:
        # Attempt 1: Try with full name
        result = _try_search_with_name(args, token_manager, headers, row, filter_config, score_threshold, session=local_session)
        if result:
            return result

        # Attempt 2-4: Try fallback name variations
        result = _try_fallback_names(args, token_manager, headers, row, filter_config, score_threshold, local_session)
        if result:
            return result

    finally:
        if session is None:
            local_session.close()


def _build_resp_schema() -> StructType:
    """Build schema matching production reltio_match_audit_tbl."""
    return StructType([
        StructField("supplier_id", StringType()),
        StructField("entity_id", StringType()),
        StructField("matched_mmhid", StringType()),
        StructField("matched_dba_name", StringType()),
        StructField("matched_mmh_name", StringType()),
        StructField("matched_street_address", StringType()),
        StructField("matched_city", StringType()),
        StructField("matched_state", StringType()),
        StructField("matched_postal_code", StringType()),
        StructField("matched_country", StringType()),
        StructField("matched_industry", StringType()),
        StructField("matched_url", StringType()),
        StructField("matched_region_name", StringType()),
        StructField("matched_mmc_code", StringType()),
        StructField("matched_sic", StringType()),
        StructField("matched_agg_merch_id", StringType()),
        StructField("matched_agg_merch_name", StringType()),
        StructField("matched_parent_agg_merch_id", StringType()),
        StructField("matched_parent_agg_merch_name", StringType()),
        StructField("matched_clearing_last_seen_date", StringType()),
        StructField("matched_auth_last_seen_date", StringType()),
        StructField("matched_zi_c_last_updated_date", StringType()),
        StructField("matched_naics", StringType()),
        StructField("matched_country_cd", StringType()),
        StructField("matched_region_code", StringType()),
        StructField("matched_score", DoubleType()),
        StructField("matched_rule", StringType()),
        StructField("successful", StringType()),  # "Y" or "N"
        StructField("match_response_time", DoubleType()),
        StructField("match_request", StringType()),
        StructField("match_response", StringType()),
        StructField("request_hash", StringType())
    ])


def _create_no_match_result(supplier_id: str, request_hash: str = "") -> Dict:
    """Create a no-match result for a supplier."""
    return {
        "supplier_id": supplier_id,
        "entity_id": "",
        "matched_mmhid": "",
        "matched_dba_name": "",
        "matched_mmh_name": "",
        "matched_street_address": "",
        "matched_city": "",
        "matched_state": "",
        "matched_postal_code": "",
        "matched_country": "",
        "matched_industry": "",
        "matched_url": "",
        "matched_region_name": "",
        "matched_mmc_code": "",
        "matched_sic": "",
        "matched_agg_merch_id": "",
        "matched_agg_merch_name": "",
        "matched_parent_agg_merch_id": "",
        "matched_parent_agg_merch_name": "",
        "matched_clearing_last_seen_date": "",
        "matched_auth_last_seen_date": "",
        "matched_zi_c_last_updated_date": "",
        "matched_naics": "",
        "matched_country_cd": "",
        "matched_region_code": "",
        "matched_score": 0.0,
        "matched_rule": "no_match",
        "successful": "N",
        "match_response_time": 0.0,
        "match_request": "",
        "match_response": "",
        "request_hash": request_hash
    }


def _process_search_misses(args: Namespace, token_manager: ReltioTokenManager, misses: List[Dict], normalized_rows: List[Dict]) -> List[Dict]:
    """
    Process misses from Match API using progressive Search API filtering.
    Reuses a single ThreadPoolExecutor and HTTP session for efficiency.
    """
    miss_input_map = {r["supplier_id"]: r for r in normalized_rows}
    remaining_rows = [miss_input_map[m["supplier_id"]] for m in misses]

    filter_names = [f"{f['name']} (threshold={f.get('score_threshold', 0.0)})" for f in SEARCH_FILTERS]
    logging.info(f"Starting progressive search for {len(remaining_rows)} misses with filters: {filter_names}")

    all_search_results = {}

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=args.search_max_workers + 10, max_retries=0)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Reuse single ThreadPoolExecutor and requests.Session for all filters
    with ThreadPoolExecutor(max_workers=args.search_max_workers) as executor, session:
        # Use SEARCH_FILTERS for progressive filtering
        for filter_config in SEARCH_FILTERS:
            if not remaining_rows:
                break

            threshold = filter_config.get("score_threshold", 0.0)
            logging.info(f"Trying {filter_config['name']} (threshold={threshold}) for {len(remaining_rows)} records...")

            futures = {executor.submit(_call_search_api, args, token_manager, row, filter_config, session): row for row in remaining_rows}

            for fut in as_completed(futures):
                row = futures[fut]
                try:
                    result = fut.result()
                    if result:
                        all_search_results[row["supplier_id"]] = result
                except Exception as e:
                    logging.error(f"Search API error for supplier {row['supplier_id']}: {e}")

            matched_ids = set(all_search_results.keys())
            remaining_rows = [r for r in remaining_rows if r["supplier_id"] not in matched_ids]
            logging.info(f"  -> Matched {len(all_search_results)} total (meeting threshold), {len(remaining_rows)} remaining")

    search_results = []
    for miss in misses:
        supplier_id = miss["supplier_id"]
        request_hash = miss.get("request_hash", "")
        if supplier_id in all_search_results:
            search_results.append(all_search_results[supplier_id])
        else:
            search_results.append(_create_no_match_result(supplier_id, request_hash))

    logging.info(f"Search API found {len(all_search_results)} matches (meeting thresholds) out of {len(misses)} misses")
    return search_results


def _validate_input_columns(input_df: DataFrame) -> None:
    """Validate that input DataFrame has all required columns."""
    missing = [c for c in REQUIRED_COLS if c not in input_df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def _normalize_and_hash_inputs(input_df: DataFrame, country_lookup: CountryLookup) -> List[Dict]:
    """
    Normalize input rows and compute request hashes for caching.

    Args:
        input_df:       Spark DataFrame with raw supplier records.
        country_lookup: Pre-built country lookup dict from _load_country_lookup.

    Returns:
        List of normalized rows with request_hash field added
    """
    rows = input_df.select(*REQUIRED_COLS).toPandas().to_dict("records")
    logging.info(f"Normalizing {len(rows)} input records...")

    normalized_rows = [normalize_input_row(row, country_lookup) for row in rows]

    # Add request_hash to each normalized row
    for row in normalized_rows:
        row["request_hash"] = build_request_hash(row)

    distinct_hashes = len({r["request_hash"] for r in normalized_rows})
    logging.info(f"Distinct request hashes: {distinct_hashes}")

    return normalized_rows


def _separate_cache_hits_and_misses(normalized_rows: List[Dict], cached_map: Dict[str, Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Separate normalized rows into cache hits and cache misses.

    Args:
        normalized_rows: List of normalized input rows with request_hash
        cached_map: Dictionary mapping request_hash to cached results

    Returns:
        Tuple of (cache_hits, cache_misses)
    """
    cache_hits, cache_misses = [], []

    for row in normalized_rows:
        hit = cached_map.get(row["request_hash"])
        if hit:
            # Use cached result
            cache_hits.append({
                "supplier_id": row["supplier_id"],
                "entity_id": hit.get("entity_id", ""),
                "matched_mmhid": hit.get("matched_mmhid", ""),
                "matched_dba_name": hit.get("matched_dba_name", ""),
                "matched_mmh_name": hit.get("matched_mmh_name", ""),
                "matched_street_address": hit.get("matched_street_address", ""),
                "matched_city": hit.get("matched_city", ""),
                "matched_state": hit.get("matched_state", ""),
                "matched_postal_code": hit.get("matched_postal_code", ""),
                "matched_country": hit.get("matched_country", ""),
                "matched_industry": hit.get("matched_industry", ""),
                "matched_url": hit.get("matched_url", ""),
                "matched_region_name": hit.get("matched_region_name", ""),
                "matched_mmc_code": hit.get("matched_mmc_code", ""),
                "matched_sic": hit.get("matched_sic", ""),
                "matched_agg_merch_id": hit.get("matched_agg_merch_id", ""),
                "matched_agg_merch_name": hit.get("matched_agg_merch_name", ""),
                "matched_parent_agg_merch_id": hit.get("matched_parent_agg_merch_id", ""),
                "matched_parent_agg_merch_name": hit.get("matched_parent_agg_merch_name", ""),
                "matched_clearing_last_seen_date": hit.get("matched_clearing_last_seen_date", ""),
                "matched_auth_last_seen_date": hit.get("matched_auth_last_seen_date", ""),
                "matched_zi_c_last_updated_date": hit.get("matched_zi_c_last_updated_date", ""),
                "matched_naics": hit.get("matched_naics", ""),
                "matched_country_cd": hit.get("matched_country_cd", ""),
                "matched_region_code": hit.get("matched_region_code", ""),
                "matched_score": hit.get("matched_score", 0.0),
                "matched_rule": hit.get("matched_rule", ""),
                "successful": hit.get("successful", "N"),
                "match_response_time": hit.get("match_response_time", 0.0),
                "match_request": hit.get("match_request", ""),
                "match_response": hit.get("match_response", ""),
                "request_hash": row["request_hash"]
            })
        else:
            cache_misses.append(row)

    logging.info(f"Cache hits: {len(cache_hits)}, API calls needed: {len(cache_misses)}")
    return cache_hits, cache_misses


def _call_match_api_for_batch(args: Namespace, token_manager: ReltioTokenManager, api_misses: List[Dict]) -> List[Dict]:
    """
    Call Match API for cache misses in batches with concurrent execution.

    Returns:
        List of match results from API
    """
    batches = [api_misses[i:i + args.match_batch_size] for i in range(0, len(api_misses), args.match_batch_size)]
    match_results = []

    with ThreadPoolExecutor(max_workers=args.match_max_concurrent_batches) as ex:
        futures = [ex.submit(_call_match_api, args, token_manager, batch) for batch in batches]
        for fut in as_completed(futures):
            match_results.extend(fut.result())

    logging.info(f"Match API returned {len(match_results)} results")
    return match_results


def _separate_match_hits_and_misses(match_results: List[Dict], args: Namespace) -> Tuple[List[Dict], List[Dict]]:
    """
    Separate Match API results into hits (good matches) and misses (no match or low score).

    Args:
        match_results: Results from Match API
        args: Contains min_match_score threshold

    Returns:
        Tuple of (match_hits, match_misses)
    """
    match_misses = [r for r in match_results if not r.get("entity_id") or r.get("matched_score", 0) < args.min_match_score]
    match_hits = [r for r in match_results if r.get("entity_id") and r.get("matched_score", 0) >= args.min_match_score]

    logging.info(f"Match API hits: {len(match_hits)}, misses: {len(match_misses)}")
    return match_hits, match_misses


def _process_api_misses(args: Namespace, token_manager: ReltioTokenManager, api_misses: List[Dict]) -> List[Dict]:
    """
    Process records that were not in cache: call Match API, then Search API for remaining misses.

    Returns:
        List of all results (Match API hits + Search API results)
    """
    if not api_misses:
        return []

    # Call Match API in batches
    match_results = _call_match_api_for_batch(args, token_manager, api_misses)

    # Separate hits and misses
    match_hits, match_misses = _separate_match_hits_and_misses(match_results, args)

    # Call Search API for Match API misses
    if match_misses:
        search_results = _process_search_misses(args, token_manager, match_misses, api_misses)
        return match_hits + search_results
    else:
        return match_hits


def _save_results_to_cache(spark: SparkSession, args: Namespace, new_results: List[Dict]) -> None:
    """
    Save new matching results to cache table for future reuse.
    Only caches records where a match was found (entity_id is not empty).

    Args:
        spark: SparkSession
        args: Contains hybrid_match_audit_tbl configuration
        new_results: New results to cache
    """
    if not new_results:
        return

    if not args.reltio_match_audit_tbl:
        logging.info("No cache table configured, skipping cache save")
        return

    # Filter out no-match results - only cache successful matches
    matched_results = [r for r in new_results if r.get("entity_id", "").strip() != ""]

    if not matched_results:
        logging.info(f"No matched results to cache (0 matches found out of {len(new_results)} records)")
        return

    try:
        new_df = spark.createDataFrame(matched_results, schema=_build_resp_schema())
        new_df = new_df.withColumn("audit_ts", F.current_timestamp())

        # Check if table exists, use insertInto for append
        if spark.catalog.tableExists(args.reltio_match_audit_tbl):
            audit_cols = spark.table(args.reltio_match_audit_tbl).columns
            new_df = new_df.select(audit_cols)
            new_df.write.mode("append").insertInto(args.reltio_match_audit_tbl)
            logging.info(f"Appended {len(matched_results)} matched results to cache table: {args.reltio_match_audit_tbl}")
        else:
            new_df.write.mode("overwrite").saveAsTable(args.reltio_match_audit_tbl)
            logging.info(f"Created cache table and saved {len(matched_results)} matched results: {args.reltio_match_audit_tbl}")
    except Exception as e:
        logging.error(f"Failed to save to cache table: {e}")


def _build_final_result_dataframe(spark: SparkSession, all_results: List[Dict]) -> DataFrame:
    result_df = spark.createDataFrame(all_results, schema=_build_resp_schema())
    result_df = result_df.withColumn("is_matched", F.when(F.col("entity_id") != "", F.lit("Y")).otherwise(F.lit("N")))
    result_cols = ["supplier_id", "entity_id", "is_matched", "matched_score", "matched_rule", "matched_mmhid",
                   "matched_dba_name",
                   "matched_mmh_name", "matched_street_address", "matched_city", "matched_state", "matched_postal_code",
                   "matched_country", "matched_industry", "matched_url",
                   "matched_region_name",
                   "matched_mmc_code", "matched_sic",
                   "matched_agg_merch_id", "matched_agg_merch_name", "matched_parent_agg_merch_id",
                   "matched_parent_agg_merch_name", "matched_clearing_last_seen_date",
                   "matched_auth_last_seen_date", "matched_zi_c_last_updated_date", "matched_naics",
                   "matched_country_cd", "matched_region_code"]
    return result_df.select(*result_cols)


def perform_matching(spark: SparkSession, input_df: DataFrame, args: Namespace) -> DataFrame:
    """
    Main hybrid matching with caching:
    1. Normalize input and compute request hashes
    2. Check cache for existing matches
    3. For cache misses: call Match API first, then Search API for remaining misses
    4. Save new results to cache table
    5. Return combined results
    """
    # Step 1: Validate input
    _validate_input_columns(input_df)

    # Step 2: Get authentication token
    token_manager = ReltioTokenManager(args)
    token_manager.get_token()

    # Step 3: Load country lookup table once (args.country_map_table may be None to use static map only)
    country_lookup = load_country_lookup(spark, cnty_map_tbl_name=args.country_map_table, cnty_alias_map_tbl_name=args.country_alias_map_table)

    # Step 4: Normalize input and compute request hashes
    normalized_rows = _normalize_and_hash_inputs(input_df, country_lookup)

    # Step 5: Check cache for existing matches
    distinct_hashes = list({r["request_hash"] for r in normalized_rows})
    cached_map = _load_cached_matches(spark, args, distinct_hashes)
    cache_hits, api_misses = _separate_cache_hits_and_misses(normalized_rows, cached_map)

    # Step 6: Process cache misses through Match API and Search API
    new_results = _process_api_misses(args, token_manager, api_misses)

    # Step 7: Save new results to cache
    _save_results_to_cache(spark, args, new_results)

    # Step 8: Combine all results and return
    all_results = cache_hits + new_results
    return _build_final_result_dataframe(spark, all_results)


if __name__ == "__main__":  # pragma: no cover
    # NOTE: SET the environment variable RELTIO_ACCESS_TOKEN with access token before running...
    spark = SparkSession.builder.appName("HybridMatcher").getOrCreate()

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    input_dir = os.path.join(project_root, "test_data", "input")

    # country_map: country_name → iso2  (source of truth)
    country_map_csv = os.path.join(input_dir, "country_map.csv")
    country_map_view = "country_map"
    spark.read.option("header", "true").option("inferSchema", "false").csv(country_map_csv).createOrReplaceTempView(country_map_view)
    logging.info(f"Registered country_map CSV as Spark temp view '{country_map_view}' from {country_map_csv}")

    # country_alias_map: alias → country_name  (typo / variant resolver)
    country_alias_map_csv = os.path.join(input_dir, "country_alias_map.csv")
    country_alias_map_view = "country_alias_map"
    spark.read.option("header", "true").option("inferSchema", "false").csv(country_alias_map_csv).createOrReplaceTempView(country_alias_map_view)
    logging.info(f"Registered country_alias_map CSV as Spark temp view '{country_alias_map_view}' from {country_alias_map_csv}")

    args = Namespace(
        reltio_match_url="https://test.reltio.com/reltio/api/xR9TjhHENjZH8HZ/entities/_matches",
        reltio_search_url="https://test.reltio.com/reltio/api/xR9TjhHENjZH8HZ/entities",
        reltio_auth_url="https://auth.reltio.com/oauth/token",
        reltio_secret_name="secret/reltio/api_credentials",
        match_batch_size=100,
        match_max_concurrent_batches=20,
        match_max_retries=2,
        match_backoff_secs=2,
        match_read_timeout=30,
        match_connect_timeout=10,
        search_max_workers=30,
        min_match_score=0.80,
        reltio_match_audit_tbl=None,       # Set to None to disable caching
        match_cache_expiry=30,             # Days
        country_map_table=country_map_view,
        country_alias_map_table=country_alias_map_view,
    )

    cols = ["supplier_id", "supplier_name", "street_address", "city", "state", "postal_code", "phone_number", "country"]
    data = [
        # ("01", "MARIAM MARTIROSYAN", "TIGRAN METS 53/6", "YEREVAN", "ARM", "1", "", "ARMENIA"),
        # ("02", "ATM ASHIB HEAD (R)", "G. LUSAVORICH 13", "YEREVAN", "ARM", "374", "", "ARMENIA"),
        # ("03", "UCOM", "17/1, 2-ND STR., AMASIA", "AMASIA", "ARM", "3750", "", "ARMENIA"),
        ("06", "HANCED", "104 Meade Street 6530", "GEORGE", "ZA", "6530", "", "SOUTH AFRICA"),
    ]

    input_df = spark.createDataFrame(data, cols)
    input_df.show(truncate=False)
    step_start = time.time()
    result_df = perform_matching(spark, input_df, args)
    result_df.show(truncate=False)
    logging.info(f"Hybrid matching completed in {(time.time()) - step_start:.2f} seconds)")
