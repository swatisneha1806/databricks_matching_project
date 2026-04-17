"""Pure, stateless text / string utility functions for the Reltio matching pipeline."""
from __future__ import annotations

import re

from rapidfuzz import fuzz


def fix_encoding(v: str) -> str:
    """Fix common latin-1 → utf-8 mojibake sequences found in upstream data."""
    if not v:
        return ""
    try:
        v = v.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    for a, b in [
        ("√â", "É"), ("√Ç", "Â"), ("√Å", "Á"),
        ("√É", "Ã"), ("√ì", "Ó"), ("√ç", "Í"),
        ("Ê", "É"), ("í", "Í"),
        ("√Ñ", "Ä"), ("√ñ", "ä"),
        ("√∂", "ö"), ("√§", "ç"), ("√£", "ã"),
        ("√°", "á"), ("√à", "à"), ("√º", "ú"),
        ("√´", "ô"), ("√ö", "Ö"), ("√ü", "Ü"),
        ("√Ä", "ä"), ("√â‰", "É"),
    ]:
        v = v.replace(a, b)
    return v


def escape_reltio_value(value: str) -> str:
    """Escape special characters for safe use inside a Reltio filter expression."""
    if not value:
        return ""
    value = value.replace("\\", "\\\\")
    value = value.replace("'", "\\'")
    return value


def cleanse_supplier_name(name: str) -> str:
    """Normalize supplier name: split camelCase, remove legal suffixes,
    replace separators with spaces, lowercase."""
    cleaned = re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
    cleaned = cleaned.lower()

    # Remove domain suffixes
    cleaned = re.sub(r'\.(com|org|net|io|co|gov|edu|biz|info|us|uk|ca)$', '', cleaned, flags=re.IGNORECASE, ).strip()

    # Remove common legal-entity suffixes
    suffixes = [
        "inc", "inc.", "corporation", "corp", "corp.", "llc", "l.l.c.",
        "co", "co.", "company", "limited", "ltd", "ltd.",
    ]
    pattern = r'(?:' + '|'.join(re.escape(s) for s in suffixes) + r')$'
    cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()

    # Replace separators (hyphen, underscore, dot, comma) with space
    cleaned = re.sub(r'[-_\.,]', ' ', cleaned)

    # Collapse multiple spaces
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Add space between letters and digits (e.g. "VEN0000002" → "VEN 0000002")
    cleaned = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', cleaned)
    cleaned = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', cleaned)

    return cleaned


def remove_special_chars(text: str) -> str:
    """Remove non-alphanumeric characters, keeping only letters, digits, and spaces."""
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return ' '.join(cleaned.split())


def normalize_city(city: str) -> str:
    """Normalise city name for comparison: lowercase, collapse whitespace."""
    return " ".join(str(city).strip().lower().split())


def extract_zip_code(postal_code: str, country: str = "") -> str:
    """Extract a comparable postal code segment.

    For USA: digits-only, first 5 characters (ZIP5). For all others: strip spaces, dots, and hyphens, uppercase.
    """
    if str(country).strip().upper() == "USA":
        return re.sub(r"\D", "", str(postal_code))[:5]
    return re.sub(r"[\s.\-]", "", str(postal_code).upper())


def compute_name_similarity(input_name: str, entity_name: str, entity_dba: str) -> float:
    """Return max(sim(input, legalName), sim(input, DBAName)) using token_sort_ratio.

    Special characters are stripped before comparison so punctuation differences do not penalise the score.

    Returns:
        Similarity score in [0.0, 1.0]
    """
    input_cleaned = remove_special_chars(input_name)
    entity_cleaned = remove_special_chars(entity_name)
    dba_cleaned = remove_special_chars(entity_dba)

    sim_legal = fuzz.token_sort_ratio(input_cleaned, entity_cleaned) / 100.0
    sim_dba = fuzz.token_sort_ratio(input_cleaned, dba_cleaned) / 100.0
    return max(sim_legal, sim_dba)


def compute_address_similarity(input_address: str, entity_address: str) -> float:
    """Return address similarity using token_set_ratio and partial_ratio.

    Common noise tokens ("po box", "unit") are stripped from both sides before comparison so they don't artificially inflate or deflate the score.

    Returns:
        Similarity score in [0.0, 1.0]
    """
    if not input_address or not entity_address:
        return 0.0

    _REMOVAL_TOKENS = ["po box", "unit"]

    def _norm(text: str) -> str:
        t = text.lower().strip()
        t = re.sub(r'[^a-z0-9\s]', ' ', t)
        return ' '.join(t.split())

    def _strip_common(t1: str, t2: str) -> tuple[str, str]:
        n1, n2 = _norm(t1), _norm(t2)
        for tok in _REMOVAL_TOKENS:
            if tok in n1 and tok in n2:
                n1 = ' '.join(n1.replace(tok, ' ', 1).split())
                n2 = ' '.join(n2.replace(tok, ' ', 1).split())
        return n1, n2

    r1, r2 = _strip_common(input_address, entity_address)
    token_score = fuzz.token_set_ratio(r1, r2) / 100.0
    partial_score = fuzz.partial_ratio(r1, r2) / 100.0
    return max(token_score, partial_score)


def check_city_match(input_city: str, entity_city: str) -> bool:
    """Return True if both city strings normalise to the same value."""
    if not input_city or not entity_city:
        return False
    return normalize_city(input_city) == normalize_city(entity_city)


def check_postal_match(input_zip: str, entity_zip: str, country: str = "") -> bool:
    """Return True if the entity postal code starts with the input postal code. Handles ZIP5 vs ZIP+4 (e.g. "12345" matches "12345-6789")."""
    if not input_zip or not entity_zip:
        return False
    input_z = extract_zip_code(input_zip, country)
    entity_z = extract_zip_code(entity_zip, country)
    if not input_z or not entity_z:
        return False
    return entity_z.startswith(input_z)


def calculate_boost(similarity: float, threshold: float, boost_weight: float) -> float:
    """Calculate a proportional boost score for how far similarity exceeds threshold.

    Args:
        similarity:   Actual similarity score (0.0–1.0)
        threshold:    Minimum value before any boost is applied
        boost_weight: Maximum boost amount returned when similarity == 1.0

    Returns:
        Boost in [0.0, boost_weight]
    """
    if similarity < threshold:
        return 0.0
    return boost_weight * (similarity - threshold) / (1.0 - threshold)
