"""
utils/data_quality.py
─────────────────────
Data Quality Rules enforced at the Silver layer.

Rules implemented:
  1. No null values in key fields
  2. PM2.5 must be >= 0
  3. Temperature range: -10 to 60 °C
  4. Flood risk must be a recognised category
  (Duplicate detection is handled via MongoDB unique indexes)
"""

from typing import List, Tuple

from utils.logger import get_logger

logger = get_logger("data_quality")


# ─────────────────────────────────────────────────────────────────────────────
# Air Quality Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_air_quality(record: dict) -> Tuple[bool, List[str]]:
    """
    Validate a cleaned air quality record.

    Returns:
        (is_valid, list_of_errors)
    """
    errors: List[str] = []

    # Rule 1 — No nulls in key fields
    for field in ("timestamp", "province", "pm25"):
        if record.get(field) is None:
            errors.append(f"Null value in key field: '{field}'")

    # Rule 2 — PM2.5 must be >= 0
    pm25 = record.get("pm25")
    if pm25 is not None and pm25 < 0:
        errors.append(f"PM2.5 out of range: {pm25} (must be >= 0)")

    return len(errors) == 0, errors


# ─────────────────────────────────────────────────────────────────────────────
# Weather Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_weather(record: dict) -> Tuple[bool, List[str]]:
    """
    Validate a cleaned weather record.

    Returns:
        (is_valid, list_of_errors)
    """
    errors: List[str] = []

    # Rule 1 — No nulls in key fields
    for field in ("timestamp", "province", "temperature"):
        if record.get(field) is None:
            errors.append(f"Null value in key field: '{field}'")

    # Rule 3 — Temperature range -10 to 60 °C
    temp = record.get("temperature")
    if temp is not None and not (-10 <= temp <= 60):
        errors.append(f"Temperature out of range: {temp} °C (must be -10 to 60)")

    return len(errors) == 0, errors


# ─────────────────────────────────────────────────────────────────────────────
# Flood Risk Validation
# ─────────────────────────────────────────────────────────────────────────────

VALID_FLOOD_RISKS = {"Low", "Medium", "High", "Very High"}


def validate_flood(record: dict) -> Tuple[bool, List[str]]:
    """
    Validate a cleaned flood risk record.

    Returns:
        (is_valid, list_of_errors)
    """
    errors: List[str] = []

    # Rule 1 — No nulls in key fields
    for field in ("date", "province", "flood_risk"):
        if record.get(field) is None:
            errors.append(f"Null value in key field: '{field}'")

    # Rule 4 — Flood risk category must be valid
    flood_risk = record.get("flood_risk")
    if flood_risk and flood_risk not in VALID_FLOOD_RISKS:
        errors.append(
            f"Invalid flood_risk value: '{flood_risk}'. "
            f"Expected one of: {VALID_FLOOD_RISKS}"
        )

    return len(errors) == 0, errors
