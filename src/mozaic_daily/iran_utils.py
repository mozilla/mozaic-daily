# -*- coding: utf-8 -*-
"""Utility functions for Iran synthetic data generation.

Provides reverse-mapping from mozaic population strings back to the boolean
segment columns used in BigQuery query output.  The mozaic package encodes
boolean segment columns into a single ``population`` string during
``populate_tiles()`` (see ``mozaic/utils.py``).  This module reverses that
encoding so synthetic forecast data can be stored in the same format as raw
BigQuery results.

Used by:
- ``scripts/generate_iran_synthetic.py`` (Branch 1: iran-synthetic-generation)
- ``tests/test_iran_utils.py``
"""

from typing import Dict, List

DESKTOP_SEGMENT_COLUMNS = ["modern_windows", "winX"]
MOBILE_SEGMENT_COLUMNS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]


def population_to_segment_bools(
    population: str, segment_columns: List[str]
) -> Dict[str, bool]:
    """Convert a mozaic population string back to boolean segment columns.

    The mozaic package (``mozaic/utils.py:populate_tiles``) encodes segment
    columns into a population string by joining column names where the boolean
    value is True.  If all booleans are False the population is ``"other"``.

    This function reverses that encoding.

    Args:
        population: Population string from mozaic output
            (e.g., ``"modern_windows"``, ``"fenix_android"``, ``"other"``).
        segment_columns: List of boolean column names for this platform
            (e.g., ``DESKTOP_SEGMENT_COLUMNS`` or ``MOBILE_SEGMENT_COLUMNS``).

    Returns:
        Dict mapping each column name to its boolean value.

    Raises:
        ValueError: If *population* cannot be reverse-mapped to the given
            *segment_columns*.

    Examples:
        >>> population_to_segment_bools("modern_windows", DESKTOP_SEGMENT_COLUMNS)
        {'modern_windows': True, 'winX': False}
        >>> population_to_segment_bools("other", MOBILE_SEGMENT_COLUMNS)
        {'fenix_android': False, 'firefox_ios': False, 'focus_android': False, 'focus_ios': False}
    """
    if population == "other":
        return {col: False for col in segment_columns}

    # Single-column match (the common case — each BQ row has exactly one True boolean)
    if population in segment_columns:
        return {col: (col == population) for col in segment_columns}

    # Compound population (multiple True columns joined by "_").
    # Parse by greedily matching the longest column names first.
    active_cols: set = set()
    remaining = population
    for col in sorted(segment_columns, key=len, reverse=True):
        if col in remaining:
            active_cols.add(col)
            remaining = remaining.replace(col, "", 1).strip("_")

    if not remaining and active_cols:
        return {col: (col in active_cols) for col in segment_columns}

    raise ValueError(
        f"Cannot reverse-map population '{population}' "
        f"to segment columns {segment_columns}"
    )
