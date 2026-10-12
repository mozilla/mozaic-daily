# -*- coding: utf-8 -*-
"""Holiday calendar for the Glean-history desktop source: the 2025 Legacy telemetry outage.

The legacy-to-glean export translates Legacy telemetry day by day, so where Legacy itself was
low the Glean-equivalent series is low too. The export project's windows of record are
Windows 2025-08-16..11-06 and Mac 2025-08-28..12-10 (its D-079 / D-066). The packaged
``DesktopBugs`` calendar covers only 2025-10-04..10-31, about a third of the Mac trough
(measured in ``data-official/2027-glean-draft/plots/legacy_outage_windows_2025.png``).

Holidays in mozaic are per country tile, not per OS population, so the two windows are applied
as their union. ``holiday_smart`` measures the effect per tile and acts only where the dip
exceeds ``holiday_threshold``, which keeps the Mac-only part of the window from moving Windows
and Linux tiles. The calendar populates 2025 only, so it cannot leak into the forecast years.

This module has no mozaic_daily imports on purpose: ``queries.py`` imports it to register the
calendar in ``ADDITIONAL_HOLIDAYS`` and would otherwise import itself.
"""

import holidays
import pandas as pd

# Per-OS windows of record (inclusive), kept separately so the union is derived, not typed.
LEGACY_OUTAGE_WINDOWS_2025 = {
    "windows": ("2025-08-16", "2025-11-06"),
    "mac": ("2025-08-28", "2025-12-10"),
}

LEGACY_OUTAGE_LABEL = "Legacy telemetry outage 2025 (translated series)"


class LegacyOutage2025(holidays.HolidayBase):
    """Union of the per-OS 2025 Legacy outage windows, as one holiday run."""

    def _populate(self, year):
        if year != 2025:
            return
        start = min(pd.Timestamp(a) for a, _ in LEGACY_OUTAGE_WINDOWS_2025.values())
        end = max(pd.Timestamp(b) for _, b in LEGACY_OUTAGE_WINDOWS_2025.values())
        for day in pd.date_range(start, end):
            self[day.date()] = LEGACY_OUTAGE_LABEL
