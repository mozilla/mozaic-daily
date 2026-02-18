# -*- coding: utf-8 -*-
"""SQL query specifications and configuration.

This module defines all SQL query metadata using dataclasses, replacing the
duplicated configuration that existed in config.py and data.py.

Key components:
- Enums for Platform, Metric, TelemetrySource, DataSource
- DateConstraints: date filtering with SQL generation
- QuerySpec: complete specification for a single query
- QUERY_SPECS: dictionary of all query configurations
- Helper functions for validation and backward compatibility
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
from enum import Enum

import pandas as pd


# =============================================================================
# ENUMS
# =============================================================================

class Platform(Enum):
    """Platform type: Desktop or Mobile."""
    DESKTOP = "desktop"
    MOBILE = "mobile"


class Metric(Enum):
    """Available metrics for forecasting."""
    DAU = "DAU"
    NEW_PROFILES = "New Profiles"
    EXISTING_ENGAGEMENT_DAU = "Existing Engagement DAU"
    EXISTING_ENGAGEMENT_MAU = "Existing Engagement MAU"


class TelemetrySource(Enum):
    """Telemetry data source: Glean or Legacy."""
    GLEAN = "glean"
    LEGACY = "legacy"


class DataSource(Enum):
    """Output data_source column values (derived from Platform + TelemetrySource)."""
    GLEAN_DESKTOP = "glean_desktop"
    LEGACY_DESKTOP = "legacy_desktop"
    GLEAN_MOBILE = "glean_mobile"
    # Note: LEGACY_MOBILE does not exist

    @property
    def platform(self) -> Platform:
        """Return the platform for this data source."""
        if self in (DataSource.GLEAN_DESKTOP, DataSource.LEGACY_DESKTOP):
            return Platform.DESKTOP
        return Platform.MOBILE

    @property
    def telemetry_source(self) -> TelemetrySource:
        """Return the telemetry source for this data source."""
        if self in (DataSource.GLEAN_DESKTOP, DataSource.GLEAN_MOBILE):
            return TelemetrySource.GLEAN
        return TelemetrySource.LEGACY

    @property
    def display_name(self) -> str:
        """Return human-readable name for logging (e.g., 'Desktop Glean')."""
        return f"{self.platform.value.capitalize()} {self.telemetry_source.value.capitalize()}"

    @classmethod
    def from_platform_source(cls, platform: str, source: str) -> 'DataSource':
        """Convert platform and source strings to DataSource enum.

        Args:
            platform: Platform string ('desktop' or 'mobile')
            source: Telemetry source string ('glean' or 'legacy')

        Returns:
            Corresponding DataSource enum value

        Raises:
            ValueError: If combination is invalid (e.g., 'mobile' + 'legacy')
        """
        platform_lower = platform.lower()
        source_lower = source.lower()

        if platform_lower == 'desktop':
            if source_lower == 'glean':
                return cls.GLEAN_DESKTOP
            elif source_lower == 'legacy':
                return cls.LEGACY_DESKTOP
        elif platform_lower == 'mobile':
            if source_lower == 'glean':
                return cls.GLEAN_MOBILE

        raise ValueError(f"Invalid platform/source combination: {platform}/{source}")


# Query key is a 3-tuple
QueryKey = Tuple[Platform, Metric, TelemetrySource]


# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass(frozen=True)
class DateConstraints:
    """Date filtering constraints for a query.

    Attributes:
        date_field: Name of the date column (e.g., 'submission_date', 'first_seen_date')
        date_start: Start date in 'YYYY-MM-DD' format
        date_excludes: List of (start, end) date ranges to exclude
    """
    date_field: str
    date_start: str
    date_excludes: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)

    def to_sql_clause(self, quote: str = '"') -> str:
        """Generate SQL WHERE clause for date constraints.

        Args:
            quote: Quote character for date strings (default: double quote)

        Returns:
            SQL clause like: date_field >= "2023-01-01" AND date_field NOT BETWEEN ...
        """
        parts = [f'{self.date_field} >= {quote}{self.date_start}{quote}']
        for ex_start, ex_end in self.date_excludes:
            parts.append(
                f'{self.date_field} NOT BETWEEN {quote}{ex_start}{quote} AND {quote}{ex_end}{quote}'
            )
        return " AND ".join(parts)


@dataclass(frozen=True)
class QuerySpec:
    """Complete specification for a single SQL query.

    Attributes:
        platform: Platform enum (Desktop or Mobile)
        metric: Metric enum (DAU, New Profiles, etc.)
        telemetry_source: TelemetrySource enum (Glean or Legacy)
        table: Full BigQuery table name
        segment_column: Column name for segmentation (os_version, app_name, etc.)
        where_clause: SQL WHERE clause for filtering rows
        date_constraints: DateConstraints object with date filtering
        x_column: Column name for x-axis (date)
        y_column: Column name for y-axis (metric value)
    """
    platform: Platform
    metric: Metric
    telemetry_source: TelemetrySource
    table: str
    segment_column: str
    where_clause: str
    date_constraints: DateConstraints
    x_column: str
    y_column: str

    @property
    def key(self) -> QueryKey:
        """Return the 3-tuple query key."""
        return (self.platform, self.metric, self.telemetry_source)

    @property
    def data_source(self) -> DataSource:
        """Derive the data_source value from platform + telemetry_source.

        Returns:
            DataSource enum value (Glean_Desktop, Legacy_Desktop, or Glean_Mobile)
        """
        if self.platform == Platform.DESKTOP:
            if self.telemetry_source == TelemetrySource.GLEAN:
                return DataSource.GLEAN_DESKTOP
            else:
                return DataSource.LEGACY_DESKTOP
        else:  # MOBILE
            # Mobile only has GLEAN
            return DataSource.GLEAN_MOBILE

    def build_query(self, countries: str) -> str:
        """Build the complete SQL query for this specification.

        Automatically uses the appropriate segmentation logic based on platform:
        - Desktop: win10, win11, winX columns from Windows version
        - Mobile: fenix_android, firefox_ios, focus_android, focus_ios from app name

        Args:
            countries: SQL-formatted country list string (e.g., "'US', 'DE', 'FR'")

        Returns:
            Complete SQL query string ready for BigQuery execution
        """
        where_clause = f'{self.where_clause} AND {self.date_constraints.to_sql_clause()}'

        if self.platform == Platform.DESKTOP:
            segment_columns = _build_desktop_segment_columns(self.segment_column)
        else:  # MOBILE
            segment_columns = _build_mobile_segment_columns(self.segment_column)

        return f"""
    SELECT {self.x_column} AS x,
           IF(country IN ({countries}), country, 'ROW') AS country,
           {segment_columns},
           SUM({self.y_column}) AS y,
     FROM `{self.table}`
    WHERE {where_clause}
    GROUP BY ALL
    ORDER BY 1, 2 ASC
    """


@dataclass(frozen=True)
class AvailabilityCheckQuery:
    """Query to check maximum available date in a BigQuery table.

    Used for pre-flight checks to verify training data has landed before
    starting the full pipeline.

    Attributes:
        table: Full BigQuery table name
        date_field: Name of the date column to check
        where_clause: SQL WHERE clause to filter rows (same as the main query)
        sql: Complete SQL query string ready for BigQuery execution
    """
    table: str
    date_field: str
    where_clause: str
    sql: str


def _build_desktop_segment_columns(segment_column: str) -> str:
    """Build SQL SELECT columns for Windows version segmentation."""
    return f"""IFNULL(LOWER({segment_column}) LIKE '%windows 10%', FALSE) AS win10,
           IFNULL(LOWER({segment_column}) LIKE '%windows 11%', FALSE) AS win11,
           IFNULL(LOWER({segment_column}) LIKE '%windows%' AND LOWER({segment_column}) NOT LIKE '%windows 10%' AND LOWER({segment_column}) NOT LIKE '%windows 11%', FALSE) AS winX"""


def _build_mobile_segment_columns(segment_column: str) -> str:
    """Build SQL SELECT columns for mobile app segmentation."""
    return f"""IFNULL(LOWER({segment_column}) LIKE '%fenix%', FALSE) AS fenix_android,
           IFNULL(LOWER({segment_column}) LIKE '%firefox ios%', FALSE) AS firefox_ios,
           IFNULL(LOWER({segment_column}) LIKE '%focus android%', FALSE) AS focus_android,
           IFNULL(LOWER({segment_column}) LIKE '%focus ios%', FALSE) AS focus_ios"""


# =============================================================================
# QUERY SPECIFICATIONS
# =============================================================================

QUERY_SPECS: Dict[QueryKey, QuerySpec] = {
    # =========================================================================
    # DESKTOP GLEAN
    # =========================================================================
    (Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.DAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates',
        segment_column='os_version',
        where_clause='app_name = "Firefox Desktop"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2023-04-17',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.DESKTOP, Metric.NEW_PROFILES, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.NEW_PROFILES,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.firefox_desktop.new_profiles_aggregates',
        segment_column='windows_version',
        where_clause='is_desktop',
        date_constraints=DateConstraints(
            date_field='first_seen_date',
            date_start='2023-07-01',
            date_excludes=(('2023-07-18', '2023-07-19'),),
        ),
        x_column='first_seen_date',
        y_column='new_profiles',
    ),
    (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.EXISTING_ENGAGEMENT_DAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates',
        segment_column='normalized_os_version',
        where_clause='is_desktop AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2023-06-07',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.EXISTING_ENGAGEMENT_MAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.firefox_desktop.desktop_engagement_aggregates',
        segment_column='normalized_os_version',
        where_clause='is_desktop AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2023-06-07',
        ),
        x_column='submission_date',
        y_column='mau',
    ),

    # =========================================================================
    # DESKTOP LEGACY
    # =========================================================================
    (Platform.DESKTOP, Metric.DAU, TelemetrySource.LEGACY): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.DAU,
        telemetry_source=TelemetrySource.LEGACY,
        table='moz-fx-data-shared-prod.telemetry.active_users_aggregates',
        segment_column='os_version',
        where_clause='app_name = "Firefox Desktop"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2020-01-01',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.DESKTOP, Metric.NEW_PROFILES, TelemetrySource.LEGACY): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.NEW_PROFILES,
        telemetry_source=TelemetrySource.LEGACY,
        table='moz-fx-data-shared-prod.telemetry.desktop_new_profiles',
        segment_column='windows_version',
        where_clause='is_desktop',
        date_constraints=DateConstraints(
            date_field='first_seen_date',
            date_start='2021-01-01',
            date_excludes=(('2023-07-18', '2023-07-19'),),
        ),
        x_column='first_seen_date',
        y_column='new_profiles',
    ),
    (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.LEGACY): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.EXISTING_ENGAGEMENT_DAU,
        telemetry_source=TelemetrySource.LEGACY,
        table='moz-fx-data-shared-prod.telemetry.desktop_engagement',
        segment_column='normalized_os_version',
        where_clause='is_desktop AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2021-01-01',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.LEGACY): QuerySpec(
        platform=Platform.DESKTOP,
        metric=Metric.EXISTING_ENGAGEMENT_MAU,
        telemetry_source=TelemetrySource.LEGACY,
        table='moz-fx-data-shared-prod.telemetry.desktop_engagement',
        segment_column='normalized_os_version',
        where_clause='is_desktop AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2021-01-01',
        ),
        x_column='submission_date',
        y_column='mau',
    ),

    # =========================================================================
    # MOBILE GLEAN (Mobile only has GLEAN, no LEGACY)
    # =========================================================================
    (Platform.MOBILE, Metric.DAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.MOBILE,
        metric=Metric.DAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates',
        segment_column='app_name',
        where_clause='app_name IN ("Fenix", "Firefox iOS", "Focus Android", "Focus iOS")',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2020-12-31',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.MOBILE, Metric.NEW_PROFILES, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.MOBILE,
        metric=Metric.NEW_PROFILES,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.telemetry.mobile_new_profiles',
        segment_column='app_name',
        where_clause='is_mobile',
        date_constraints=DateConstraints(
            date_field='first_seen_date',
            date_start='2023-07-01',
            date_excludes=(('2023-07-18', '2023-07-19'),),
        ),
        x_column='first_seen_date',
        y_column='new_profiles',
    ),
    (Platform.MOBILE, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.MOBILE,
        metric=Metric.EXISTING_ENGAGEMENT_DAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.telemetry.mobile_engagement',
        segment_column='app_name',
        where_clause='is_mobile AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2023-07-01',
        ),
        x_column='submission_date',
        y_column='dau',
    ),
    (Platform.MOBILE, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.GLEAN): QuerySpec(
        platform=Platform.MOBILE,
        metric=Metric.EXISTING_ENGAGEMENT_MAU,
        telemetry_source=TelemetrySource.GLEAN,
        table='moz-fx-data-shared-prod.telemetry.mobile_engagement',
        segment_column='app_name',
        where_clause='is_mobile AND lifecycle_stage = "existing_user"',
        date_constraints=DateConstraints(
            date_field='submission_date',
            date_start='2023-07-01',
        ),
        x_column='submission_date',
        y_column='mau',
    ),
}


# =============================================================================
# HELPERS
# =============================================================================

def get_availability_check_queries() -> List[AvailabilityCheckQuery]:
    """Build pre-flight check queries for all unique table/date combinations.

    Deduplicates QUERY_SPECS by (table, date_field, where_clause) so that tables
    shared across metrics (e.g., desktop_engagement_aggregates for DAU and MAU)
    are only checked once. The 12 query specs reduce to 9 unique checks.

    Each check queries MAX(date_field) to verify training data has landed in BQ.

    Returns:
        List of AvailabilityCheckQuery instances, one per unique table/date combo
    """
    seen = set()
    checks = []

    for spec in QUERY_SPECS.values():
        dedup_key = (spec.table, spec.date_constraints.date_field, spec.where_clause)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        table = spec.table
        date_field = spec.date_constraints.date_field
        where_clause = spec.where_clause
        sql = (
            f"SELECT MAX({date_field}) AS max_date FROM `{table}`"
            f" WHERE {where_clause}"
            f" AND {date_field} >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
        )
        checks.append(AvailabilityCheckQuery(
            table=table,
            date_field=date_field,
            where_clause=where_clause,
            sql=sql,
        ))

    return checks


def get_date_keys() -> List[Tuple[str, str, str]]:
    """Return all unique (platform, metric, source) keys from query specifications.

    Returns:
        List of (platform, metric, source) 3-tuples
    """
    seen = set()
    keys = []
    for spec in QUERY_SPECS.values():
        key = (spec.platform.value, spec.metric.value, spec.telemetry_source.value)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def get_training_date_index(
    key: Tuple[str, str, str],
    end: Optional[str] = None,
) -> pd.DatetimeIndex:
    """Generate DatetimeIndex for training data, excluding specified date ranges.

    Args:
        key: (platform, metric, source) 3-tuple
        end: Optional end date string, defaults to training_end_date from runtime config

    Returns:
        DatetimeIndex with valid training dates

    Raises:
        KeyError: If the key is not found in QUERY_SPECS
    """
    # Find matching spec
    for spec in QUERY_SPECS.values():
        if (spec.platform.value, spec.metric.value, spec.telemetry_source.value) == key:
            start = pd.to_datetime(spec.date_constraints.date_start).normalize()
            if end:
                end_dt = pd.to_datetime(end).normalize()
            else:
                # Import here to avoid circular dependency
                from .config import get_runtime_config
                end_dt = pd.to_datetime(get_runtime_config()['training_end_date']).normalize()

            full = pd.date_range(start=start, end=end_dt, freq='D')
            excludes = spec.date_constraints.date_excludes
            if not excludes:
                return full

            mask = pd.Series(True, index=full)
            for ex_start, ex_end in excludes:
                ex_s = pd.to_datetime(ex_start).normalize()
                ex_e = pd.to_datetime(ex_end).normalize()
                mask.loc[(full >= ex_s) & (full <= ex_e)] = False
            return full[mask.values]

    raise KeyError(f"Unknown key: {key}")
