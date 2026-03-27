# -*- coding: utf-8 -*-
"""
Tests for SQL query specifications in mozaic_daily.queries.

Tests cover query specs, date constraints, and SQL generation.
"""

import holidays

from mozaic_daily.queries import (
    QUERY_SPECS, ADDITIONAL_HOLIDAYS,
    Platform, Metric, TelemetrySource, DataSource,
    DateConstraints, AvailabilityCheckQuery, get_availability_check_queries,
)
from mozaic.holiday_smart import DesktopBugs
from mozaic_daily.data import get_queries
from mozaic_daily.config import get_runtime_config, build_filter_code


# ===== QUERY_SPECS STRUCTURE =====

def test_query_specs_contains_expected_count():
    """Verify QUERY_SPECS contains exactly 12 query specifications.

    Expected: 4 Desktop Glean + 4 Desktop Legacy + 4 Mobile Glean = 12

    Failure indicates query specifications added/removed without test update.
    """
    assert len(QUERY_SPECS) == 12, (
        f"Expected 12 query specs, got {len(QUERY_SPECS)}"
    )


def test_query_specs_key_structure():
    """Verify all keys in QUERY_SPECS are (Platform, Metric, TelemetrySource) tuples.

    Failure indicates wrong key format, breaks query lookup.
    """
    for key in QUERY_SPECS.keys():
        assert isinstance(key, tuple), (
            f"Expected key to be tuple, got {type(key)}: {key}"
        )
        assert len(key) == 3, (
            f"Expected key to be 3-tuple, got length {len(key)}: {key}"
        )
        platform, metric, telemetry_source = key
        assert isinstance(platform, Platform), (
            f"Expected first element to be Platform enum, got {type(platform)}: {platform}"
        )
        assert isinstance(metric, Metric), (
            f"Expected second element to be Metric enum, got {type(metric)}: {metric}"
        )
        assert isinstance(telemetry_source, TelemetrySource), (
            f"Expected third element to be TelemetrySource enum, got {type(telemetry_source)}: {telemetry_source}"
        )


def test_query_specs_covers_all_platform_metric_combinations():
    """Verify all required platform/metric combinations are present.

    Expected combinations:
    - Desktop: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU (Glean + Legacy)
    - Mobile: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU (Glean only)

    Failure indicates missing query specifications.
    """
    required_desktop_glean = [
        (Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN),
        (Platform.DESKTOP, Metric.NEW_PROFILES, TelemetrySource.GLEAN),
        (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.GLEAN),
        (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.GLEAN),
    ]

    required_desktop_legacy = [
        (Platform.DESKTOP, Metric.DAU, TelemetrySource.LEGACY),
        (Platform.DESKTOP, Metric.NEW_PROFILES, TelemetrySource.LEGACY),
        (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.LEGACY),
        (Platform.DESKTOP, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.LEGACY),
    ]

    required_mobile = [
        (Platform.MOBILE, Metric.DAU, TelemetrySource.GLEAN),
        (Platform.MOBILE, Metric.NEW_PROFILES, TelemetrySource.GLEAN),
        (Platform.MOBILE, Metric.EXISTING_ENGAGEMENT_DAU, TelemetrySource.GLEAN),
        (Platform.MOBILE, Metric.EXISTING_ENGAGEMENT_MAU, TelemetrySource.GLEAN),
    ]

    all_required = required_desktop_glean + required_desktop_legacy + required_mobile

    for key in all_required:
        assert key in QUERY_SPECS, (
            f"Missing required query spec: {key}"
        )


# ===== QuerySpec.data_source TESTS =====

def test_query_spec_data_source_desktop_glean():
    """Verify Desktop + Glean = glean_desktop.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.GLEAN_DESKTOP, (
        f"Expected Desktop + Glean → glean_desktop, got {spec.data_source}"
    )


def test_query_spec_data_source_desktop_legacy():
    """Verify Desktop + Legacy = legacy_desktop.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.DESKTOP, Metric.DAU, TelemetrySource.LEGACY)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.LEGACY_DESKTOP, (
        f"Expected Desktop + Legacy → legacy_desktop, got {spec.data_source}"
    )


def test_query_spec_data_source_mobile_glean():
    """Verify Mobile + Glean = glean_mobile.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.MOBILE, Metric.DAU, TelemetrySource.GLEAN)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.GLEAN_MOBILE, (
        f"Expected Mobile + Glean → glean_mobile, got {spec.data_source}"
    )


def test_all_query_specs_have_valid_data_source():
    """Verify all query specs derive valid DataSource enum values.

    Failure indicates invalid data_source derivation logic.
    """
    for key, spec in QUERY_SPECS.items():
        platform, metric, telemetry_source = key
        data_source = spec.data_source

        # Check it's a valid DataSource enum
        assert isinstance(data_source, DataSource), (
            f"Query spec {key}: data_source is not DataSource enum, got {type(data_source)}"
        )

        # Verify expected mappings
        if platform == Platform.DESKTOP:
            if telemetry_source == TelemetrySource.GLEAN:
                assert data_source == DataSource.GLEAN_DESKTOP, (
                    f"Query spec {key}: expected glean_desktop, got {data_source}"
                )
            else:  # LEGACY
                assert data_source == DataSource.LEGACY_DESKTOP, (
                    f"Query spec {key}: expected legacy_desktop, got {data_source}"
                )
        else:  # MOBILE
            assert data_source == DataSource.GLEAN_MOBILE, (
                f"Query spec {key}: expected glean_mobile, got {data_source}"
            )


# ===== DateConstraints TESTS =====

def test_date_constraints_simple_start_date():
    """Verify DateConstraints generates correct SQL for simple start date.

    Example: date_field >= "2023-04-17"

    Failure indicates broken SQL generation.
    """
    constraints = DateConstraints(
        date_field='submission_date',
        date_start='2023-04-17',
    )

    sql = constraints.to_sql_clause()
    expected = 'submission_date >= "2023-04-17"'

    assert sql == expected, (
        f"Expected SQL: {expected}\nGot: {sql}"
    )


def test_date_constraints_with_exclusion():
    """Verify DateConstraints generates SQL with NOT BETWEEN clause.

    Example: date >= "2023-07-01" AND date NOT BETWEEN "2023-07-18" AND "2023-07-19"

    Failure indicates broken exclusion logic.
    """
    constraints = DateConstraints(
        date_field='first_seen_date',
        date_start='2023-07-01',
        date_excludes=(('2023-07-18', '2023-07-19'),),
    )

    sql = constraints.to_sql_clause()

    # Check both parts are present
    assert 'first_seen_date >= "2023-07-01"' in sql, (
        f"Expected start date constraint in SQL: {sql}"
    )
    assert 'first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19"' in sql, (
        f"Expected exclusion constraint in SQL: {sql}"
    )


def test_date_constraints_custom_quote_character():
    """Verify DateConstraints supports custom quote character.

    Default uses double quotes, but should support single quotes.

    Failure indicates broken quote parameter.
    """
    constraints = DateConstraints(
        date_field='submission_date',
        date_start='2023-04-17',
    )

    sql = constraints.to_sql_clause(quote="'")
    expected = "submission_date >= '2023-04-17'"

    assert sql == expected, (
        f"Expected SQL with single quotes: {expected}\nGot: {sql}"
    )


# ===== get_queries() TESTS =====

def test_get_queries_returns_dict_with_platform_keys():
    """Verify get_queries() returns dict with 'desktop' and 'mobile' keys.

    Failure indicates wrong return structure.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'])

    assert isinstance(queries, dict), (
        f"Expected dict, got {type(queries)}"
    )
    assert 'desktop' in queries, (
        f"Expected 'desktop' key in queries. Found keys: {queries.keys()}"
    )
    assert 'mobile' in queries, (
        f"Expected 'mobile' key in queries. Found keys: {queries.keys()}"
    )


def test_get_queries_desktop_contains_all_metrics():
    """Verify Desktop queries contain all 4 metrics in both glean and legacy sources.

    Expected: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU

    Failure indicates missing Desktop metric queries.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'])

    expected_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

    # Check Desktop has both glean and legacy sources
    assert 'glean' in queries['desktop'], "Expected 'glean' source in Desktop queries"
    assert 'legacy' in queries['desktop'], "Expected 'legacy' source in Desktop queries"

    # Check glean source has all metrics
    desktop_glean_metrics = list(queries['desktop']['glean'].keys())
    assert set(desktop_glean_metrics) == set(expected_metrics), (
        f"Expected Desktop Glean metrics {expected_metrics}, got {desktop_glean_metrics}"
    )

    # Check legacy source has all metrics
    desktop_legacy_metrics = list(queries['desktop']['legacy'].keys())
    assert set(desktop_legacy_metrics) == set(expected_metrics), (
        f"Expected Desktop Legacy metrics {expected_metrics}, got {desktop_legacy_metrics}"
    )


def test_get_queries_mobile_contains_all_metrics():
    """Verify Mobile queries contain all 4 metrics in glean source.

    Expected: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU

    Failure indicates missing Mobile metric queries.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'])

    expected_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']

    # Check Mobile has glean source
    assert 'glean' in queries['mobile'], "Expected 'glean' source in Mobile queries"

    # Check glean source has all metrics
    mobile_glean_metrics = list(queries['mobile']['glean'].keys())
    assert set(mobile_glean_metrics) == set(expected_metrics), (
        f"Expected Mobile Glean metrics {expected_metrics}, got {mobile_glean_metrics}"
    )


def test_get_queries_includes_date_constraints_in_sql():
    """Verify generated SQL includes date constraints from QuerySpec.

    Checks Desktop Legacy New Profiles query for:
    - first_seen_date >= "2020-01-01" (Legacy Desktop constraint)
    - first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19"

    Failure indicates date constraints not applied to SQL.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'])

    # Access the SQL from the (sql, spec) tuple
    desktop_new_profiles_sql, _ = queries['desktop']['legacy']['New Profiles']

    # Check for the Legacy Desktop date constraint
    assert 'first_seen_date >= "2021-01-01"' in desktop_new_profiles_sql, (
        "Expected date start constraint in Desktop Legacy New Profiles SQL"
    )
    assert 'first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19"' in desktop_new_profiles_sql, (
        "Expected date exclusion in Desktop Legacy New Profiles SQL"
    )


def test_get_queries_includes_country_filter():
    """Verify country_string parameter is included in generated SQL.

    Failure indicates country filtering not applied.
    """
    config = get_runtime_config()
    country_string = "'US', 'DE', 'FR'"
    queries = get_queries(country_string)

    # Access the SQL from the (sql, spec) tuple
    desktop_dau_sql, _ = queries['desktop']['glean']['DAU']

    # Country filter appears in IF(country IN (...), country, 'ROW') clause
    assert country_string in desktop_dau_sql, (
        f"Expected country filter '{country_string}' in Desktop DAU SQL"
    )


def test_get_queries_data_source_filter_returns_matching_queries():
    """Verify data_source_filter limits queries to the specified data source.

    Failure indicates filter not working.
    """
    config = get_runtime_config()
    queries = get_queries(
        config['country_string'],
        data_source_filter={DataSource.GLEAN_DESKTOP},
    )

    # Desktop Glean should have all metrics
    assert len(queries['desktop']['glean']) > 0, "Expected Desktop Glean queries"

    # Desktop legacy and mobile should be empty
    assert len(queries['desktop']['legacy']) == 0, (
        f"Expected 0 Desktop Legacy queries, got {len(queries['desktop']['legacy'])}"
    )
    assert len(queries['mobile']['glean']) == 0, (
        f"Expected 0 Mobile queries, got {len(queries['mobile']['glean'])}"
    )


def test_get_queries_metric_filter_returns_matching_queries():
    """Verify metric_filter limits queries to the specified metric."""
    config = get_runtime_config()
    queries = get_queries(
        config['country_string'],
        metric_filter={Metric.DAU},
    )

    # Every non-empty source dict should only contain DAU
    for platform, sources in queries.items():
        for source, metrics in sources.items():
            for metric_name in metrics:
                assert metric_name == 'DAU', (
                    f"Expected only DAU metric, got '{metric_name}' in {platform}/{source}"
                )


def test_get_queries_combined_filters():
    """Verify data_source_filter + metric_filter narrows to intersection."""
    config = get_runtime_config()
    queries = get_queries(
        config['country_string'],
        data_source_filter={DataSource.GLEAN_DESKTOP},
        metric_filter={Metric.DAU},
    )

    # Should have exactly 1 query: Desktop Glean DAU
    assert len(queries['desktop']['glean']) == 1
    assert 'DAU' in queries['desktop']['glean']
    assert len(queries['desktop']['legacy']) == 0
    assert len(queries['mobile']['glean']) == 0


# ===== QuerySpec.build_query() TESTS =====

def test_build_query_contains_select_clause():
    """Verify build_query() generates valid SQL with SELECT clause.

    Failure indicates broken SQL generation.
    """
    spec = QUERY_SPECS[(Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN)]
    query = spec.build_query("'US', 'DE'")

    assert 'SELECT' in query.upper(), (
        "Expected SELECT clause in generated SQL"
    )
    assert 'FROM' in query.upper(), (
        "Expected FROM clause in generated SQL"
    )
    assert 'WHERE' in query.upper(), (
        "Expected WHERE clause in generated SQL"
    )
    assert 'GROUP BY' in query.upper(), (
        "Expected GROUP BY clause in generated SQL"
    )


def test_build_query_desktop_includes_windows_segments():
    """Verify Desktop queries include modern_windows, winX columns.

    Failure indicates Desktop segmentation broken.
    """
    spec = QUERY_SPECS[(Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN)]
    query = spec.build_query("'US'")

    assert 'modern_windows' in query.lower(), (
        "Expected modern_windows column in Desktop SQL"
    )
    assert 'winx' in query.lower(), (
        "Expected winX column in Desktop SQL"
    )


def test_build_query_mobile_includes_app_segments():
    """Verify Mobile queries include fenix_android, firefox_ios, focus_android, focus_ios columns.

    Failure indicates Mobile segmentation broken.
    """
    spec = QUERY_SPECS[(Platform.MOBILE, Metric.DAU, TelemetrySource.GLEAN)]
    query = spec.build_query("'US'")

    assert 'fenix_android' in query.lower(), (
        "Expected fenix_android column in Mobile SQL"
    )
    assert 'firefox_ios' in query.lower(), (
        "Expected firefox_ios column in Mobile SQL"
    )
    assert 'focus_android' in query.lower(), (
        "Expected focus_android column in Mobile SQL"
    )
    assert 'focus_ios' in query.lower(), (
        "Expected focus_ios column in Mobile SQL"
    )


# ===== get_availability_check_queries() TESTS =====

def test_get_availability_check_queries_returns_list_of_correct_type():
    """Verify get_availability_check_queries() returns a list of AvailabilityCheckQuery.

    Failure indicates wrong return type from the function.
    """
    checks = get_availability_check_queries()

    assert isinstance(checks, list), (
        f"Expected list, got {type(checks)}"
    )
    for check in checks:
        assert isinstance(check, AvailabilityCheckQuery), (
            f"Expected AvailabilityCheckQuery, got {type(check)}: {check}"
        )


def test_get_availability_check_queries_deduplicates():
    """Verify deduplication reduces 12 query specs to fewer unique checks.

    Desktop Glean EE DAU/MAU share the same table and filter.
    Desktop Legacy EE DAU/MAU share the same table and filter.
    Mobile Glean EE DAU/MAU share the same table and filter.
    So 12 specs -> 9 unique checks.

    Failure indicates deduplication is not working.
    """
    checks = get_availability_check_queries()

    assert len(checks) < len(QUERY_SPECS), (
        f"Expected fewer checks than query specs ({len(QUERY_SPECS)}), "
        f"got {len(checks)} — deduplication may not be working"
    )
    assert len(checks) == 9, (
        f"Expected 9 unique availability checks (12 specs minus 3 duplicates), "
        f"got {len(checks)}"
    )


def test_get_availability_check_queries_each_has_nonempty_fields():
    """Verify every check has non-empty table, date_field, where_clause, and sql.

    Failure indicates incomplete AvailabilityCheckQuery construction.
    """
    checks = get_availability_check_queries()

    for check in checks:
        assert check.table, f"Expected non-empty table, got: {check.table!r}"
        assert check.date_field, f"Expected non-empty date_field, got: {check.date_field!r}"
        assert check.where_clause, f"Expected non-empty where_clause, got: {check.where_clause!r}"
        assert check.sql, f"Expected non-empty sql, got: {check.sql!r}"


def test_get_availability_check_queries_sql_structure():
    """Verify each check's SQL is a valid MAX(date_field) query with partition filter.

    Expected form:
        SELECT MAX(date_field) AS max_date FROM `table`
        WHERE where_clause AND date_field >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)

    The DATE_SUB filter is required for BigQuery partition elimination — the tables
    are views over partitioned underlying tables that reject unfiltered scans.

    Failure indicates wrong SQL construction.
    """
    checks = get_availability_check_queries()

    for check in checks:
        sql_upper = check.sql.upper()
        assert 'SELECT MAX(' in sql_upper, (
            f"Expected 'SELECT MAX(' in SQL: {check.sql}"
        )
        assert 'AS MAX_DATE' in sql_upper, (
            f"Expected 'AS max_date' in SQL: {check.sql}"
        )
        assert 'FROM' in sql_upper, (
            f"Expected 'FROM' in SQL: {check.sql}"
        )
        assert 'WHERE' in sql_upper, (
            f"Expected 'WHERE' in SQL: {check.sql}"
        )
        # Table name should be backtick-quoted for BigQuery
        assert f'`{check.table}`' in check.sql, (
            f"Expected table name '{check.table}' to be backtick-quoted in SQL: {check.sql}"
        )
        # Partition filter is required to satisfy BigQuery partition elimination
        assert 'DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)' in check.sql, (
            f"Expected DATE_SUB partition filter in SQL: {check.sql}"
        )


def test_get_availability_check_queries_no_duplicate_combinations():
    """Verify no two checks have the same (table, date_field, where_clause).

    Failure indicates deduplication logic is broken.
    """
    checks = get_availability_check_queries()
    seen_keys = set()

    for check in checks:
        key = (check.table, check.date_field, check.where_clause)
        assert key not in seen_keys, (
            f"Duplicate check found: table={check.table}, "
            f"date_field={check.date_field}, where_clause={check.where_clause}"
        )
        seen_keys.add(key)


# ===== ADDITIONAL_HOLIDAYS TESTS =====

def test_additional_holidays_only_on_legacy_desktop():
    """Verify ADDITIONAL_HOLIDAYS maps only legacy_desktop to [DesktopBugs].

    Failure indicates holidays assigned to wrong data source or missing for legacy_desktop.
    """
    assert set(ADDITIONAL_HOLIDAYS.keys()) == {DataSource.LEGACY_DESKTOP}, (
        f"Expected ADDITIONAL_HOLIDAYS only for LEGACY_DESKTOP, got: {set(ADDITIONAL_HOLIDAYS.keys())}"
    )
    assert ADDITIONAL_HOLIDAYS[DataSource.LEGACY_DESKTOP] == [DesktopBugs], (
        f"Expected [DesktopBugs] for LEGACY_DESKTOP, got: {ADDITIONAL_HOLIDAYS[DataSource.LEGACY_DESKTOP]}"
    )


def test_additional_holidays_entries_are_holiday_base_subclasses():
    """Verify all entries in ADDITIONAL_HOLIDAYS are holidays.HolidayBase subclasses.

    Failure indicates invalid holiday class in the mapping.
    """
    for data_source, holiday_list in ADDITIONAL_HOLIDAYS.items():
        assert isinstance(holiday_list, list), (
            f"Expected list for {data_source}, got {type(holiday_list)}"
        )
        for holiday_cls in holiday_list:
            assert isinstance(holiday_cls, type) and issubclass(holiday_cls, holidays.HolidayBase), (
                f"Expected HolidayBase subclass for {data_source}, got {holiday_cls}"
            )


# ===== SHORT CODE TESTS =====

def test_data_source_short_codes():
    """Verify short_code for all DataSource enum members."""
    assert DataSource.GLEAN_DESKTOP.short_code == "gd"
    assert DataSource.LEGACY_DESKTOP.short_code == "ld"
    assert DataSource.GLEAN_MOBILE.short_code == "gm"


def test_metric_short_codes():
    """Verify short_code for all Metric enum members."""
    assert Metric.DAU.short_code == "D"
    assert Metric.NEW_PROFILES.short_code == "NP"
    assert Metric.EXISTING_ENGAGEMENT_DAU.short_code == "EED"
    assert Metric.EXISTING_ENGAGEMENT_MAU.short_code == "EEM"


def test_all_data_sources_have_short_code():
    """Every DataSource member must have a short_code."""
    for ds in DataSource:
        assert isinstance(ds.short_code, str) and len(ds.short_code) > 0


def test_all_metrics_have_short_code():
    """Every Metric member must have a short_code."""
    for m in Metric:
        assert isinstance(m.short_code, str) and len(m.short_code) > 0


# ===== build_filter_code TESTS =====

def test_build_filter_code_single_source_single_metric():
    """Single data source + single metric produces 'code-code' format."""
    result = build_filter_code({DataSource.LEGACY_DESKTOP}, {Metric.DAU})
    assert result == "ld-D"


def test_build_filter_code_multiple_sources():
    """Multiple data sources joined with '+', sorted alphabetically."""
    result = build_filter_code({DataSource.GLEAN_DESKTOP, DataSource.GLEAN_MOBILE}, None)
    assert result == "gd+gm"


def test_build_filter_code_multiple_metrics():
    """Multiple metrics joined with '+', sorted alphabetically."""
    result = build_filter_code(None, {Metric.DAU, Metric.NEW_PROFILES})
    assert result == "D+NP"


def test_build_filter_code_sources_only():
    """Only data source filter, no metric filter."""
    result = build_filter_code({DataSource.GLEAN_MOBILE}, None)
    assert result == "gm"


def test_build_filter_code_metrics_only():
    """Only metric filter, no data source filter."""
    result = build_filter_code(None, {Metric.EXISTING_ENGAGEMENT_DAU})
    assert result == "EED"


def test_build_filter_code_no_filters():
    """No filters returns empty string."""
    result = build_filter_code(None, None)
    assert result == ""


def test_build_filter_code_all_sources_all_metrics():
    """All sources and all metrics."""
    all_sources = set(DataSource)
    all_metrics = set(Metric)
    result = build_filter_code(all_sources, all_metrics)
    # Sources sorted: gd, gm, ld; Metrics sorted: D, EED, EEM, NP
    assert result == "gd+gm+ld-D+EED+EEM+NP"
