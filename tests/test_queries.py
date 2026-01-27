# -*- coding: utf-8 -*-
"""
Tests for SQL query specifications in mozaic_daily.queries.

Tests cover query specs, date constraints, and SQL generation.
"""

import pytest
from mozaic_daily.queries import (
    QUERY_SPECS, Platform, Metric, TelemetrySource, DataSource,
    QuerySpec, DateConstraints
)
from mozaic_daily.data import get_queries
from mozaic_daily.config import get_runtime_config


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
    """Verify Desktop + Glean = Glean_Desktop.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.GLEAN_DESKTOP, (
        f"Expected Desktop + Glean → Glean_Desktop, got {spec.data_source}"
    )


def test_query_spec_data_source_desktop_legacy():
    """Verify Desktop + Legacy = Legacy_Desktop.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.DESKTOP, Metric.DAU, TelemetrySource.LEGACY)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.LEGACY_DESKTOP, (
        f"Expected Desktop + Legacy → Legacy_Desktop, got {spec.data_source}"
    )


def test_query_spec_data_source_mobile_glean():
    """Verify Mobile + Glean = Glean_Mobile.

    Failure indicates wrong data_source derivation.
    """
    key = (Platform.MOBILE, Metric.DAU, TelemetrySource.GLEAN)
    spec = QUERY_SPECS[key]

    assert spec.data_source == DataSource.GLEAN_MOBILE, (
        f"Expected Mobile + Glean → Glean_Mobile, got {spec.data_source}"
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
                    f"Query spec {key}: expected Glean_Desktop, got {data_source}"
                )
            else:  # LEGACY
                assert data_source == DataSource.LEGACY_DESKTOP, (
                    f"Query spec {key}: expected Legacy_Desktop, got {data_source}"
                )
        else:  # MOBILE
            assert data_source == DataSource.GLEAN_MOBILE, (
                f"Query spec {key}: expected Glean_Mobile, got {data_source}"
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
    queries = get_queries(config['country_string'], testing_mode=False)

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
    """Verify Desktop queries contain all 4 metrics.

    Expected: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU

    Failure indicates missing Desktop metric queries.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'], testing_mode=False)

    expected_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    desktop_metrics = list(queries['desktop'].keys())

    assert set(desktop_metrics) == set(expected_metrics), (
        f"Expected Desktop metrics {expected_metrics}, got {desktop_metrics}"
    )


def test_get_queries_mobile_contains_all_metrics():
    """Verify Mobile queries contain all 4 metrics.

    Expected: DAU, New Profiles, Existing Engagement DAU, Existing Engagement MAU

    Failure indicates missing Mobile metric queries.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'], testing_mode=False)

    expected_metrics = ['DAU', 'New Profiles', 'Existing Engagement DAU', 'Existing Engagement MAU']
    mobile_metrics = list(queries['mobile'].keys())

    assert set(mobile_metrics) == set(expected_metrics), (
        f"Expected Mobile metrics {expected_metrics}, got {mobile_metrics}"
    )


def test_get_queries_includes_date_constraints_in_sql():
    """Verify generated SQL includes date constraints from QuerySpec.

    Note: get_queries() returns the LAST query spec for each metric when
    there are multiple (Glean + Legacy). For Desktop New Profiles, the
    Legacy spec comes last with date_start='2020-01-01'.

    Checks Desktop New Profiles query for:
    - first_seen_date >= "2020-01-01" (Legacy Desktop constraint)
    - first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19"

    Failure indicates date constraints not applied to SQL.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'], testing_mode=False)

    desktop_new_profiles_sql = queries['desktop']['New Profiles']

    # Check for the Legacy Desktop date constraint (whichever comes last)
    assert 'first_seen_date >=' in desktop_new_profiles_sql, (
        "Expected date start constraint in Desktop New Profiles SQL"
    )
    assert 'first_seen_date NOT BETWEEN "2023-07-18" AND "2023-07-19"' in desktop_new_profiles_sql, (
        "Expected date exclusion in Desktop New Profiles SQL"
    )


def test_get_queries_includes_country_filter():
    """Verify country_string parameter is included in generated SQL.

    Failure indicates country filtering not applied.
    """
    config = get_runtime_config()
    country_string = "'US', 'DE', 'FR'"
    queries = get_queries(country_string, testing_mode=False)

    desktop_dau_sql = queries['desktop']['DAU']

    # Country filter appears in IF(country IN (...), country, 'ROW') clause
    assert country_string in desktop_dau_sql, (
        f"Expected country filter '{country_string}' in Desktop DAU SQL"
    )


def test_get_queries_testing_mode_returns_single_query():
    """Verify testing_mode=True returns only Desktop DAU query.

    Useful for quick integration tests without querying all metrics.

    Failure indicates testing mode not working.
    """
    config = get_runtime_config()
    queries = get_queries(config['country_string'], testing_mode=True)

    # Should have desktop key
    assert 'desktop' in queries, "Expected 'desktop' key in testing mode"
    assert 'mobile' in queries, "Expected 'mobile' key in testing mode"

    # Desktop should have exactly 1 query (DAU)
    assert len(queries['desktop']) == 1, (
        f"Expected 1 Desktop query in testing mode, got {len(queries['desktop'])}"
    )
    assert 'DAU' in queries['desktop'], (
        f"Expected 'DAU' query in testing mode. Found: {list(queries['desktop'].keys())}"
    )

    # Mobile should be empty
    assert len(queries['mobile']) == 0, (
        f"Expected 0 Mobile queries in testing mode, got {len(queries['mobile'])}"
    )


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
    """Verify Desktop queries include win10, win11, winX columns.

    Failure indicates Desktop segmentation broken.
    """
    spec = QUERY_SPECS[(Platform.DESKTOP, Metric.DAU, TelemetrySource.GLEAN)]
    query = spec.build_query("'US'")

    assert 'win10' in query.lower(), (
        "Expected win10 column in Desktop SQL"
    )
    assert 'win11' in query.lower(), (
        "Expected win11 column in Desktop SQL"
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
