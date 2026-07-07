# -*- coding: utf-8 -*-
"""Main orchestration for mozaic-daily forecasting pipeline.

This module ties together all components to run the full forecast pipeline:
1. Load configuration and constants
2. Fetch data from BigQuery (with checkpoint support)
3. Generate forecasts using Mozaic
4. Format output for BigQuery upload
5. Return validated DataFrame

Functions:
- main(): Entry point for forecast generation

Usage:
    python -m mozaic_daily.main
"""

from typing import Optional, Set
import glob
import json
import pandas as pd
from pathlib import Path
import os
from joblib.externals import cloudpickle
from .adjustments import (
    add_lift_to_forecast,
    add_marketing_lift_to_forecast,
    compute_country_shares,
    compute_fenix_country_shares,
    load_lift_series,
    load_marketing_lift_series,
    load_marketing_spec,
    load_overlay_spec,
    subtract_lift_from_training,
    subtract_marketing_lift_from_training,
)
from .config import get_runtime_config, STATIC_CONFIG, build_filter_code
from .data import get_queries, get_aggregate_data, check_training_data_availability
from .forecast import get_desktop_forecast_dfs, get_mobile_forecast_dfs
from .tables import (
    combine_tables, update_desktop_format, update_mobile_format,
    format_output_table
)
from .queries import Platform, Metric, DataSource, ADDITIONAL_HOLIDAYS


# =============================================================================
# CONSTANTS
# =============================================================================

DATA_SOURCES_TO_PROCESS = [
    DataSource.GLEAN_DESKTOP,
    DataSource.LEGACY_DESKTOP,
    DataSource.GLEAN_MOBILE,
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def print_filter_banner(
    data_source_filter: Optional[Set[DataSource]],
    metric_filter: Optional[Set[Metric]]
):
    """Print a banner showing active filters."""
    width = 60
    char = '='
    print('\n' + char * width)
    print('FILTERED MODE ENABLED')
    if data_source_filter is not None:
        sources = ', '.join(sorted(ds.value for ds in data_source_filter))
        print(f'  Data sources: {sources}')
    if metric_filter is not None:
        metrics = ', '.join(sorted(m.value for m in metric_filter))
        print(f'  Metrics: {metrics}')
    print(char * width + '\n')


def get_format_function(platform: Platform):
    """Return the appropriate format function for a platform."""
    if platform == Platform.DESKTOP:
        return update_desktop_format
    return update_mobile_format


def get_forecast_function(platform: Platform):
    """Return the appropriate forecast function for a platform."""
    if platform == Platform.DESKTOP:
        return get_desktop_forecast_dfs
    return get_mobile_forecast_dfs


def get_mozaic_objects_filename(
    forecast_start_date: str,
    data_source: DataSource,
    output_dir: str = ".",
) -> str:
    """Return the path for saving mozaic objects for a given data source run."""
    filename = f'mozaic_objects.{data_source.value}.{forecast_start_date}.pkl'
    return os.path.join(output_dir, filename)


def save_mozaic_objects(mozaics: dict, filename: str) -> None:
    """Pickle the mozaics dict (metric -> Mozaic) to disk."""
    with open(filename, 'wb') as f:
        cloudpickle.dump(mozaics, f)
    print(f'Saved mozaic objects to {filename}')


def get_checkpoint_filename(
    forecast_start_date: str,
    output_dir: str = ".",
    data_source_filter: Optional[Set[DataSource]] = None,
    metric_filter: Optional[Set[Metric]] = None,
) -> str:
    """Return appropriate checkpoint filename based on filters and output directory."""
    filter_code = build_filter_code(data_source_filter, metric_filter)
    if filter_code:
        filename = STATIC_CONFIG['forecast_checkpoint_filename_filtered_template'].format(
            date=forecast_start_date, filter_code=filter_code
        )
    else:
        filename = STATIC_CONFIG['forecast_checkpoint_filename_template'].format(date=forecast_start_date)
    return os.path.join(output_dir, filename)


def load_checkpoint_if_exists(filename: str) -> Optional[pd.DataFrame]:
    """Load checkpoint if file exists, return None otherwise."""
    if os.path.exists(filename):
        print('Forecast already generated. Loading existing data.')
        return pd.read_parquet(filename)
    return None


def save_checkpoint(df: pd.DataFrame, filename: str) -> None:
    """Save DataFrame to checkpoint file."""
    df.to_parquet(filename)


def _find_marketing_spec_for_forecast(forecast_start_date: str) -> Optional[Path]:
    """Locate the marketing.json spec whose applies_to_forecast_start matches.

    Globs ``data-official/*/marketing/marketing.json`` (relative to repo root) and
    returns the spec whose ``applies_to_forecast_start`` equals
    ``forecast_start_date``. Returns ``None`` if no spec matches — this is the
    "no marketing-lift adjustment for this forecast cycle" path and is not an
    error.
    """
    repo_root = Path(__file__).resolve().parents[2]
    candidates = sorted(
        glob.glob(str(repo_root / "data-official" / "*" / "marketing" / "marketing.json"))
    )
    matches = []
    for candidate in candidates:
        with open(candidate) as f:
            spec = json.load(f)
        if spec.get("applies_to_forecast_start") == forecast_start_date:
            matches.append(Path(candidate))
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"Multiple marketing.json specs claim applies_to_forecast_start="
            f"{forecast_start_date!r}: {[str(m) for m in matches]}"
        )
    return matches[0]


def _apply_marketing_lift_pre_mozaic(
    source_data: dict,
    spec_path: Path,
    training_end_date: str,
) -> tuple[dict, dict]:
    """Subtract marketing-lift from the DAU training data before mozaic runs.

    Returns ``(modified_source_data, marketing_context)`` where
    ``marketing_context`` carries ``{daily_lift_series, country_shares, spec}``
    so the symmetric add-back can use byte-identical inputs.
    """
    spec = load_marketing_spec(spec_path)
    daily_lift_series = load_marketing_lift_series(spec, spec_path.parent)
    metric_key = Metric.DAU.value
    training_df = source_data[metric_key]
    country_shares = compute_fenix_country_shares(
        training_df,
        training_end_date=pd.Timestamp(training_end_date),
        window_days=spec["allocation"]["window_days"],
        app_flag_column=spec["allocation"]["app_flag_column"],
    )
    print(
        f'Marketing-lift: subtracting from {metric_key} training data '
        f'({len(country_shares)} countries, {(training_df[spec["allocation"]["app_flag_column"]] == True).sum()} fenix rows)'  # noqa: E712
    )
    modified_source_data = dict(source_data)
    modified_source_data[metric_key] = subtract_marketing_lift_from_training(
        training_df,
        daily_lift_series=daily_lift_series,
        country_shares=country_shares,
        app_flag_column=spec["allocation"]["app_flag_column"],
    )
    return modified_source_data, {
        "daily_lift_series": daily_lift_series,
        "country_shares": country_shares,
        "spec": spec,
    }


def _find_launch_on_login_spec_for_forecast(forecast_start_date: str) -> Optional[Path]:
    """Locate the launch_on_login/lol.json spec whose applies_to_forecast_start matches.

    Mirrors ``_find_marketing_spec_for_forecast`` for the desktop LOL overlay (code
    ``l``). Returns ``None`` if no spec matches — the "no LOL overlay for this cycle"
    path, which is not an error.
    """
    repo_root = Path(__file__).resolve().parents[2]
    candidates = sorted(
        glob.glob(str(repo_root / "data-official" / "*" / "launch_on_login" / "lol.json"))
    )
    matches = []
    for candidate in candidates:
        with open(candidate) as f:
            spec = json.load(f)
        if spec.get("applies_to_forecast_start") == forecast_start_date:
            matches.append(Path(candidate))
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"Multiple launch_on_login/lol.json specs claim applies_to_forecast_start="
            f"{forecast_start_date!r}: {[str(m) for m in matches]}"
        )
    return matches[0]


def _apply_launch_on_login_pre_mozaic(
    source_data: dict,
    spec_path: Path,
    training_end_date: str,
) -> tuple[dict, dict]:
    """Subtract the LOL desktop tailwind from DAU training before mozaic runs.

    Structurally identical to ``_apply_marketing_lift_pre_mozaic`` but for the
    desktop ``desktop_overlay`` spec: allocation keys off the boolean
    ``modern_windows`` segment column instead of ``app_flag_column``.

    Returns ``(modified_source_data, lol_context)`` where ``lol_context`` carries
    ``{daily_lift_series, country_shares, spec}`` so the symmetric add-back uses
    byte-identical inputs.
    """
    spec = load_overlay_spec(spec_path)
    daily_lift_series = load_lift_series(spec, spec_path.parent)
    flag_column = spec["allocation"]["flag_column"]
    metric_key = Metric.DAU.value
    training_df = source_data[metric_key]
    country_shares = compute_country_shares(
        training_df,
        training_end_date=pd.Timestamp(training_end_date),
        window_days=spec["allocation"]["window_days"],
        flag_column=flag_column,
    )
    print(
        f'Launch-on-login: subtracting from {metric_key} training data '
        f'({len(country_shares)} countries, {(training_df[flag_column] == True).sum()} {flag_column} rows)'  # noqa: E712
    )
    modified_source_data = dict(source_data)
    modified_source_data[metric_key] = subtract_lift_from_training(
        training_df,
        daily_lift_series=daily_lift_series,
        country_shares=country_shares,
        flag_column=flag_column,
        sentinel_attr="launch_on_login_subtracted",
    )
    return modified_source_data, {
        "daily_lift_series": daily_lift_series,
        "country_shares": country_shares,
        "spec": spec,
    }


def process_data_source(
    data_source: DataSource,
    datasets: dict,
    forecast_start: str,
    forecast_end: str,
    training_end_date: Optional[str] = None,
    marketing_spec_path: Optional[Path] = None,
    lol_spec_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Process a single data source through the forecast pipeline.

    Args:
        data_source: DataSource enum identifying which data to process
        datasets: Nested dict of data by platform/source/metric
        forecast_start: Start date for forecast period
        forecast_end: End date for forecast period
        training_end_date: Last date of training data (used by the bidirectional
            overlay allocation-key windows). Required when ``marketing_spec_path``
            or ``lol_spec_path`` is set.
        marketing_spec_path: If set and ``data_source == GLEAN_MOBILE``, applies
            the marketing-lift `m` adjustment: subtracts lift from the DAU
            training rows before mozaic and adds it back to the per-tile
            forecast after mozaic. No-op for other data sources.
        lol_spec_path: If set and ``data_source == LEGACY_DESKTOP``, applies the
            launch-on-login `l` desktop overlay: subtracts the measured LOL rise
            from the modern_windows DAU training rows before mozaic and adds the
            capped curve back to the per-tile forecast after mozaic. No-op for
            other data sources.

    Returns:
        Tuple of (DataFrame with forecasts, dict of metric -> fitted Mozaic objects)
    """
    # Get platform-specific data and functions
    platform = data_source.platform
    source = data_source.telemetry_source
    source_data = datasets[platform.value][source.value]

    # Normalize the date column to datetime64. BigQuery DATE columns arrive as the `dbdate`
    # extension dtype; mozaic's built-in gap fill (Iran 2026) carries datetime64 dates. mozaic
    # normalizes x in its pivot as of commit 97c971c, so this is now defensive (and guards against
    # an older mozaic where a dbdate-vs-datetime64 mismatch silently NaN-ed every tile's fit series).
    source_data = {
        metric: (df.assign(x=pd.to_datetime(df["x"]))
                 if isinstance(df, pd.DataFrame) and "x" in df.columns else df)
        for metric, df in source_data.items()
    }

    marketing_context = None
    if marketing_spec_path is not None and data_source == DataSource.GLEAN_MOBILE \
            and Metric.DAU.value in source_data:
        if training_end_date is None:
            raise ValueError(
                "training_end_date is required when marketing_spec_path is set"
            )
        source_data, marketing_context = _apply_marketing_lift_pre_mozaic(
            source_data, marketing_spec_path, training_end_date
        )

    lol_context = None
    if lol_spec_path is not None and data_source == DataSource.LEGACY_DESKTOP \
            and Metric.DAU.value in source_data:
        if training_end_date is None:
            raise ValueError(
                "training_end_date is required when lol_spec_path is set"
            )
        source_data, lol_context = _apply_launch_on_login_pre_mozaic(
            source_data, lol_spec_path, training_end_date
        )

    # Generate forecasts
    forecast_func = get_forecast_function(platform)
    additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])
    forecast_result = forecast_func(
        source_data, forecast_start, forecast_end,
        additional_holidays=additional_holidays,
        data_source=data_source.value,
    )

    # Combine
    df_combined = combine_tables(forecast_result.dfs)

    # Marketing-lift add-back (before format_func so the population column still exists).
    # Operates on every row where the lift series has a non-zero value — that
    # includes the post-campaign-launch training rows we subtracted from, so the
    # training→forecast transition stays coherent for downstream rolling stats.
    if marketing_context is not None and Metric.DAU.value in df_combined.columns:
        df_combined = add_marketing_lift_to_forecast(
            df_combined,
            daily_lift_series=marketing_context["daily_lift_series"],
            country_shares=marketing_context["country_shares"],
            forecast_start=pd.Timestamp(forecast_start),
            metric_column=Metric.DAU.value,
            app_population_value=marketing_context["spec"]["allocation"]["app_flag_column"],
        )
        n_total = len(df_combined)
        n_forecast = (df_combined["source"] == "forecast").sum()
        print(f'Marketing-lift: added back across {n_total} rows '
              f'({n_forecast} forecast + {n_total - n_forecast} training/actual)')

    # Launch-on-login add-back (same timing/rationale as marketing-lift above).
    if lol_context is not None and Metric.DAU.value in df_combined.columns:
        df_combined = add_lift_to_forecast(
            df_combined,
            daily_lift_series=lol_context["daily_lift_series"],
            country_shares=lol_context["country_shares"],
            forecast_start=pd.Timestamp(forecast_start),
            metric_column=Metric.DAU.value,
            population_value=lol_context["spec"]["allocation"]["flag_column"],
        )
        n_total = len(df_combined)
        n_forecast = (df_combined["source"] == "forecast").sum()
        print(f'Launch-on-login: added back across {n_total} rows '
              f'({n_forecast} forecast + {n_total - n_forecast} training/actual)')

    # Format
    format_func = get_format_function(platform)
    format_func(df_combined, data_source=data_source.value)

    return df_combined, forecast_result.mozaics


def generate_forecasts(
    datasets: dict,
    runtime_config: dict,
    data_source_filter: Optional[Set[DataSource]] = None,
    checkpoints: bool = False,
    output_dir: str = ".",
    marketing_spec_path: Optional[Path] = None,
    lol_spec_path: Optional[Path] = None,
) -> pd.DataFrame:
    """Generate forecasts for all data sources and combine them.

    Args:
        datasets: Nested dict of data by platform/source/metric
        runtime_config: Runtime configuration with dates
        data_source_filter: If set, only process these data sources
        checkpoints: If True, save fitted Mozaic objects to disk alongside other checkpoints
        output_dir: Directory for checkpoint files
        marketing_spec_path: If set, the marketing-lift `m` adjustment is applied
            to glean_mobile DAU (subtract pre-mozaic, add back post-mozaic).
            Other data sources are unaffected.
        lol_spec_path: If set, the launch-on-login `l` desktop overlay is applied
            to legacy_desktop DAU (subtract pre-mozaic, add back post-mozaic).
            Other data sources are unaffected.

    Returns:
        Combined DataFrame with all forecasts
    """
    all_dfs = []

    sources_to_process = [
        ds for ds in DATA_SOURCES_TO_PROCESS
        if data_source_filter is None or ds in data_source_filter
    ]
    total_sources = len(sources_to_process)

    for source_num, data_source in enumerate(sources_to_process, start=1):
        print(f'\n[{source_num}/{total_sources}] Forecasting {data_source.display_name}')

        df, mozaics = process_data_source(
            data_source,
            datasets,
            runtime_config['forecast_start_date'],
            runtime_config['forecast_end_date'],
            training_end_date=runtime_config['training_end_date'],
            marketing_spec_path=marketing_spec_path,
            lol_spec_path=lol_spec_path,
        )
        all_dfs.append(df)

        if checkpoints:
            mozaic_filename = get_mozaic_objects_filename(
                runtime_config['forecast_start_date'], data_source, output_dir
            )
            save_mozaic_objects(mozaics, mozaic_filename)

    print('\n\nDone with forecasts')

    # Combine all data sources and format for output
    df = pd.concat(all_dfs, ignore_index=True)
    df = format_output_table(
        df,
        runtime_config['forecast_start_date'],
        runtime_config['forecast_run_dt']
    )

    return df


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main(
    project: Optional[str] = None,
    checkpoints: bool = False,
    clean: bool = False,
    data_source_filter: Optional[Set[DataSource]] = None,
    metric_filter: Optional[Set[Metric]] = None,
    forecast_start_date: Optional[str] = None,
    output_dir: Optional[str] = None,
    marketing_lift: bool = True,
    launch_on_login: bool = True,
) -> pd.DataFrame:
    """Run the full forecasting pipeline.

    Args:
        project: GCP project ID for BigQuery (defaults to config value)
        checkpoints: Enable file-based checkpointing for faster iteration
        clean: Ignore existing checkpoints (re-query and re-forecast) but still
            save new ones. Useful when iterating on model changes.
        data_source_filter: If set, only process these data sources (e.g., {DataSource.GLEAN_MOBILE})
        metric_filter: If set, only process these metrics (e.g., {Metric.DAU})
        forecast_start_date: Override date (YYYY-MM-DD) for historical forecast runs.
            Simulates running the forecast on this date.
        output_dir: Directory to write checkpoint files to (defaults to current directory).
            Created automatically if it doesn't exist.
        marketing_lift: If True (default), look for a marketing-lift spec
            matching this forecast cycle's start date and apply the `m`
            adjustment to glean_mobile DAU. If False, force the adjustment off
            even if a matching spec exists.
        launch_on_login: If True (default), look for a launch-on-login spec
            matching this forecast cycle's start date and apply the `l`
            desktop overlay to legacy_desktop DAU. If False, force it off even
            if a matching spec exists.

    Returns:
        DataFrame with forecasts
    """
    # Resolve output directory and create it if needed
    resolved_output_dir = output_dir if output_dir is not None else "."
    os.makedirs(resolved_output_dir, exist_ok=True)

    # Load configuration with optional date override
    config = get_runtime_config(forecast_start_date_override=forecast_start_date)
    if not project:
        project = STATIC_CONFIG['default_project']

    is_filtered = data_source_filter is not None or metric_filter is not None
    if is_filtered:
        print_filter_banner(data_source_filter, metric_filter)

    print(f'Running forecast from {config["forecast_start_date"]} through {config["forecast_end_date"]}')
    print(f'Other config:\n{config}')

    # Set up checkpointing
    checkpoint_filename = get_checkpoint_filename(
        config['forecast_start_date'], resolved_output_dir,
        data_source_filter=data_source_filter, metric_filter=metric_filter
    )

    # Run pre-flight data availability check unless forecast checkpoint already exists.
    # Skipping when the checkpoint exists avoids unnecessary BQ calls during iteration.
    load_checkpoints = checkpoints and not clean
    forecast_checkpoint_exists = load_checkpoints and os.path.exists(checkpoint_filename)
    if not forecast_checkpoint_exists:
        check_training_data_availability(project, config['training_end_date'])

    # Fetch data from BigQuery (with internal checkpointing)
    datasets = get_aggregate_data(
        get_queries(
            config['country_string'],
            data_source_filter=data_source_filter,
            metric_filter=metric_filter,
        ),
        project,
        checkpoints=checkpoints,
        clean=clean,
        output_dir=resolved_output_dir
    )

    # Load checkpoint OR generate forecasts
    df = None
    if load_checkpoints:
        df = load_checkpoint_if_exists(checkpoint_filename)

    if df is None:
        marketing_spec_path = (
            _find_marketing_spec_for_forecast(config['forecast_start_date'])
            if marketing_lift else None
        )
        if marketing_lift and marketing_spec_path is None:
            print(
                f'Marketing-lift: no spec found for forecast_start='
                f'{config["forecast_start_date"]}; adjustment disabled for this cycle.'
            )
        elif marketing_spec_path is not None:
            print(f'Marketing-lift: using spec {marketing_spec_path}')

        lol_spec_path = (
            _find_launch_on_login_spec_for_forecast(config['forecast_start_date'])
            if launch_on_login else None
        )
        if launch_on_login and lol_spec_path is None:
            print(
                f'Launch-on-login: no spec found for forecast_start='
                f'{config["forecast_start_date"]}; overlay disabled for this cycle.'
            )
        elif lol_spec_path is not None:
            print(f'Launch-on-login: using spec {lol_spec_path}')

        df = generate_forecasts(
            datasets, config,
            data_source_filter=data_source_filter,
            checkpoints=checkpoints,
            output_dir=resolved_output_dir,
            marketing_spec_path=marketing_spec_path,
            lol_spec_path=lol_spec_path,
        )
        save_checkpoint(df, checkpoint_filename)

    # Return result
    return df


if __name__ == '__main__':
    main(checkpoints=True)
