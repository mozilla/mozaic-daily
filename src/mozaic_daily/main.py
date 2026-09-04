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

from typing import Iterable, Optional, Set
import pandas as pd
from pathlib import Path
import os
from joblib.externals import cloudpickle
from .organic import (
    add_paid_to_forecast,
    build_share_lookup,
    load_organic_spec,
    load_split_frame,
    marketing_paid_level,
    measured_paid_country_shares,
    paid_seam_step,
    split_training_to_organic,
)
from .adjustments import (
    add_marketing_lift_to_forecast,
    compute_fenix_country_shares,
    load_marketing_lift_series,
    load_marketing_spec,
    subtract_marketing_lift_from_training,
)
from .overlays import (
    ResolvedOverlay,
    add_overlays_post_mozaic,
    find_spec_for_forecast,
    registered_overlay_codes,
    resolve_overlays,
    subtract_overlays_pre_mozaic,
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


def _find_organic_spec_for_forecast(forecast_start_date: str) -> Optional[Path]:
    """Locate the organic/organic.json spec whose applies_to_forecast_start matches.

    Mirrors the other overlay finders for the mobile paid/organic split (code ``p``).
    """
    return find_spec_for_forecast(
        "data-official/*/organic/organic.json", forecast_start_date, "organic/organic.json"
    )


def _apply_organic_split_pre_mozaic(
    source_data: dict,
    spec_path: Path,
    training_end_date: str,
) -> tuple[dict, dict]:
    """Scale Fenix DAU training rows down to their measured organic component.

    Returns ``(modified_source_data, organic_context)`` carrying everything the two-piece
    add-back needs: the measured paid we removed (for training rows), marketing's paid level
    (for forecast rows), the country allocation, and the spec.
    """
    spec = load_organic_spec(spec_path)
    split = load_split_frame(spec, spec_path.parent)
    metric_key = Metric.DAU.value
    training_df = source_data[metric_key]

    flag_column = spec["scope"]["app_flag_column"]
    excluded = spec["scope"].get("exclude_countries", [])
    training_dates = pd.DatetimeIndex(pd.to_datetime(training_df["x"]).unique())
    countries = sorted(set(training_df["country"].unique()) - set(excluded))

    share_lookup = build_share_lookup(
        split,
        share_column=spec["share_column"],
        training_dates=training_dates,
        countries=countries,
    )
    measured_from = pd.Timestamp(spec["share_backfill"]["measured_from"])
    n_backfilled = int((training_dates < measured_from).sum())
    print(
        f'Paid/organic split: {len(countries)} countries, '
        f'{(training_df[flag_column] == True).sum()} fenix rows, '  # noqa: E712
        f'excluding {excluded or "nothing"}; '
        f'{n_backfilled} of {len(training_dates)} training days predate the measured window '
        f'({measured_from.date()}) and use the held-flat earliest share'
    )

    modified_source_data = dict(source_data)
    modified_source_data[metric_key], measured_paid = split_training_to_organic(
        training_df,
        share_lookup=share_lookup,
        flag_column=flag_column,
        exclude_countries=excluded,
    )

    country_shares = measured_paid_country_shares(
        split,
        training_end_date=pd.Timestamp(training_end_date),
        window_days=spec["allocation"]["window_days"],
        exclude_countries=excluded,
        allocation_key=spec["allocation"]["key"],
        dau_training=training_df,
        flag_column=flag_column,
    )
    return modified_source_data, {
        "measured_paid": measured_paid,
        "country_shares": country_shares,
        "spec": spec,
        "spec_dir": spec_path.parent,
    }


def _find_marketing_spec_for_forecast(forecast_start_date: str) -> Optional[Path]:
    """Locate the marketing.json spec whose applies_to_forecast_start matches (retired ``m``).

    Kept on its own path rather than the overlay registry because ``m`` uses the
    ``marketing_lift`` spec type and must keep reproducing July's and August's
    pre-swap artifacts. Returns ``None`` when no spec matches.
    """
    return find_spec_for_forecast(
        "data-official/*/marketing/marketing.json", forecast_start_date, "marketing/marketing.json"
    )


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


def process_data_source(
    data_source: DataSource,
    datasets: dict,
    forecast_start: str,
    forecast_end: str,
    training_end_date: Optional[str] = None,
    marketing_spec_path: Optional[Path] = None,
    overlays: Optional[list[ResolvedOverlay]] = None,
    organic_spec_path: Optional[Path] = None,
    model_configs: Optional[dict] = None,
) -> pd.DataFrame:
    """Process a single data source through the forecast pipeline.

    Args:
        data_source: DataSource enum identifying which data to process
        datasets: Nested dict of data by platform/source/metric
        forecast_start: Start date for forecast period
        forecast_end: End date for forecast period
        training_end_date: Last date of training data (used by the bidirectional
            overlay allocation-key windows). Required when ``marketing_spec_path``
            or ``overlays`` is set.
        overlays: Registry-resolved per-tile overlays (``l``, ``o``, and any code
            registered with ``applier: per_tile_overlay``). Only those whose spec
            names this ``data_source`` in ``applies_to_data_source`` are applied:
            each curve is subtracted from the DAU training rows before mozaic and
            added back to the per-tile forecast after. Overlays stack because each
            carries a distinct idempotency sentinel.
        organic_spec_path: If set and ``data_source == GLEAN_MOBILE``, applies the
            paid/organic split `p` adjustment: scales Fenix DAU training rows down
            to their measured organic component before mozaic, then stacks paid
            back on after (measured paid for training rows, marketing's paid level
            for forecast rows). Mutually exclusive with ``marketing_spec_path``.
            No-op for other data sources.
        model_configs: Optional ``{DataSource: ModelConfig}``. When a config is
            present for this data source it is passed to the forecast function,
            which is what routes the tuned Prophet *and* holiday parameters
            through. Omitting it uses the package's hardcoded defaults — which is
            why a plain ``run_main.py`` run cannot reproduce a tuned build.
        marketing_spec_path: If set and ``data_source == GLEAN_MOBILE``, applies
            the marketing-lift `m` adjustment: subtracts lift from the DAU
            training rows before mozaic and adds it back to the per-tile
            forecast after mozaic. No-op for other data sources.
            RETIRED for mobile as of the 2026-08 cycle — see ``organic_spec_path``.

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

    # `m` and `p` both remove paid DAU from the same Fenix training rows and both add it back.
    # Running them together would subtract twice and add twice — the totals would look plausible
    # while the training rows and the fitted trend were both wrong. Fail loudly instead.
    if marketing_spec_path is not None and organic_spec_path is not None:
        raise ValueError(
            f"Both a marketing-lift spec ({marketing_spec_path}) and a paid/organic-split spec "
            f"({organic_spec_path}) claim this forecast start. They are mutually exclusive: `p` "
            f"supersedes `m` for mobile. Clear `applies_to_forecast_start` in the marketing spec."
        )

    organic_context = None
    if organic_spec_path is not None and data_source == DataSource.GLEAN_MOBILE \
            and Metric.DAU.value in source_data:
        if training_end_date is None:
            raise ValueError(
                "training_end_date is required when organic_spec_path is set"
            )
        source_data, organic_context = _apply_organic_split_pre_mozaic(
            source_data, organic_spec_path, training_end_date
        )

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

    overlay_contexts: list[dict] = []
    overlays_for_source = [o for o in (overlays or []) if o.data_source == data_source]
    if overlays_for_source and Metric.DAU.value in source_data:
        if training_end_date is None:
            raise ValueError("training_end_date is required when overlays are set")
        source_data, overlay_contexts = subtract_overlays_pre_mozaic(
            source_data, overlays_for_source, training_end_date
        )

    # Generate forecasts
    forecast_func = get_forecast_function(platform)
    additional_holidays = ADDITIONAL_HOLIDAYS.get(data_source, [])
    model_config = (model_configs or {}).get(data_source)
    forecast_result = forecast_func(
        source_data, forecast_start, forecast_end,
        additional_holidays=additional_holidays,
        data_source=data_source.value,
        config=model_config,
    )

    # Combine
    df_combined = combine_tables(forecast_result.dfs)

    # Paid add-back (before format_func so the population column still exists).
    #
    # Two regions, deliberately: training rows get back the MEASURED paid we removed, so they
    # return to raw actuals exactly (verify_training_rows_are_actuals.py enforces that, and the
    # canonical notebook's 28-day MA straddles the seam); forecast rows get marketing's paid
    # LEVEL. The two disagree where they meet, and that step is reported rather than smoothed —
    # see research/mobile-organic/paid_seam_methods.ipynb for the open decision.
    if organic_context is not None and Metric.DAU.value in df_combined.columns:
        marketing_paid = marketing_paid_level(
            organic_context["spec"],
            organic_context["spec_dir"],
            forecast_start=pd.Timestamp(forecast_start),
            forecast_end=pd.Timestamp(forecast_end),
        )
        df_combined = add_paid_to_forecast(
            df_combined,
            measured_paid=organic_context["measured_paid"],
            marketing_paid=marketing_paid,
            country_shares=organic_context["country_shares"],
            forecast_start=pd.Timestamp(forecast_start),
            metric_column=Metric.DAU.value,
            population_value=organic_context["spec"]["scope"]["app_flag_column"],
        )
        step = paid_seam_step(
            organic_context["measured_paid"], marketing_paid,
            training_end_date=pd.Timestamp(training_end_date),
        )
        n_total = len(df_combined)
        n_forecast = (df_combined["source"] == "forecast").sum()
        print(f'Paid/organic split: added back across {n_total} rows '
              f'({n_forecast} forecast + {n_total - n_forecast} training/actual)')
        print(f'  seam step at {step["training_end"]}: measured 28d mean '
              f'{step["measured_paid_mean"]:,.0f} -> marketing {step["marketing_paid_mean"]:,.0f} '
              f'= {step["step_abs"]:+,.0f} ({step["step_rel"]:+.2%} of paid)')

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

    # Overlay add-backs (same timing/rationale as marketing-lift above), in code order.
    df_combined = add_overlays_post_mozaic(
        df_combined, overlay_contexts, forecast_start=pd.Timestamp(forecast_start)
    )

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
    overlays: Optional[list[ResolvedOverlay]] = None,
    organic_spec_path: Optional[Path] = None,
    model_configs: Optional[dict] = None,
) -> pd.DataFrame:
    """Generate forecasts for all data sources and combine them.

    Args:
        datasets: Nested dict of data by platform/source/metric
        runtime_config: Runtime configuration with dates
        data_source_filter: If set, only process these data sources
        checkpoints: If True, save fitted Mozaic objects to disk alongside other checkpoints
        output_dir: Directory for checkpoint files
        organic_spec_path: If set, the paid/organic split `p` adjustment is applied
            to glean_mobile DAU. Mutually exclusive with ``marketing_spec_path``.
        model_configs: Optional ``{DataSource: ModelConfig}`` passed through to the
            per-source forecast call. This is the supported way to run a tuned
            build; the param-scan runners use it instead of monkeypatching.
        marketing_spec_path: If set, the marketing-lift `m` adjustment is applied
            to glean_mobile DAU (subtract pre-mozaic, add back post-mozaic).
            Other data sources are unaffected. Retired for mobile as of 2026-08.
        overlays: Registry-resolved per-tile overlays (see ``resolve_overlays``);
            each is applied only to the data source its spec names.

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
            overlays=overlays,
            organic_spec_path=organic_spec_path,
            model_configs=model_configs,
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
# ADJUSTMENT RESOLUTION
# =============================================================================

# Boolean aliases on main() and their adjustment codes.
LEGACY_FLAG_CODES = {
    "marketing_lift": "m",
    "launch_on_login": "l",
    "mozillaonline": "o",
    "organic_split": "p",
}


def _resolve_disabled_adjustments(disabled_adjustments: Optional[Iterable[str]], **legacy_flags: bool) -> set[str]:
    """Union of explicitly disabled codes and the codes whose boolean alias is False."""
    disabled = set(disabled_adjustments or ())
    for flag_name, enabled in legacy_flags.items():
        if not enabled:
            disabled.add(LEGACY_FLAG_CODES[flag_name])
    return disabled


def _resolve_cycle_adjustments(
    forecast_start_date: str, disabled: set[str]
) -> tuple[Optional[Path], Optional[Path], list[ResolvedOverlay]]:
    """Find every adjustment spec that gates on this forecast start and log the outcome.

    Returns ``(marketing_spec_path, organic_spec_path, overlays)``. ``m`` and ``p``
    keep their own loaders; every ``per_tile_overlay`` code comes from the registry.
    """
    marketing_spec_path = (
        _find_marketing_spec_for_forecast(forecast_start_date) if "m" not in disabled else None
    )
    _log_spec_resolution("Marketing-lift `m`", marketing_spec_path, forecast_start_date, "m" in disabled)

    organic_spec_path = (
        _find_organic_spec_for_forecast(forecast_start_date) if "p" not in disabled else None
    )
    _log_spec_resolution("Paid/organic split `p`", organic_spec_path, forecast_start_date, "p" in disabled)

    overlays = resolve_overlays(forecast_start_date, disabled_codes=disabled)
    resolved_by_code = {o.code: o for o in overlays}
    for code, entry in sorted(registered_overlay_codes().items()):
        overlay = resolved_by_code.get(code)
        _log_spec_resolution(
            f"Overlay `{code}` ({entry['name']})",
            overlay.spec_path if overlay else None,
            forecast_start_date,
            code in disabled,
        )
    return marketing_spec_path, organic_spec_path, overlays


def _log_spec_resolution(label: str, spec_path: Optional[Path], forecast_start_date: str, is_disabled: bool) -> None:
    if is_disabled:
        print(f'{label}: disabled for this run by flag.')
    elif spec_path is None:
        print(f'{label}: no spec found for forecast_start={forecast_start_date}; not applied this cycle.')
    else:
        print(f'{label}: using spec {spec_path}')


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
    mozillaonline: bool = True,
    organic_split: bool = True,
    disabled_adjustments: Optional[Iterable[str]] = None,
    model_configs: Optional[dict] = None,
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
        disabled_adjustments: Adjustment codes to force off for this run even
            when a spec matches the forecast start (e.g. ``{"l", "o"}``). This is
            the general switch; the four boolean flags below are aliases kept for
            existing callers and map onto it (``m``, ``l``, ``o``, ``p``).
        marketing_lift: Alias for keeping `m` enabled (default True). False adds
            ``m`` to ``disabled_adjustments``.
        launch_on_login: Alias for keeping `l` enabled (default True).
        mozillaonline: Alias for keeping `o` enabled (default True).
        organic_split: Alias for keeping `p` enabled (default True). Disabling it
            yields a *total*-DAU mobile forecast with no paid treatment at all,
            not an organic one.
        model_configs: Optional ``{DataSource: ModelConfig}`` for tuned runs. The
            CLI does not expose this; the param-scan runners pass it directly.

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
        disabled = _resolve_disabled_adjustments(
            disabled_adjustments,
            marketing_lift=marketing_lift,
            launch_on_login=launch_on_login,
            mozillaonline=mozillaonline,
            organic_split=organic_split,
        )
        marketing_spec_path, organic_spec_path, overlays = _resolve_cycle_adjustments(
            config['forecast_start_date'], disabled
        )

        df = generate_forecasts(
            datasets, config,
            data_source_filter=data_source_filter,
            checkpoints=checkpoints,
            output_dir=resolved_output_dir,
            marketing_spec_path=marketing_spec_path,
            overlays=overlays,
            organic_spec_path=organic_spec_path,
            model_configs=model_configs,
        )
        save_checkpoint(df, checkpoint_filename)

    # Return result
    return df


if __name__ == '__main__':
    main(checkpoints=True)
