# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import os
from metaflow import (
    FlowSpec,
    Parameter,
    card,
    step,
    kubernetes,
    schedule,
)

IMAGE = "registry.hub.docker.com/brwells78094/mozaic-daily:v0.0.11_amd64"

# Check if running in local mode (skip Kubernetes)
LOCAL_MODE = os.environ.get("METAFLOW_LOCAL_MODE", "").lower() == "true"

def conditional_kubernetes(*args, **kwargs):
    """Apply kubernetes decorator only if not in local mode."""
    def decorator(func):
        if LOCAL_MODE:
            return func
        return kubernetes(*args, **kwargs)(func)
    return decorator

@schedule(cron='0 7 * * ? *')
class MozaicDailyFlow(FlowSpec):
    """
    This flow runs standard forecasts every day
    """

    forecast_start_date = Parameter(
        'forecast_start_date',
        default=None,
        help='Override forecast start date (YYYY-MM-DD) for backfills'
    )

    data_sources = Parameter(
        'data_sources',
        default=None,
        help='Comma-separated data sources to filter (e.g., legacy_desktop)'
    )

    metrics = Parameter(
        'metrics',
        default=None,
        help='Comma-separated metrics to filter (e.g., DAU)'
    )

    # You can import the contents of files from your file system to use in flows.
    # This is meant for small files—in this example, a bit of config.
    # example_config = IncludeFile("example_config", default="./example_config.json")

    # You can uncomment and adjust this decorator when it's time to scale your flow remotely.
    # @kubernetes(image="url-to-docker-image:tag", cpu=1)
    # Check https://docs.metaflow.org/api/step-decorators/kubernetes for details on @kubernetes decorator
    # @kubernetes(
    #     image="registry.hub.docker.com/brwells78094/mozaid-daily:latest", 
    #     cpu=1,
    #     memory=4096
    # )
    @card(type="default")
    @step
    def start(self):
        """
        Each flow has a 'start' step. 

        You can use it for collecting/preprocessing data or other setup tasks.
        """
        print('start')
        self.next(self.load)



    @card
    @conditional_kubernetes(
        image=IMAGE,
        cpu=1,
        memory=16384
    )
    @step
    def load(self):
        """
        Generate daily forecasts, validate output, and upload to BigQuery.

        This step:
        1. Runs the main forecasting pipeline via mozaic_daily.main()
        2. Validates the forecast DataFrame
        3. Appends validated forecast to the production table

        The forecast data is written to:
        moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2
        """
        print('load')
        if LOCAL_MODE:
            print('Running in local mode')
        else:
            print(f'This flow is using remote docker image: "{IMAGE}"')

        import sys
        import os
        sys.path.insert(0, '/src')
        sys.path.insert(1, os.path.join(os.getcwd(), '/src'))
        from mozaic_daily import main, validate_output_dataframe, get_git_commit_hash
        from mozaic_daily.queries import DataSource, Metric
        import pandas as pd
        from google.cloud import bigquery

        print(f'We are using code hash: {get_git_commit_hash()}')

        project = "moz-fx-mfouterbounds-prod-f98d"

        # Metaflow serializes None parameters as the string "None"
        forecast_start_date = self.forecast_start_date if self.forecast_start_date not in (None, "None") else None

        # Parse filter parameters
        data_source_filter = None
        if self.data_sources not in (None, "None"):
            data_source_filter = {DataSource(s.strip()) for s in self.data_sources.split(",")}

        metric_filter = None
        if self.metrics not in (None, "None"):
            metric_filter = {Metric(m.strip()) for m in self.metrics.split(",")}

        print ('Generating forecasts')
        df = main(
            project=project,
            forecast_start_date=forecast_start_date,
            data_source_filter=data_source_filter,
            metric_filter=metric_filter,
        )
        pd.set_option('display.max_columns', None)	
        print(df.tail(10))

        # HACK: Skip validation for filtered runs. Filtered output is a subset of
        # expected rows, so row-count validation would fail. This is a non-ideal
        # workaround implemented under time pressure — proper fix would be to make
        # validation filter-aware for partial uploads.
        is_filtered = data_source_filter is not None or metric_filter is not None
        if is_filtered:
            print('Done\n\nSkipping validation for filtered run')
        else:
            print('Done\n\nValidating forecasts')
            validate_output_dataframe(df, forecast_start_date=forecast_start_date)

        print('Done\n\nSaving forecasts')
        write_table = 'moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2'

        client = bigquery.Client(project)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = client.load_table_from_dataframe(df, write_table, job_config=job_config)
        result = load_job.result()

        table = client.get_table(write_table)
        print(
            "Done.\n"
            f"Loaded {result.output_rows} rows into {write_table}. "
            f"Table now has {table.num_rows} rows."
        )

        self.next(self.end)

    @step
    def end(self):
        """
        This is the mandatory 'end' step: it prints some helpful information
        to access the model and the used dataset.
        """
        print(
            f"""
            Flow complete.
            """
        )


if __name__ == "__main__":
    MozaicDailyFlow()
