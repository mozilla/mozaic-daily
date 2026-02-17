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

IMAGE = "registry.hub.docker.com/brwells78094/mozaic-daily:v0.0.9_amd64"

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

        """
        print('load')
        if not LOCAL_MODE:
            print(f'This flow is using remote docker image: "{IMAGE}"')

        import sys
        import os
        sys.path.insert(0, '/src')
        sys.path.insert(1, os.path.join(os.getcwd(), '/src'))
        from mozaic_daily import main, validate_output_dataframe, get_git_commit_hash
        import pandas as pd
        from google.cloud import bigquery

        print(f'We are using code hash: {get_git_commit_hash()}')

        project = "moz-fx-mfouterbounds-prod-f98d"

        print ('Generating forecasts')
        df = main(project=project, forecast_start_date=self.forecast_start_date)
        pd.set_option('display.max_columns', None)	
        print(df.tail(10))

        print('Done\n\nValidating forecasts')
        validate_output_dataframe(df)

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
