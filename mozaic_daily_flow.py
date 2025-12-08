# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import os, datetime

from metaflow import (
    FlowSpec,
    IncludeFile,
    Parameter,
    card,
    current,
    step,
    environment,
    kubernetes,
    schedule,
)
from metaflow.cards import Markdown

IMAGE = "registry.hub.docker.com/brwells78094/mozaic-daily:v0.0.5_amd64"

#from bq_utilities import *

@schedule(cron='0 7 * * ? *')
class MozaicDailyFlow(FlowSpec):
    """
    This flow runs standard forecasts every day
    """

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
    # You can uncomment and adjust this decorator to scale your flow remotely with a custom image.
    # Note: the image parameter must be a fully qualified registry path otherwise Metaflow will default to
    # the AWS public registry.
    # The image referenced HERE is the mozmlops demo image,
    # which has both the dependencies you need run this template flow:
    # scikit-learn (for the specific model called in this demo) and mozmlops (for all your ops tools).
    # Check https://docs.metaflow.org/api/step-decorators/kubernetes for details on @kubernetes decorator
    @kubernetes(
        image=IMAGE, 
        cpu=1,
        memory=16384
    )
    @step
    def load(self):
        """

        """
        print('load')
        print(f'This flow is using docker image: "{IMAGE}"')

        import mozaic_daily
        import pandas as pd
        from google.cloud import bigquery

        project = "moz-fx-mfouterbounds-prod-f98d"

        df = mozaic_daily.main(project=project)
        pd.set_option('display.max_columns', None)	
        print(df.tail(10))

        write_table = 'moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v1'

        mozaic_daily.validate_df_against_table(
            df = df,
            table_id = write_table,
            project = project
        )

        client = bigquery.Client(project)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = client.load_table_from_dataframe(df, write_table, job_config=job_config)
        result = load_job.result()

        table = client.get_table(write_table)
        print(
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
