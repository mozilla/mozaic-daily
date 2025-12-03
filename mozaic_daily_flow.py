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
        image="registry.hub.docker.com/brwells78094/mozaic-daily:v0.0.5_amd64", 
        cpu=1,
        memory=16384
    )
    @step
    def load(self):
        """

        """
        print('load')
        print('This flow is using docker image: "registry.hub.docker.com/brwells78094/mozaic-daily:v0.0.5_amd64"')

        import mozaic_daily
        import pandas as pd

        df = mozaic_daily.main(project="moz-fx-mfouterbounds-prod-f98d")
        pd.set_option('display.max_columns', None)	
        print(df.tail(10))

        # from mozaic_daily import TOKEN
        # print(TOKEN)

        # forecast_date_dt = (datetime.datetime.today()-datetime.timedelta(days=3))
        # forecast_start_date = forecast_date_dt.strftime("%Y-%m-%d")

        # from google.cloud import bigquery

        # def desktop_query(x, y, table, countries, windows_version_column, where):
        #     return (f"""
        #     SELECT {x} AS x,
        #            IF(country IN ({countries}), country, 'ROW') AS country,
        #            IFNULL(LOWER({windows_version_column}) LIKE '%windows 10%', FALSE) AS win10,
        #            IFNULL(LOWER({windows_version_column}) LIKE '%windows 11%', FALSE) AS win11,
        #            IFNULL(LOWER({windows_version_column}) LIKE '%windows%' AND LOWER({windows_version_column}) NOT LIKE '%windows 10%' AND LOWER({windows_version_column}) NOT LIKE '%windows 11%', FALSE) AS winX,
        #            SUM({y}) AS y,
        #      FROM `{table}`
        #     WHERE {where}
        #     GROUP BY ALL
        #     ORDER BY 1, 2 ASC
        #     LIMIT 20
        #     """)

        # # Trivial query to test perms
        # bq = bigquery.Client(project="moz-fx-mfouterbounds-prod-f98d")
        # df = bq.query(desktop_query(
        #         x="submission_date",
        #         y="dau",
        #         table="moz-fx-data-shared-prod.telemetry.active_users_aggregates",
        #         countries=', '.join(f"'{i}'" for i in set(['US', 'GB', 'FR', 'AU', 'JP', 'PL'])),
        #         windows_version_column="os_version",
        #         where=f'app_name = "Firefox Desktop" AND submission_date >= "{forecast_start_date}"'
        #     )
        # ).to_dataframe()
        # print(df)

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
