# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import os

from metaflow import (
    FlowSpec,
    IncludeFile,
    Parameter,
    card,
    current,
    step,
    environment,
    kubernetes,
)
from metaflow.cards import Markdown

class MozaicDailyFlow(FlowSpec):
    @card(type="default")
    @step
    def start(self):
        print('start')
        self.next(self.load)

    @card
    @kubernetes(
        image="registry.hub.docker.com/brwells78094/mozaic-daily:v_amd_test"
    )
    @step
    def load(self):
        print('load')

        self.next(self.end)

    @step
    def end(self):
        print(
            f"""
            Flow complete.

            """
        )


if __name__ == "__main__":
    MozaicDailyFlow()
