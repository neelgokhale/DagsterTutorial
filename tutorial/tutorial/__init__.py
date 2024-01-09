from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)

from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that will materilize the assets
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# A ScheduleDefinition the job it should run and a cron schedule of run frequency
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 * * * *"
)

defs = Definitions(
    assets=all_assets,
    schedules=[hackernews_schedule]
)
