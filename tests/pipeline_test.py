"""Tests for the pipeline module"""

# pyre-ignore-all-errors[16]
from apscheduler.schedulers.background import BlockingScheduler
import mock

from driver.pipeline import DB_LEVEL_MONITOR_JOB_ID, TABLE_LEVEL_MONITOR_JOB_ID, SCHEMA_MONITOR_JOB_ID


import driver.pipeline as module_under_test

# pylint: disable=missing-function-docstring
# pylint: disable=unused-argument
# pylint: disable=attribute-defined-outside-init
# pylint: disable=protected-access


class MockConfig:
    """Quick mock for testing pipeline code"""

    def __init__(self) -> None:
        self.database_uid = "test_database_uid"
        self.server_url = "test_server_url"
        self.tokenizer_url = "test_tokenizer_url"
        self.monitor_interval = 1
        self.tune_interval = 3
        self.enable_restart = True
        self.db_provider = "amazon"
        self.enable_tuning = True
        self.table_level_monitor_interval = 3600
        self.schema_monitor_interval = 3600


@mock.patch("driver.pipeline.driver_pipeline")
def test_schedule_or_update_job_cloud(pipeline_function_patch: mock.Mock) -> None:
    scheduler = BlockingScheduler()
    config = MockConfig()
    config.db_provider = "amazon"

    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, DB_LEVEL_MONITOR_JOB_ID)
    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, TABLE_LEVEL_MONITOR_JOB_ID)


@mock.patch("driver.pipeline.driver_pipeline")
def test_schedule_or_update_job_same_job_twice(
    pipeline_function_patch: mock.Mock,
) -> None:
    scheduler = BlockingScheduler()
    config = MockConfig()
    config.db_provider = "amazon"

    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, DB_LEVEL_MONITOR_JOB_ID)
    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, DB_LEVEL_MONITOR_JOB_ID)

    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, TABLE_LEVEL_MONITOR_JOB_ID)
    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, TABLE_LEVEL_MONITOR_JOB_ID)
    module_under_test.schedule_or_update_job(scheduler, config, SCHEMA_MONITOR_JOB_ID)