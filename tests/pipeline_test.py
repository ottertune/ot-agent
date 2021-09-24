"""Tests for the pipeline module"""

# pyre-ignore-all-errors[16]
from apscheduler.schedulers.background import BlockingScheduler
import mock

from driver.pipeline import MONITOR_JOB_ID


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


@mock.patch("driver.pipeline.driver_pipeline_for")
def test_schedule_or_update_job_cloud(pipeline_function_patch: mock.Mock) -> None:
    scheduler = BlockingScheduler()
    config = MockConfig()
    config.db_provider = "amazon"

    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, MONITOR_JOB_ID)


@mock.patch("driver.pipeline.driver_pipeline_for")
def test_schedule_or_update_job_same_job_twice(
    pipeline_function_patch: mock.Mock,
) -> None:
    scheduler = BlockingScheduler()
    config = MockConfig()
    config.db_provider = "amazon"

    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, MONITOR_JOB_ID)
    # pyre-ignore - mocks used
    module_under_test.schedule_or_update_job(scheduler, config, MONITOR_JOB_ID)
