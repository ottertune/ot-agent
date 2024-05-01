"""Tests for cloudwatch utils"""
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest.mock import MagicMock
import pytest
from driver.aws.cloudwatch import _get_metrics_from_cloudwatch

# pylint: disable=missing-class-docstring, invalid-name
# pylint: disable=missing-function-docstring
UTC_NOW: datetime = datetime.utcnow()
DATA_SAMPLES: Dict[str, Any] = {
    "WriteIOPS": {
        "Id": "id_WriteIOPS",
        "Label": "label",
        "Timestamps": [UTC_NOW],
        "Values": [1234, 2234, 3234],
        "StatusCode": "Complete",
    },
    "CPUUtilization": {
        "Id": "id_CPUUtilization",
        "Label": "label",
        "Timestamps": [UTC_NOW - timedelta(seconds=70)],
        "Values": [5555, 6666, 7777],
        "StatusCode": "Complete",
    },
    "CPUCreditUsage": {
        "Id": "id_CPUCreditUsage",
        "Label": "label",
        "Timestamps": [UTC_NOW - timedelta(seconds=300)],
        "Values": [94041, 94085, 15213],
        "StatusCode": "Complete",
    },
    "ReplicaLag": {
        "Id": "id_ReplicaLag",
        "Label": "label",
        "Timestamps": [UTC_NOW - timedelta(seconds=90)],
        "Values": [7890, 8890, 9890],
        "StatusCode": "Complete",
    },
}


def fake_get_metric_data(
    MetricDataQueries: Dict[str, Any],
    StartTime: datetime,
    EndTime: datetime,
    ScanBy: str,
) -> Dict[str, Any]:
    assert ScanBy == "TimestampDescending"
    ret: Dict[str, Any] = {
        "Messages": [],
        "ResponseMetadata": {
            "RequestId": "c6e231b2-9850-4497-8822-5b50e633582e",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "c6e231b2-9850-4497-8822-5b50e633582e",
                "content-type": "text/xml",
                "content-length": "493",
                "date": "Thu, 17 Jun 2021 01:52:07 GMT",
            },
            "RetryAttempts": 0,
        },
        "MetricDataResults": [],
    }
    for query in MetricDataQueries:
        # pyre-ignore[6]: intentional wrong type for test
        metric = query["MetricStat"]["Metric"]["MetricName"]
        if (
            DATA_SAMPLES[metric]["Timestamps"][0] >= StartTime
            and DATA_SAMPLES[metric]["Timestamps"][0] <= EndTime
        ):
            ret["MetricDataResults"].append(DATA_SAMPLES[metric])
        else:
            ret["MetricDataResults"].append(
                {
                    "Id": f"id_{metric}",
                    "Label": "label",
                    "Timestamps": [],
                    "Values": [],
                    "StatusCode": "Complete",
                }
            )
    return ret


@pytest.fixture(name="mock_cloudwatch_client")
def _mock_cloudwatch_client() -> MagicMock:
    mock_client = MagicMock()
    mock_client.get_metric_data = MagicMock(side_effect=fake_get_metric_data)
    return mock_client


def test_get_metrics(mock_cloudwatch_client: MagicMock) -> None:
    metrics = _get_metrics_from_cloudwatch(
        "db_id",
        "",
        mock_cloudwatch_client,
        ["WriteIOPS", "CPUUtilization", "ReplicaLag", "CPUCreditUsage"],
        [],
        UTC_NOW,
    )

    expected_data = {
        "WriteIOPS": 1234,
        "CPUUtilization": 5555,
        "CPUCreditUsage": 94041,
        "ReplicaLag": 7890,
    }
    assert metrics == expected_data
