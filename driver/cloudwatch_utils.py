"""Util functions for collecting data from cloudwatch"""

import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
from mypy_boto3_cloudwatch.client import CloudWatchClient
# from driver_lib.aws import AwsWrapper, get_sts_credentials
from driver.exceptions import CloudWatchException

# TODO make cloudwatch work locally
def cloudwatch_collector(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    preparations = _prepare_for_cloudwatch(driver_conf)
    return _get_metrics_from_cloudwatch(**preparations)


def _prepare_for_cloudwatch(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare db_id, client, and list of metrics info so that we
    can collect data from cloudwatch"""
    # TODO(nkaviratna) reimplement this in agent friendly manner
    raise NotImplementedError
    # TODO(bohan): [OTT-312]change arn to role_arn when the frontend is supported
    # tokens = [
    #     driver_conf["aws_credentials"]["arn"],
    #     driver_conf["aws_credentials"]["external_id"],
    #     driver_conf["rds_identifier"],
    # ]
    # try:
    #     # todo get these into agent
    #     role_arn, external_id, rds_identifier = "None", "None", "None"
    #     creds = get_sts_credentials(role_arn, external_id, AwsWrapper.sts_client())
    # except Exception as ex:
    #     msg = "Failed to get encrypted data / credentials"
    #     raise DriverConfigException(msg, ex) from ex

    # preparations: Dict[str, Any] = {}
    # preparations["db_identifier"] = rds_identifier
    # preparations["client"] = AwsWrapper.cloudwatch_client(
    #     access_key_id=creds["AccessKeyId"],
    #     secret_access_key=creds["SecretAccessKey"],
    #     region_name=driver_conf["db_region"],
    #     session_token=creds["SessionToken"],
    # )
    # preparations["metrics_to_retrieve"] = driver_conf[
    #     "metrics_to_retrieve_from_source"
    # ]["cloudwatch"]
    # preparations["now_time"] = datetime.utcnow()
    # return preparations


def _get_metrics_from_cloudwatch(
    db_identifier: str,
    client: CloudWatchClient,
    metrics_to_retrieve: List[str],
    now_time: datetime,
    query_window_in_seconds: int = 600,
) -> Dict[str, Any]:
    """Get metric data from cloudwatch.
    Args:
        db_identifier: the identifier of the db instance
        client: cloudwatch client which could issue cloudwatch queries
        metrics_to_retrieve: a list of metric names that we use to collect data
        now_time: the latest timestamp based on which the data will be collected
        query_window_in_seconds: the time window allowed for metric data points. The
                                 larger it is, the more data points it will return.
                                 Currently default to 600 seconds to make sure that
                                 we can get data for all valid metrics.
    Returns:
        A dict of metric_name: metric_value pairs
    """
    ret = {}
    queries = []
    for metric in metrics_to_retrieve:
        queries.append(
            {
                "Id": f"id_{metric}",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/RDS",
                        "MetricName": metric,
                        "Dimensions": [
                            {"Name": "DBInstanceIdentifier", "Value": db_identifier},
                        ],
                    },
                    "Period": 60,
                    "Stat": "Average",
                },
            },
        )
    try:
        response = client.get_metric_data(
            MetricDataQueries=queries,
            StartTime=now_time - timedelta(seconds=query_window_in_seconds),
            EndTime=now_time,
            # Returns newest data first
            ScanBy="TimestampDescending",
        )
    except Exception as ex:
        msg = f"Failed to collect metrics from cloudwatch, metrics list={metrics_to_retrieve}"
        raise CloudWatchException(msg, ex) from ex

    for result in response["MetricDataResults"]:
        metric_name = result["Id"][3:]
        if result["Values"]:
            # We need to get the metric name here. Since name was not included in the response,
            # in queries we use "id_" + metric_name as the metric Id so that in the response
            # we can extract the name by discarding the first three chars.
            # Also, we get the newest data here
            ret[metric_name] = result["Values"][0]
        else:
            logging.warning("Unable to collect metric %s from cloudwatch", metric_name)
    return ret
