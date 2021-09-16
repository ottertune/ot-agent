"""Driver exceptions"""
import traceback
from typing import Optional


class DriverException(Exception):
    """Generic exception of OtterTune driver"""

    def __init__(self, message: str, ex: Optional[Exception] = None) -> None:
        self.message = message
        if ex:
            self.message += f", caused by {type(ex)}: {ex}"
        super().__init__(self.message)

    def chained_traceback_str(self) -> str:
        """A string of chained tracebacks"""

        chained_tb = traceback.format_exception(
            self.__class__, self, self.__traceback__
        )
        chained_tb_str = "".join(chained_tb)
        return chained_tb_str


class DriverConfigException(DriverException):
    """Exception thrown when driver configuration file/directory not valid"""


class DbCollectorException(DriverException):
    """Exception thrown when database collector failed"""


class MysqlCollectorException(DbCollectorException):
    """Exception thrown when mysql database collector failed"""


class PostgresCollectorException(DbCollectorException):
    """Exception thrown when postgres database collector failed"""


class ComputeServerException(DriverException):
    """Exception thrown when external computer server failed"""


class ComputeServerClientException(DriverException):
    """Exception thrown when computer server client failed"""


class MetricSourceException(DriverException):
    """Exception thrown when getting metric data from a source failed"""


class CloudWatchException(MetricSourceException):
    """Exception thrown when calling cloudwatch failed"""
