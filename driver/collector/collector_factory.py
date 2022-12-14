"""Driver collector methods"""

from contextlib import contextmanager
from typing import Dict, Any, Generator
import os

from mysql.connector.constants import ClientFlag  # for SSL
import mysql.connector
import mysql.connector.connection as mysql_conn
import psycopg2

from driver.collector.base_collector import BaseDbCollector
from driver.exceptions import (
    DbCollectorException,
    DriverConfigException,
    PostgresCollectorException,
    MysqlCollectorException,
)
from driver.aws.rds import (
    get_db_auth_token,
)
from driver.collector.mysql_collector import MysqlCollector
from driver.collector.postgres_collector import PostgresCollector
from driver.aws.wrapper import AwsWrapper


def get_db_password(driver_conf: Dict[str, Any]) -> str:
    if driver_conf.get("enable_aws_iam_auth"):
        rds_client = AwsWrapper.rds_client(driver_conf["aws_region"])
        return get_db_auth_token(
            driver_conf["db_user"],
            driver_conf["db_host"],
            driver_conf["db_port"],
            rds_client,
        )
    return driver_conf["db_password"]


def create_db_config_mysql(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the driver configuration to the MySQL database configuration

    Args:
        driver_conf: Dict of the driver configuration
    Returns:
        Database configuration for connecting to the target database
    Raises:
        DriverConfigException: invalid database configuration
    """
    try:
        conf = {
            "host": driver_conf["db_host"],
            "port": driver_conf["db_port"],
            "user": driver_conf["db_user"],
            "password": get_db_password(driver_conf),
            "charset": "utf8",
        }
    except Exception as ex:
        msg = "Invalid MySQL database configuration: parameter is not defined"
        raise DriverConfigException(msg, ex) from ex

    if driver_conf.get("db_name"):
        conf["database"] = driver_conf["db_name"]
    else:
        conf[
            "database"
        ] = "information_schema"  # connect to information_schema by default
    if driver_conf.get("db_enable_ssl"):
        conf["client_flags"] = [ClientFlag.SSL]
        if driver_conf.get("db_ssl_ca"):
            conf["ssl_ca"] = driver_conf["db_ssl_ca"]
        if driver_conf.get("db_ssl_cert"):
            conf["ssl_cert"] = driver_conf["db_ssl_cert"]
        if driver_conf.get("db_ssl_key"):
            conf["ssl_key"] = driver_conf["db_ssl_key"]

    # All mysql connection configuration parameters:
    # https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
    if driver_conf.get("db_conf_extend"):
        conf_extend = driver_conf["db_conf_extend"]
        if not isinstance(conf_extend, dict):
            msg = (
                "Invalid MySQL database configuration: db_conf_extend type "
                f"{type(conf_extend)} is not dictionary"
            )
            raise DriverConfigException(msg)
        extend_params = set(conf_extend.keys())
        driver_params = set(driver_conf.keys())
        duplicate_params = extend_params.intersection(driver_params)
        if duplicate_params:
            msg = (
                "Invalid MySQL database configuration: duplicate parameters "
                f"{duplicate_params} in db_conf_extend"
            )
            raise DriverConfigException(msg)

        conf.update(conf_extend)

    return conf


def create_db_config_postgres(driver_conf: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the driver configuration to the Postgres database configuration

    Args:
        driver_conf: Dict of the driver configuration
    Returns:
        Database configuration for connecting to the target database
    Raises:
        DriverConfigException: invalid database configuration
    """
    try:
        conf = {
            "host": driver_conf["db_host"],
            "port": driver_conf["db_port"],
            "user": driver_conf["db_user"],
            "password": get_db_password(driver_conf),
        }
    except Exception as ex:
        msg = "Invalid Postgres database configuration: parameter is not defined"
        raise DriverConfigException(msg, ex) from ex

    if driver_conf.get("db_name"):
        conf["dbname"] = driver_conf["db_name"]
    else:
        conf["dbname"] = "postgres"  # connect to postgres database by default
    if driver_conf.get("db_enable_ssl"):
        conf["sslmode"] = "require"
        if driver_conf.get("db_ssl_ca"):
            conf["sslrootcert"] = driver_conf["db_ssl_ca"]
        if driver_conf.get("db_ssl_cert"):
            conf["sslcert"] = driver_conf["db_ssl_cert"]
        if driver_conf.get("db_ssl_key"):
            conf["sslkey"] = driver_conf["db_ssl_key"]

    # All postgres connection configuration parameters:
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    if driver_conf.get("db_conf_extend"):
        conf_extend = driver_conf["db_conf_extend"]
        if not isinstance(conf_extend, dict):
            msg = (
                "Invalid Postgres database configuration: db_conf_extend type "
                f"{type(conf_extend)} is not dictionary"
            )
            raise DriverConfigException(msg)
        extend_params = set(conf_extend.keys())
        driver_params = set(driver_conf.keys())
        duplicate_params = extend_params.intersection(driver_params)
        if duplicate_params:
            msg = (
                "Invalid Postgres database configuration: duplicate parameters "
                f"{duplicate_params} in db_conf_extend"
            )
            raise DriverConfigException(msg)
        conf.update(conf_extend)

    return conf


def connect_mysql(mysql_conf: Dict[str, Any]) -> mysql.connector.MySQLConnection:
    """
    Connects to target mysql database
    Args:
        mysql_conf: configuration for mysql connection
    Returns:
        mysql connection
    Raises:
        MysqlCollectorException: unable to query DB for version
    """
    try:
        return mysql.connector.connect(**mysql_conf, autocommit=True)
    except mysql.connector.Error as ex:
        raise MysqlCollectorException("Failed to connect to MySQL", ex) from ex


def connect_postgres(postgres_conf: Dict[str, Any]):
    """
    Connects to target postres database
    Args:
        postgres_conf: configuration for postgres connection
    Returns:
        postgres connection
    Raises:
        PostgresCollectorException: unable to query DB for version
    """
    try:
        conn = psycopg2.connect(**postgres_conf)
        conn.autocommit = True
        return conn
    except psycopg2.Error as ex:
        raise PostgresCollectorException("Failed to connect to Postgres", ex) from ex


def get_mysql_version(conn: mysql_conn.MySQLConnection) -> str:
    """
    Returns the version number from mysql (e.g. 5.7.34)
    Args:
        conn: the mysql connection
    Raises:
        MysqlCollectorException: unable to query DB for version
    """
    try:
        cursor = conn.cursor(dictionary=False)
        cursor.execute("SELECT VERSION();")
        res = cursor.fetchall()
        version = res[0][0].split("-")[0]
        return version
    except mysql.connector.Error as ex:
        raise MysqlCollectorException("Failed to get MySQL version", ex) from ex


def get_postgres_version(conn) -> str:
    """
    Args:
        conn: The postgres connection
    Raises:
        PostgresCollectorException: unable to query DB for version
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW server_version;")
        res = cursor.fetchall()
        version = res[0][0].split()[0]
        return version
    except psycopg2.Error as ex:
        raise PostgresCollectorException("Failed to get Postgres version") from ex


@contextmanager
def get_collector(
    driver_conf: Dict[str, Any],
) -> Generator[BaseDbCollector, None, None]:
    """Get the database collector according to database type

    Callers should use in a "with" block to ensure connection object is closed.

    e.g.    with get_collector(driver_conf) as collector:
                ...

    Args:
        driver_conf: Dict of the driver configuration
    Returns:
        Database collector that is responsible for getting data from the target database
    Raises:
        DriverConfigException: invalid database configuration
        DbCollectorException: database type is not supported
        MysqlCollectorException: unable to connect to MySQL database or get version
        PostgresCollectorException: unable to connect to Postgres database or get version
    """
    try:
        conn = None
        conns: Dict[str, Any] = {}

        # wrap test code together here. long term we will want to refactor to instead have all the
        # code that calls externalities able to be redirected to mock endpoints outside container in
        # a way that the code inside can't tell the difference.
        should_stub_collector = (
            os.environ.get("STUB_COLLECTOR", "false").lower() == "true"
        )
        if driver_conf["db_type"] == "mock" or should_stub_collector:
            from tests.mocks.mock_collector import (  # pylint: disable=import-outside-toplevel
                MockCollector,
            )

            collector = MockCollector()

        elif driver_conf["db_type"] in ["mysql", "aurora_mysql"]:
            mysql_conf = create_db_config_mysql(driver_conf)
            conn = connect_mysql(mysql_conf)
            version = get_mysql_version(conn)
            collector = MysqlCollector(conn, version)
        elif driver_conf["db_type"] in ["postgres", "aurora_postgresql"]:
            pg_conf = create_db_config_postgres(driver_conf)
            conns: Dict[str, Any] = {}

            db_names = [x.strip() for x in pg_conf["dbname"].split(',')]
            for logical_database in db_names:
                pg_conf_logical = pg_conf.copy()
                pg_conf_logical["dbname"] = logical_database
                conns[logical_database] = connect_postgres(pg_conf_logical)
            main_db = db_names[0]
            version = get_postgres_version(conns[main_db])
            collector = PostgresCollector(conns, main_db, version)
        else:
            error_message = (
                f"Database type {driver_conf['db_type']} is not supported in driver"
            )
            raise DbCollectorException(error_message)

        yield collector
    finally:
        if conn:
            conn.close()
        if conns:
            for _conn in conns.values():
                _conn.close()
