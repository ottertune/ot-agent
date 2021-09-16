"""Arguments for pytest"""


def pytest_addoption(parser) -> None:  # type: ignore
    """arguments for pytest"""
    parser.addoption("--db_type", default="mysql")

    # mysql database connection options
    parser.addoption("--mysql_user", default="root")
    parser.addoption("--mysql_password", default="")
    parser.addoption("--mysql_host", default="127.0.0.1")
    parser.addoption("--mysql_port", default="3306")
    parser.addoption("--mysql_database", default="")

    # postgres database connection options
    parser.addoption("--pg_user", default="postgres")
    parser.addoption("--pg_password", default="")
    parser.addoption("--pg_host", default="127.0.0.1")
    parser.addoption("--pg_port", default="5432")
    parser.addoption("--pg_database", default="")


def pytest_generate_tests(metafunc) -> None:  # type: ignore
    """
    This is called for every test. Only get/set command line arguments
    if the argument is specified in the list of test "fixturenames".
    """
    if "db_type" in metafunc.fixturenames:
        metafunc.parametrize("db_type", [metafunc.config.option.db_type])

    # mysql database connection options
    if "mysql_user" in metafunc.fixturenames:
        metafunc.parametrize("mysql_user", [metafunc.config.option.mysql_user])
    if "mysql_password" in metafunc.fixturenames:
        metafunc.parametrize("mysql_password", [metafunc.config.option.mysql_password])
    if "mysql_host" in metafunc.fixturenames:
        metafunc.parametrize("mysql_host", [metafunc.config.option.mysql_host])
    if "mysql_port" in metafunc.fixturenames:
        metafunc.parametrize("mysql_port", [metafunc.config.option.mysql_port])
    if "mysql_database" in metafunc.fixturenames:
        metafunc.parametrize("mysql_database", [metafunc.config.option.mysql_database])

    # postgres database connection options
    if "pg_user" in metafunc.fixturenames:
        metafunc.parametrize("pg_user", [metafunc.config.option.pg_user])
    if "pg_password" in metafunc.fixturenames:
        metafunc.parametrize("pg_password", [metafunc.config.option.pg_password])
    if "pg_host" in metafunc.fixturenames:
        metafunc.parametrize("pg_host", [metafunc.config.option.pg_host])
    if "pg_port" in metafunc.fixturenames:
        metafunc.parametrize("pg_port", [metafunc.config.option.pg_port])
    if "pg_database" in metafunc.fixturenames:
        metafunc.parametrize("pg_database", [metafunc.config.option.pg_database])
