# To achieve full test coverage with pytest unit tests and monkeypatching for the given ProcessMonitoring class, we will focus on mocking:
#
# The get_conn method to simulate a database connection without actually establishing one.
#
# The ManagedIdentityCredential to simulate obtaining an Azure AD token.
#
# The pandas.read_sql function for query results.
#
# The retry_helper decorator for retry logic without actually triggering retries.
#
# We will also test various scenarios, including success, failure, dry-run behavior, and query handling.
#

import pytest
from unittest.mock import MagicMock, patch
from your_module import ProcessMonitoring  # Replace with the actual module name
import pyodbc
import pandas as pd
from sqlalchemy.exc import OperationalError
from azure.identity import ManagedIdentityCredential
from urllib.parse import quote

# Mock data
mock_job_args = {
    "process_monitoring_conn_str": "your_connection_string",
    "dry_run": False
}


@pytest.fixture
def mock_lc():
    lc_mock = MagicMock()
    lc_mock.logger = MagicMock()
    return lc_mock


@pytest.fixture
def mock_common_utils():
    return MagicMock()


@pytest.fixture
def process_monitoring(mock_lc, mock_common_utils):
    return ProcessMonitoring(mock_lc, mock_common_utils, mock_job_args)


# Test get_conn method
def test_get_conn(mock_lc, mock_common_utils, process_monitoring):
    # Mock ManagedIdentityCredential to simulate token retrieval
    with patch("azure.identity.ManagedIdentityCredential") as MockCredential:
        mock_credential = MagicMock()
        mock_credential.get_token.return_value.token = "mock_token"
        MockCredential.return_value = mock_credential

        # Mock pyodbc.connect to avoid real DB connection
        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_conn = MagicMock()
            mock_create_engine.return_value = mock_conn

            # Call get_conn
            conn = process_monitoring.get_conn()

            # Ensure ManagedIdentityCredential was used
            MockCredential.assert_called_once()

            # Check that the connection engine was created with the correct parameters
            mock_create_engine.assert_called_once_with(
                "mssql+pyodbc:///?odbc_connect=your_connection_string",
                connect_args={"attrs_before": {1256: struct.pack("<I10s", 10, b"mock_token")}}
            )

            # Assert that the returned connection is the mock connection
            assert conn == mock_conn


# Test execute_query_and_get_results method (normal scenario)
def test_execute_query_and_get_results(mock_lc, mock_common_utils, process_monitoring):
    # Mock the get_conn method to return a mock connection
    mock_conn = MagicMock()
    process_monitoring.get_conn = MagicMock(return_value=mock_conn)

    # Mock pandas.read_sql to simulate query results
    with patch("pandas.read_sql") as mock_read_sql:
        mock_df = pd.DataFrame({"column1": ["row1", "row2"]})
        mock_read_sql.return_value = mock_df

        # Run the method
        query = "SELECT * FROM my_table"
        result_df = process_monitoring.execute_query_and_get_results(query, fetch_results=True)

        # Ensure pandas.read_sql was called with the correct parameters
        mock_read_sql.assert_called_once_with(query, mock_conn)

        # Ensure the result is a Spark DataFrame
        assert result_df is not None
        assert hasattr(result_df, "toPandas")  # Check if it's a Spark DataFrame


# Test execute_query_and_get_results method with dry_run=True
def test_execute_query_with_dry_run(mock_lc, mock_common_utils, process_monitoring):
    # Set dry_run to True
    mock_job_args["dry_run"] = True

    # Mock get_conn to ensure it doesn't get called
    process_monitoring.get_conn = MagicMock(return_value=MagicMock())

    # Run the method
    query = "SELECT * FROM my_table"
    result_df = process_monitoring.execute_query_and_get_results(query, fetch_results=True)

    # Ensure no connection was made
    process_monitoring.get_conn.assert_not_called()

    # Ensure the result is the empty DataFrame
    assert result_df is process_monitoring.empty_df


# Test execute_query_and_get_results method with no results from query
def test_execute_query_with_no_results(mock_lc, mock_common_utils, process_monitoring):
    # Mock the get_conn method to return a mock connection
    mock_conn = MagicMock()
    process_monitoring.get_conn = MagicMock(return_value=mock_conn)

    # Mock pandas.read_sql to simulate an empty result set
    with patch("pandas.read_sql") as mock_read_sql:
        mock_df = pd.DataFrame()  # Empty DataFrame
        mock_read_sql.return_value = mock_df

        # Run the method
        query = "SELECT * FROM my_table"
        result_df = process_monitoring.execute_query_and_get_results(query, fetch_results=True)

        # Ensure pandas.read_sql was called
        mock_read_sql.assert_called_once_with(query, mock_conn)

        # Ensure that the logger was called for empty results
        process_monitoring.lc.logger.info.assert_called_once_with(
            "[ProcessMonitoring.execute_query_and_get_results()] - "
            "passed_query --> SELECT * FROM my_table, pandas_df.empty or pandas_df is None"
        )

        # Ensure the result is still the empty DataFrame
        assert result_df is process_monitoring.empty_df


# Test execute_query_and_get_results method with a failed connection
def test_execute_query_with_failed_connection(mock_lc, mock_common_utils, process_monitoring):
    # Mock get_conn to raise an exception (simulate a failed connection)
    process_monitoring.get_conn = MagicMock(side_effect=OperationalError("Connection failed"))

    # Run the method and check if exception is raised
    with pytest.raises(OperationalError):
        process_monitoring.execute_query_and_get_results("SELECT * FROM my_table", fetch_results=True)


# Test that retry_helper decorator retries on exception
def test_retry_on_exception(mock_lc, mock_common_utils, process_monitoring):
    # Mock the retry_helper to simulate retries
    with patch("your_module.ProcessMonitoring.retry_helper") as mock_retry:
        mock_retry.side_effect = lambda **kwargs: kwargs["func"]()

        # Mock the get_conn method to raise an exception
        process_monitoring.get_conn = MagicMock(side_effect=ConnectionError("Retrying"))

        # Call the method and check if retries are triggered
        with pytest.raises(ConnectionError):
            process_monitoring.execute_query_and_get_results("SELECT * FROM my_table", fetch_results=True)

        # Check that the retry logic is triggered
        assert mock_retry.call_count == 1  # Ensure retry was called once


# Test that invalid queries are properly logged
def test_invalid_query_logging(mock_lc, mock_common_utils, process_monitoring):
    # Mock the get_conn method to return a mock connection
    mock_conn = MagicMock()
    process_monitoring.get_conn = MagicMock(return_value=mock_conn)

    # Test invalid query (not starting with SELECT, INSERT, or UPDATE)
    query = "INVALID QUERY"
    process_monitoring.execute_query_and_get_results(query, fetch_results=False)

    # Ensure that the logger logged the invalid query
    process_monitoring.lc.logger.info.assert_called_with("CANNOT EXECUTE THE QUERY")


# Explanation:
# Fixtures:
#
# mock_lc: Mocks the logger object (lc).
#
# mock_common_utils: Mocks the common utilities object (common_utils).
#
# process_monitoring: Instantiates the ProcessMonitoring class using the mock objects.
#
# Test Methods:
#
# test_get_conn: Mocks the ManagedIdentityCredential and sqlalchemy.create_engine to simulate a secure connection to the database without making a real connection.
#
# test_execute_query_and_get_results: Tests the query execution, including handling of results, and ensures a Spark DataFrame is returned.
#
# test_execute_query_with_dry_run: Verifies that the method does not attempt to connect or run the query when dry_run is set to True.
#
# test_execute_query_with_no_results: Mocks empty query results and checks that the logger is triggered when no results are returned.
#
# test_execute_query_with_failed_connection: Simulates a failed connection using OperationalError and ensures the exception is raised.
#
# test_retry_on_exception: Mocks retry logic to ensure the retry decorator works as expected when an exception is thrown.
#
# test_invalid_query_logging: Verifies that invalid queries (i.e., not starting with SELECT, INSERT, or UPDATE) are logged appropriately.
#
# Mocking:
#
# ManagedIdentityCredential: Simulates token fetching.
#
# sqlalchemy.create_engine: Simulates database connection creation.
#
# pandas.read_sql: Mocks the SQL query execution and result fetching.
#
# retry_helper: Mocks the retry logic to avoid actual retries during tests.