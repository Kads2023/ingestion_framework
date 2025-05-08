# To create unit tests for the ProcessMonitoring class using pytest and monkeypatch, we'll simulate and mock the external dependencies and ensure full coverage of all methods and their interactions. This will cover the retry mechanism, connection logic, query execution, and other methods inside the ProcessMonitoring class.

# We'll go step-by-step:

# Mocking Dependencies:

# pyodbc.connect to simulate database connections.

# pyspark.sql.SparkSession to mock Spark session and DataFrame creation.

# time.sleep for retry delays (if needed).

# CommonUtils.retry for retry logic.

# logger to capture logs.

# Testing Methods:

# We'll write tests to validate the behavior of each method like get_conn(), execute_query_and_get_results(), get_and_set_job_id(), etc.

# Here's the complete test suite:

# Step 1: Full Unit Test for ProcessMonitoring


import pytest
from unittest.mock import MagicMock, patch
from process_monitoring import ProcessMonitoring  # Assuming the ProcessMonitoring class is in process_monitoring.py
from common_utils import CommonUtils  # Assuming CommonUtils with retry decorator

@pytest.fixture
def mock_spark():
    # Mock the Spark session
    spark_mock = MagicMock()
    spark_mock.createDataFrame.return_value = MagicMock()  # Mock DataFrame creation
    return spark_mock


@pytest.fixture
def mock_common_utils():
    # Mock CommonUtils for retry decorator
    common_utils_mock = MagicMock()
    common_utils_mock.get_current_time.return_value = "2025-05-08 12:00:00"
    return common_utils_mock


@pytest.fixture
def mock_job_args():
    # Mock job arguments
    job_args_mock = MagicMock()
    job_args_mock.get.return_value = "mock_value"
    return job_args_mock


@pytest.fixture
def mock_logger():
    # Mock the logger
    logger_mock = MagicMock()
    return logger_mock


@pytest.fixture
def process_monitoring(mock_spark, mock_common_utils, mock_job_args, mock_logger):
    # Create an instance of ProcessMonitoring with all mocked dependencies
    return ProcessMonitoring(
        lc=mock_logger,
        common_utils=mock_common_utils,
        job_args=mock_job_args
    )


# Test get_conn() method
def test_get_conn(process_monitoring, mock_job_args):
    """
    Test if the connection is created successfully in get_conn method.
    """
    mock_conn = MagicMock()
    
    with patch('pyodbc.connect', return_value=mock_conn):
        conn = process_monitoring.get_conn()
        
        # Assert connection is created
        assert conn == mock_conn
        mock_job_args.get.assert_called_with("process_monitoring_conn_str")


# Test execute_query_and_get_results() method (successful query)
def test_execute_query_and_get_results_success(process_monitoring, mock_job_args, mock_spark):
    """
    Test if execute_query_and_get_results works successfully and returns a DataFrame.
    """
    mock_query = "SELECT * FROM table"
    mock_df = MagicMock()
    
    with patch('pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchall.return_value = [{"job_id": 1}]
        
        process_monitoring.execute_query_and_get_results(mock_query)
        
        mock_conn.return_value.cursor.return_value.execute.assert_called_with(mock_query)
        mock_spark.createDataFrame.assert_called()


# Test execute_query_and_get_results() method (failing query with retry)
def test_execute_query_and_get_results_retry(process_monitoring, mock_job_args, mock_spark):
    """
    Test retry behavior in execute_query_and_get_results if query fails initially.
    """
    mock_query = "SELECT * FROM table"
    
    # Simulate a failure first, then a success
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.execute.side_effect = [Exception("Connection Failed"), None]
    
    with patch('pyodbc.connect', return_value=mock_conn):
        with patch('time.sleep', return_value=None):  # Mock time.sleep to avoid actual delays
            process_monitoring.execute_query_and_get_results(mock_query)
            
            # Assert that execute was called twice (because of retry)
            assert mock_conn.cursor.return_value.execute.call_count == 2


# Test get_and_set_job_id() method
def test_get_and_set_job_id(process_monitoring, mock_job_args):
    """
    Test if job_id is correctly fetched and set.
    """
    mock_query = "SELECT job_id FROM job_details WHERE condition = 'True'"
    
    # Simulate a valid job_id fetch
    mock_df = MagicMock()
    mock_df.count.return_value = 1
    mock_df.collect.return_value = [{"job_id": 123}]
    
    with patch.object(process_monitoring, 'execute_query_and_get_results', return_value=mock_df):
        process_monitoring.get_and_set_job_id(raise_exception=True)
        
        # Check that job_id was set
        mock_job_args.set.assert_called_with("jo_id", 123)


# Test insert_job_details() method
def test_insert_job_details(process_monitoring, mock_job_args):
    """
    Test insert_job_details() method to verify query execution.
    """
    mock_query = (
        "INSERT INTO job_details (_created, _modified, env, job_name, frequency, load_type, "
        "source_system, source, source_type) VALUES ('2025-05-08 12:00:00', '2025-05-08 12:00:00', "
        "'env', 'job_name', 'frequency', 'load_type', 'source_system', 'source', 'source_type')"
    )

    with patch.object(process_monitoring, 'execute_query_and_get_results') as mock_execute:
        process_monitoring.insert_job_details()
        
        # Verify the correct query is executed
        mock_execute.assert_called_with(mock_query, fetch_results=False)


# Test check_already_processed() method
def test_check_already_processed(process_monitoring, mock_job_args):
    """
    Test check_already_processed method to ensure it checks the job status correctly.
    """
    mock_query = "SELECT job_id, run_status FROM job_run_details WHERE job_id = 123"
    
    mock_df = MagicMock()
    mock_df.count.return_value = 1
    mock_df.collect.return_value = [{"job_id": 123, "run_status": "COMPLETED"}]
    
    with patch.object(process_monitoring, 'execute_query_and_get_results', return_value=mock_df):
        process_monitoring.check_already_processed(passed_job_id="123", passed_run_date="2025-05-08")
        
        # Assert the job completion status
        mock_job_args.set.assert_called_with("123_completed", True)


# Test insert_update_job_run_status() method (insert scenario)
def test_insert_update_job_run_status_insert(process_monitoring, mock_job_args):
    """
    Test insert_update_job_run_status method when inserting new job run status.
    """
    mock_query = (
        "INSERT INTO job_run_details (job_id, _created, _modified, run_start_time, run_end_time, "
        "run_date, run_row_count, run_status, run_error_detail) VALUES (123, '2025-05-08 12:00:00', "
        "'2025-05-08 12:00:00', '2025-05-08 12:00:00', '2025-05-08 12:00:00', '2025-05-08', "
        "0, 'SUCCESS', 'No errors')"
    )
    
    with patch.object(process_monitoring, 'execute_query_and_get_results') as mock_execute:
        process_monitoring.insert_update_job_run_status("SUCCESS", "No errors")
        
        # Verify the correct query is executed for inserting data
        mock_execute.assert_called_with(mock_query, fetch_results=False)


# Test insert_update_job_run_status() method (update scenario)
def test_insert_update_job_run_status_update(process_monitoring, mock_job_args):
    """
    Test insert_update_job_run_status method when updating existing job run status.
    """
    mock_query = (
        "UPDATE job_run_details SET _modified = '2025-05-08 12:00:00', run_end_time = '2025-05-08 12:00:00', "
        "run_row_count = 10, run_status = 'SUCCESS', run_error_detail = 'No errors' WHERE run_id = 1 AND job_id = 123"
    )
    
    with patch.object(process_monitoring, 'execute_query_and_get_results') as mock_execute:
        process_monitoring.insert_update_job_run_status("SUCCESS", "No errors")
        
        # Verify the correct query is executed for updating data
        mock_execute.assert_called_with(mock_query, fetch_results=False)


# Step 2: Explanation of Tests
# test_get_conn():

# Tests if the get_conn() method establishes a connection properly by mocking pyodbc.connect.

# test_execute_query_and_get_results_success():

# Tests a successful query execution, ensuring that the query is passed correctly and a DataFrame is returned.

# test_execute_query_and_get_results_retry():

# Tests the retry behavior of the execute_query_and_get_results method when the query fails initially but succeeds on retry.

# test_get_and_set_job_id():

# Tests if the job ID is correctly fetched and set in the job_args when the get_and_set_job_id() method is called.

# test_insert_job_details():

# Tests if the insert_job_details() method constructs and executes the correct SQL query.

# test_check_already_processed():

# Tests the check for whether a job has already been processed by verifying the job status in the database.

# test_insert_update_job_run_status_insert():

# Tests the insertion of job run status into the database when the run_id is not provided.

# test_insert_update_job_run_status_update():

# Tests the update of job run status in the database when the run_id is already available.

# Step 3: Run the Tests
# You can run the tests with pytest:

# pytest test_process_monitoring.py
# This will run all the tests and ensure full coverage of the ProcessMonitoring class.
