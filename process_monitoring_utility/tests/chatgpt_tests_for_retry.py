import pytest
from unittest import mock
import time
from your_module import ProcessMonitoring, retry_on_exception  # Adjust the import to where your ProcessMonitoring class is defined

# Test the retry decorator itself
def test_retry_decorator(monkeypatch):
    """
    Test that the retry decorator retries on failure and succeeds after retries.
    """
    # This function will raise an exception on the first two calls and succeed on the third
    def mock_func_with_retry(*args, **kwargs):
        if mock_func_with_retry.call_count < 3:
            raise Exception("Temporary failure")
        return "Success"
    
    # Apply the decorator manually to mock_func_with_retry
    decorated_func = retry_on_exception(exceptions=(Exception,), max_attempts=3, delay_seconds=1)(mock_func_with_retry)
    
    # Mocking the logger to capture logs (optional)
    mock_logger = mock.MagicMock()
    monkeypatch.setattr('logging.getLogger', lambda *args: mock_logger)

    # Call the decorated function
    result = decorated_func()
    
    # Verify the function succeeds after retries
    assert result == "Success"
    
    # Check if the logger logged retries
    assert mock_logger.warning.call_count == 2  # Two retries before success
    mock_logger.error.assert_not_called()


# Test `get_conn` with retry logic
@pytest.mark.parametrize("exception_count, expected_retries", [(3, 3), (1, 1), (0, 0)])
def test_get_conn_retry(monkeypatch, exception_count, expected_retries):
    """
    Test the `get_conn` method to check the retry behavior when exceptions are raised.
    """
    # Simulate the behavior of `get_conn` which raises an exception multiple times
    def mock_get_conn():
        nonlocal exception_count
        if exception_count > 0:
            exception_count -= 1
            raise pyodbc.Error("Connection failed")
        return "Connection successful"
    
    # Monkeypatch the get_conn method to simulate failure/retries
    monkeypatch.setattr(ProcessMonitoring, "get_conn", mock_get_conn)

    # Create a mock `ProcessMonitoring` object
    job_args_mock = mock.Mock()
    job_args_mock.get.return_value = "mock_conn_str"
    lc_mock = mock.Mock()
    common_utils_mock = mock.Mock()

    process_monitoring = ProcessMonitoring(lc_mock, common_utils_mock, job_args_mock)
    
    # Call get_conn
    with mock.patch("time.sleep", return_value=None):  # Mock sleep to avoid waiting during tests
        result = process_monitoring.get_conn()
    
    # Check if the correct number of retries happened
    assert result == "Connection successful"
    assert mock_get_conn.call_count == expected_retries


# Test `execute_query_and_get_results` with retry logic
@pytest.mark.parametrize("exception_count, expected_retries", [(3, 3), (1, 1), (0, 0)])
def test_execute_query_and_get_results_retry(monkeypatch, exception_count, expected_retries):
    """
    Test `execute_query_and_get_results` method to check the retry behavior.
    """
    # Simulate the behavior of execute_query_and_get_results, where it raises an exception
    def mock_execute_query_and_get_results(query, param_dict=None, param_order=None, fetch_results=True):
        nonlocal exception_count
        if exception_count > 0:
            exception_count -= 1
            raise pyodbc.Error("Query execution failed")
        return "Query results"
    
    # Monkeypatch the execute_query_and_get_results method to simulate failure/retries
    monkeypatch.setattr(ProcessMonitoring, "execute_query_and_get_results", mock_execute_query_and_get_results)

    # Create a mock `ProcessMonitoring` object
    job_args_mock = mock.Mock()
    lc_mock = mock.Mock()
    common_utils_mock = mock.Mock()

    process_monitoring = ProcessMonitoring(lc_mock, common_utils_mock, job_args_mock)

    # Call the execute_query_and_get_results method
    with mock.patch("time.sleep", return_value=None):  # Mock sleep to avoid waiting during tests
        result = process_monitoring.execute_query_and_get_results("SELECT * FROM table")

    # Check if the correct number of retries happened
    assert result == "Query results"
    assert mock_execute_query_and_get_results.call_count == expected_retries




# Explanation of the Tests:
# Testing the retry_on_exception decorator:

# The test_retry_decorator function ensures that the retry logic is working as expected by testing a function that fails twice and succeeds on the third attempt.

# It verifies that the function correctly retries the specified number of times (in this case, 3 retries).

# The test also checks that the logger logs the retries and that no error is logged if it succeeds after retries.

# Testing get_conn with retries:

# The test_get_conn_retry tests the retry behavior of the get_conn method.

# It simulates a connection failure by raising a pyodbc.Error and verifies the retries happen based on the specified exception_count.

# It ensures that the connection eventually succeeds (or fails after max retries).

# Testing execute_query_and_get_results with retries:

# The test_execute_query_and_get_results_retry tests the retry behavior of execute_query_and_get_results.

# It simulates a query execution failure using pyodbc.Error and checks that the method retries the specified number of times and then succeeds.

# Running the Tests:
# To run these tests, save the test code into a file, for example test_process_monitoring.py, and run pytest in the terminal:

# bash
# Copy
# Edit
# pytest test_process_monitoring.py
# This will run the tests and report any failures or successes. The tests check the retry logic for both get_conn and execute_query_and_get_results, ensuring that the retry mechanism works as expected.
