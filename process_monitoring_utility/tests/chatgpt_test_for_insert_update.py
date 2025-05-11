import pytest
from unittest.mock import MagicMock, patch
from your_module import ProcessMonitoring  # Replace with actual module name


# Mock job arguments
mock_job_args = {
    "job_run_details_table_name": "job_run_details",
    "job_id": "job123",
    "run_id": "",
    "run_date": "2025-05-11",
    "run_start_time": "2025-05-11 10:00:00",
    "run_row_count": 100,
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


# Test insert_update_job_run_status for INSERT operation (when run_id is empty)
def test_insert_job_run_status(mock_lc, mock_common_utils, process_monitoring):
    # Mock validate_function_param to pass without actual validation
    process_monitoring.common_utils.validate_function_param = MagicMock()

    # Mock get_current_time to return a fixed time
    mock_common_utils.get_current_time.return_value = "2025-05-11 10:30:00"

    # Mock execute_query_and_get_results to avoid actual database calls
    process_monitoring.execute_query_and_get_results = MagicMock()

    # Call the method with some status and comments
    process_monitoring.insert_update_job_run_status("SUCCESS", "Job completed successfully.")

    # Ensure validate_function_param was called to validate parameters
    process_monitoring.common_utils.validate_function_param.assert_called_once()

    # Ensure execute_query_and_get_results was called with the correct query
    query_to_execute = (
        "INSERT INTO job_run_details "
        "(job_id, _created, _modified, run_start_time, run_end_time, run_date, run_row_count, run_status, run_error_detail) "
        "VALUES(:job_id, CAST(:_created AS DATETIME), CAST(:_modified AS DATETIME), CAST(:run_start_time AS DATETIME), "
        "CAST(:run_end_time AS DATETIME), CAST(:run_date AS DATE), :run_row_count, :run_status, :run_error_detail)"
    )
    param_dict = {
        "job_id": "job123",
        "_created": "2025-05-11 10:30:00",
        "_modified": "2025-05-11 10:30:00",
        "run_start_time": "2025-05-11 10:00:00",
        "run_end_time": "2025-05-11 10:30:00",
        "run_date": "2025-05-11",
        "run_row_count": 100,
        "run_status": "SUCCESS",
        "run_error_detail": "Job completed successfully."
    }

    process_monitoring.execute_query_and_get_results.assert_called_once_with(query_to_execute, param_dict=param_dict, fetch_results=False)


# Test insert_update_job_run_status for UPDATE operation (when run_id is not empty)
def test_update_job_run_status(mock_lc, mock_common_utils, process_monitoring):
    # Set run_id to simulate an existing job run
    mock_job_args["run_id"] = "run123"

    # Mock validate_function_param to pass without actual validation
    process_monitoring.common_utils.validate_function_param = MagicMock()

    # Mock get_current_time to return a fixed time
    mock_common_utils.get_current_time.return_value = "2025-05-11 10:30:00"

    # Mock execute_query_and_get_results to avoid actual database calls
    process_monitoring.execute_query_and_get_results = MagicMock()

    # Call the method with some status and comments
    process_monitoring.insert_update_job_run_status("FAILED", "Job failed due to error.")

    # Ensure validate_function_param was called to validate parameters
    process_monitoring.common_utils.validate_function_param.assert_called_once()

    # Ensure execute_query_and_get_results was called with the correct query
    query_to_execute = (
        "UPDATE job_run_details SET _modified=CAST(:_modified AS DATETIME), run_end_time=CAST(:run_end_time AS DATETIME), "
        "run_row_count=:run_row_count, run_status=:run_status, run_error_detail=:run_error_detail "
        "WHERE run_id = :run_id AND job_id = :job_id"
    )
    param_dict = {
        "_modified": "2025-05-11 10:30:00",
        "run_end_time": "2025-05-11 10:30:00",
        "run_row_count": 100,
        "run_status": "FAILED",
        "run_error_detail": "Job failed due to error.",
        "run_id": "run123",
        "job_id": "job123"
    }

    process_monitoring.execute_query_and_get_results.assert_called_once_with(query_to_execute, param_dict=param_dict, fetch_results=False)


# Test get_and_set_run_id is called after INSERT operation
def test_get_and_set_run_id_called(mock_lc, mock_common_utils, process_monitoring):
    # Mock the get_and_set_run_id method
    process_monitoring.get_and_set_run_id = MagicMock()

    # Set run_id to empty to trigger INSERT
    mock_job_args["run_id"] = ""

    # Mock validate_function_param to pass without actual validation
    process_monitoring.common_utils.validate_function_param = MagicMock()

    # Mock get_current_time to return a fixed time
    mock_common_utils.get_current_time.return_value = "2025-05-11 10:30:00"

    # Mock execute_query_and_get_results to avoid actual database calls
    process_monitoring.execute_query_and_get_results = MagicMock()

    # Call the method with some status and comments
    process_monitoring.insert_update_job_run_status("SUCCESS", "Job completed successfully.")

    # Ensure that get_and_set_run_id was called after the INSERT operation
    process_monitoring.get_and_set_run_id.assert_called_once()


# Test validate_function_param for missing arguments
def test_validate_function_param_missing_args(mock_lc, mock_common_utils, process_monitoring):
    # Set run_id to empty to trigger INSERT
    mock_job_args["run_id"] = ""

    # Mock validate_function_param to raise an exception for missing parameters
    process_monitoring.common_utils.validate_function_param = MagicMock(side_effect=ValueError("Missing parameter"))

    # Call the method and ensure ValueError is raised
    with pytest.raises(ValueError, match="Missing parameter"):
        process_monitoring.insert_update_job_run_status("SUCCESS", "Job completed successfully.")

#
# Explanation:
# Fixtures:
#
# mock_lc: Mocks the logger object used in the ProcessMonitoring class.
#
# mock_common_utils: Mocks the common_utils used for helper functions like get_current_time.
#
# process_monitoring: Instantiates the ProcessMonitoring class with mocked objects.
#
# Test Methods:
#
# test_insert_job_run_status: Simulates the INSERT case where run_id is empty. The query for inserting data is tested, ensuring validate_function_param and execute_query_and_get_results are called correctly.
#
# test_update_job_run_status: Simulates the UPDATE case where run_id is not empty. The query for updating data is tested in a similar way.
#
# test_get_and_set_run_id_called: Ensures that get_and_set_run_id is called after the INSERT operation.
#
# test_validate_function_param_missing_args: Tests that the ValueError is raised if validation fails due to missing arguments.
#
# Mocking:
#
# validate_function_param: Mocks the validation logic for input parameters.
#
# get_current_time: Mocks the retrieval of the current time.
#
# execute_query_and_get_results: Mocks the database query execution to avoid actual database interactions.
#
# get_and_set_run_id: Mocks the function that would normally set a run_id after an INSERT.