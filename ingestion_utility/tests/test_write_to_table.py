import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_instance():
    class MockIngest:
        this_class_name = "TestIngest"
        spark = MagicMock()
        job_args_obj = MagicMock()

    return MockIngest()

def test_write_data_to_target_table_success(monkeypatch, mock_instance):
    mock_df = MagicMock()
    mock_df.count.return_value = 100

    mock_instance.job_args_obj.get.return_value = False  # dry_run = False
    mock_instance.job_args_obj.get_mandatory.return_value = "default.test_table"
    mock_instance.spark.catalog.tableExists.return_value = True

    monkeypatch.setattr(mock_df.write, "mode", lambda mode: MagicMock(saveAsTable=lambda tbl: None))

    # Call the method
    mock_instance.write_data_to_target_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_target_table, mock_instance
    )
    mock_instance.write_data_to_target_table(mock_df)

    mock_instance.job_args_obj.set.assert_called_with("run_row_count", 100)

def test_write_data_to_target_table_dry_run(monkeypatch, mock_instance):
    mock_df = MagicMock()
    mock_df.count.return_value = 50

    mock_instance.job_args_obj.get.return_value = True  # dry_run = True
    mock_instance.job_args_obj.get_mandatory.return_value = "default.test_table"
    mock_instance.spark.catalog.tableExists.return_value = True

    monkeypatch.setattr(mock_df.write, "mode", lambda mode: MagicMock(saveAsTable=lambda tbl: None))

    # Call the method
    mock_instance.write_data_to_target_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_target_table, mock_instance
    )
    mock_instance.write_data_to_target_table(mock_df)

    mock_instance.job_args_obj.set.assert_called_with("run_row_count", 50)

def test_write_data_to_target_table_table_not_found(mock_instance):
    mock_df = MagicMock()
    mock_df.count.return_value = 10

    mock_instance.job_args_obj.get.return_value = False
    mock_instance.job_args_obj.get_mandatory.return_value = "nonexistent_table"
    mock_instance.spark.catalog.tableExists.return_value = False

    # Call the method
    mock_instance.write_data_to_target_table = __import__("types").MethodType(
        __import__("your_module").YourClass.write_data_to_target_table, mock_instance
    )
    with pytest.raises(ValueError, match="Target table 'nonexistent_table' not found."):
        mock_instance.write_data_to_target_table(mock_df)
