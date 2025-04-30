import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from edap_ingest.base_ingest import BaseIngest  # adjust the import based on your project structure

@pytest.fixture
def mock_dependencies():
    # Create mock objects
    input_args = MagicMock()
    job_args = MagicMock()
    common_utils = MagicMock()
    process_monitoring = MagicMock()
    validation_utils = MagicMock()
    dbutils = MagicMock()

    # Mock methods
    common_utils.check_and_set_dbutils.return_value = dbutils
    common_utils.log_msg.return_value = None
    process_monitoring.insert_update_job_run_status.return_value = None
    process_monitoring.check_and_get_job_id.return_value = None
    process_monitoring.check_already_processed.return_value = None
    common_utils.read_yaml.return_value = {"key": "value"}
    job_args.get.side_effect = lambda key: {
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
        "schema": {
            "column1": {
                "data_type": "string",
                "source_column_name": "col1",
                "derived_column": "False"
            }
        },
        "source_base_location": "/base/",
        "source_reference_location": "ref/",
        "source_folder_date_pattern": "{year}/{month}/{day}/",
        "source_file_name_prefix": "prefix_",
        "source_file_name_date_pattern": "{year}{month}{day}",
        "source_file_extension": ".csv",
        "target_catalog": "catalog",
        "target_schema": "schema",
        "target_table": "table",
        "audit_columns_to_be_added": ["audit_col"],
        "table_columns_to_be_added": ["table_col"],
        "job_id": "1234",
        "1234_completed": False
    }.get(key, None)

    return input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils

@pytest.fixture
def base_ingest(mock_dependencies):
    input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils = mock_dependencies
    return BaseIngest(input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)

# -- Tests start here --

def test_init_sets_attributes(base_ingest, mock_dependencies):
    _, _, common_utils, _, _, _ = mock_dependencies
    common_utils.check_and_set_dbutils.assert_called()

def test_read_and_set_input_args(base_ingest):
    base_ingest.read_and_set_input_args()
    base_ingest.input_args_obj.set_mandatory_input_params.assert_called()
    base_ingest.input_args_obj.set_default_values_for_input_params.assert_called()

def test_read_and_set_common_config(base_ingest):
    base_ingest.read_and_set_common_config()
    base_ingest.common_utils_obj.read_yaml.assert_called()
    base_ingest.job_args_obj.set.assert_called()

def test_read_and_set_table_config(base_ingest):
    base_ingest.read_and_set_table_config()
    base_ingest.common_utils_obj.read_yaml.assert_called()
    base_ingest.job_args_obj.set.assert_called()

def test_exit_without_errors(base_ingest):
    base_ingest.exit_without_errors("Finished successfully")
    base_ingest.process_monitoring_obj.insert_update_job_run_status.assert_called_with("Exited", passed_comments="Finished successfully")
    base_ingest.dbutils.notebook.exit.assert_called_with("Finished successfully")

def test_pre_load_already_processed(monkeypatch, base_ingest):
    # Set already processed to True
    base_ingest.job_args_obj.get.side_effect = lambda key: {
        "common_config_file_location": "/path",
        "table_config_file_location": "/path",
        "job_id": "1234",
        "run_date": "2024-01-01",
        "1234_completed": True
    }.get(key, None)

    monkeypatch.setattr(base_ingest, "exit_without_errors", MagicMock())

    base_ingest.pre_load()
    base_ingest.exit_without_errors.assert_called()

def test_form_schema_from_dict(base_ingest):
    base_ingest.form_schema_from_dict()
    assert base_ingest.job_args_obj.set.called

def test_form_source_and_target_locations(base_ingest):
    base_ingest.form_source_and_target_locations()
    assert base_ingest.job_args_obj.set.call_count >= 2

def test_collate_columns_to_add(base_ingest):
    base_ingest.collate_columns_to_add()
    base_ingest.job_args_obj.set.assert_called_with(
        "columns_to_be_added",
        ["audit_col", "table_col"]
    )

def test_load(base_ingest):
    base_ingest.load()
    assert base_ingest.job_args_obj.set.called

def test_post_load(base_ingest):
    base_ingest.post_load()
    base_ingest.process_monitoring_obj.insert_update_job_run_status.assert_called_with("Completed")

def test_run_load_success(monkeypatch, base_ingest):
    monkeypatch.setattr(base_ingest, "pre_load", MagicMock())
    monkeypatch.setattr(base_ingest, "load", MagicMock())
    monkeypatch.setattr(base_ingest, "post_load", MagicMock())

    base_ingest.run_load()

    base_ingest.pre_load.assert_called()
    base_ingest.load.assert_called()
    base_ingest.post_load.assert_called()

def test_run_load_failure(monkeypatch, base_ingest):
    monkeypatch.setattr(base_ingest, "pre_load", MagicMock(side_effect=Exception("Test error")))
    with pytest.raises(Exception) as e:
        base_ingest.run_load()
    assert "Test error" in str(e.value)
    base_ingest.process_monitoring_obj.insert_update_job_run_status.assert_called_with(
        "Failed",
        passed_comments="Test error".replace('"', '').replace("'", '').replace('SELECT', 'S E L E C T')
    )


# Key features:
# Full coverage for all methods (init, pre_load, load, post_load, run_load, etc.).
#
# monkeypatch for method substitution (exit_without_errors, pre_load, load, post_load) to avoid side-effects.
#
# Mocks (MagicMock) for external objects like input_args_obj, common_utils_obj, dbutils, etc.
#
# Parameterized input indirectly with .side_effect and .get() mocking.
#
# Error simulation for testing run_load failure.
#
# ⚡ Bonus
# If you want, we can also add pytest.mark.parametrize to tests some different input variations for exit_without_errors, collate_columns_to_add, or form_source_and_target_locations.
# Would you like me to extend it even further with parametrize examples too? 🚀
# (Example: Different kinds of audit_columns, table_columns, file names, etc.)