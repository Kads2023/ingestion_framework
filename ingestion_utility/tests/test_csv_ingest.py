import pytest
from unittest.mock import MagicMock, patch
from edap_ingest.ingest.csv_ingest import CsvIngest

@pytest.fixture
def mock_dependencies():
    input_args = MagicMock()
    job_args = MagicMock()
    common_utils = MagicMock()
    process_monitoring = MagicMock()
    validation_utils = MagicMock()
    dbutils = MagicMock()

    # mock job_args.get default returns and sets
    job_args.get.side_effect = lambda key: {
        "dry_run": False,
        "source_location": "/path/to/source.csv",
        "target_location": "db.schema.table",
        "schema_struct": None,
        "columns_to_be_added": ["col1", "col2"],
    }.get(key, None)

    return input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils

@pytest.fixture
def csv_ingest_instance(mock_dependencies):
    input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils = mock_dependencies
    ingest = CsvIngest(input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
    # Patch the SparkSession inside the instance
    ingest.spark = MagicMock()
    ingest.spark.read.load.return_value.count.return_value = 10
    ingest.spark.read.load.return_value.write.mode.return_value.saveAsTable.return_value = None
    return ingest

@pytest.mark.parametrize("method_name", [
    "read_and_set_input_args",
    "read_and_set_common_config",
    "read_and_set_table_config",
    "pre_load",
    "form_schema_from_dict",
    "form_source_and_target_locations",
    "collate_columns_to_add",
    "post_load",
    "run_load"
])
def test_methods_call_super_and_log(csv_ingest_instance, method_name):
    method = getattr(csv_ingest_instance, method_name)
    method()
    # Check if common_utils_obj.log_msg was called for this method
    assert any(method_name in call_args[0] for call_args in csv_ingest_instance.common_utils_obj.log_msg.call_args_list)

def test_load_with_schema_and_without(monkeypatch, csv_ingest_instance):
    # Case 1: With schema_struct present
    csv_ingest_instance.job_args_obj.get.side_effect = lambda key: {
        "dry_run": False,
        "source_location": "/source/path",
        "target_location": "target.db.table",
        "schema_struct": "fake_schema",
        "columns_to_be_added": ["col1", "col2"],
    }.get(key, None)

    fake_df = MagicMock()
    fake_df.count.return_value = 5
    fake_df.write.mode.return_value.saveAsTable.return_value = None
    # Patch spark.read.load to return a DataFrame with schema method chaining
    mock_load = MagicMock(return_value=fake_df)
    monkeypatch.setattr(csv_ingest_instance.spark.read, "load", mock_load)

    csv_ingest_instance.load()

    mock_load.assert_called_once_with("/source/path", format="csv", header="true")
    fake_df.count.assert_called_once()
    fake_df.write.mode.assert_called_once_with("append")
    fake_df.write.mode.return_value.saveAsTable.assert_called_once_with("target.db.table")

    # Case 2: Without schema_struct (infer schema)
    csv_ingest_instance.job_args_obj.get.side_effect = lambda key: {
        "dry_run": False,
        "source_location": "/source/path2",
        "target_location": "target.db.table2",
        "schema_struct": None,
        "columns_to_be_added": ["colA", "colB"],
    }.get(key, None)

    fake_df2 = MagicMock()
    fake_df2.count.return_value = 8
    fake_df2.write.mode.return_value.saveAsTable.return_value = None

    # Patch load to return fake_df2
    mock_load2 = MagicMock(return_value=fake_df2)
    monkeypatch.setattr(csv_ingest_instance.spark.read, "load", mock_load2)

    csv_ingest_instance.load()

    mock_load2.assert_called_once_with("/source/path2", format="csv", inferSchema="true", header="true")
    fake_df2.count.assert_called_once()
    fake_df2.write.mode.assert_called_once_with("append")
    fake_df2.write.mode.return_value.saveAsTable.assert_called_once_with("target.db.table2")

def test_load_dry_run(monkeypatch, csv_ingest_instance):
    # Setup dry_run = True
    csv_ingest_instance.job_args_obj.get.side_effect = lambda key: {
        "dry_run": True,
        "source_location": "/source/dryrun",
        "target_location": "target.dryrun.table",
        "schema_struct": None,
        "columns_to_be_added": [],
    }.get(key, None)

    fake_df = MagicMock()
    fake_df.count.return_value = 3
    fake_df.write.mode.return_value.saveAsTable.return_value = None
    monkeypatch.setattr(csv_ingest_instance.spark.read, "load", MagicMock(return_value=fake_df))

    csv_ingest_instance.load()

    # Check that saveAsTable was NOT called because dry_run is True
    fake_df.write.mode.return_value.saveAsTable.assert_not_called()

def test_load_raises_exception(monkeypatch, csv_ingest_instance):
    # Patch validation_obj.run_validations to raise an Exception
    csv_ingest_instance.job_args_obj.get.side_effect = lambda key: {
        "dry_run": False,
        "source_location": "/bad/source",
        "target_location": "target.bad.table",
        "schema_struct": None,
        "columns_to_be_added": [],
    }.get(key, None)

    fake_df = MagicMock()
    fake_df.count.return_value = 3
    monkeypatch.setattr(csv_ingest_instance.spark.read, "load", MagicMock(return_value=fake_df))
    csv_ingest_instance.validation_obj.run_validations.side_effect = Exception("Validation failed")

    with pytest.raises(Exception) as excinfo:
        csv_ingest_instance.load()
    assert "Validation failed" in str(excinfo.value)


# Explanation:
# mock_dependencies fixture creates mocks for all dependencies.
#
# csv_ingest_instance fixture creates an instance with mocked Spark session & dependencies.
#
# Parametrized tests checks that all main methods call common_utils.log_msg.
#
# test_load_with_schema_and_without tests both cases of loading CSV (with/without schema).
#
# test_load_dry_run verifies that no write occurs when dry run is enabled.
#
# test_load_raises_exception checks that exceptions in validation bubble up.

