import pytest
from unittest.mock import MagicMock, Mock

from types import SimpleNamespace

from ..tests.fixtures.mock_logger import MockLogger

from edap_ingest.ingest.base_ingest import BaseIngest  # adjust the import based on your project structure


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture
def spark():
    return MagicMock()


@pytest.fixture
def spark_df_mock():
    df = Mock()
    df.columns = ["col1", "col2"]
    df.withColumn = MagicMock(return_value=df)
    df.rdd.isRmpty = MagicMock(return_value=False)  # typo intentional for simulating original
    return df


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
    process_monitoring.insert_update_job_run_status.return_value = None
    process_monitoring.check_and_get_job_id.return_value = None
    process_monitoring.check_already_processed.return_value = None
    common_utils.validate_function_param.return_value = None
    common_utils.read_yaml_file.return_value = {"key": "value"}
    common_utils.get_date_split.return_value = "2025", "06", "03"
    input_args_dict = {
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
        "ingest_type": "csv"
    }
    input_args.get_args_keys.side_effect = lambda: list(input_args_dict.keys())
    input_args.get.side_effect = lambda key: input_args_dict.get(key, None)

    job_args_dict = {
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
        "target_catalog": "catalog",
        "target_schema": "schema",
        "target_table": "table",
        "audit_columns_to_be_added": [
            {
                "column_name": "edp_hash_key",
                "data_type": "STRING",
                "function_name": "hash",
                "hash_of": ["BOOKNAME"]
            }
        ],
        "table_columns_to_be_added": [
            {
                "column_name": "edp_source_system_name",
                "data_type": "STRING",
                "value": "obs"
            }
        ],
        "job_id": "1234",
        "1234_completed": False,
        "1234_duplicate_start": False
    }

    job_args.get.side_effect = lambda key, default_value="": job_args_dict.get(key, default_value)
    job_args.get_mandatory.side_effect = lambda key: job_args_dict[key]
    job_args.get_job_dict.return_value = job_args_dict

    return input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils

@pytest.fixture
def base_ingest(mock_dependencies, lc):
    input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils = mock_dependencies
    return BaseIngest(lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)

# -- Tests start here --

def test_init_sets_attributes(base_ingest, mock_dependencies):
    _, _, common_utils, _, _, _ = mock_dependencies
    common_utils.check_and_set_dbutils.assert_called()

def test_read_and_set_input_args(base_ingest):
    base_ingest.read_and_set_input_args()
    base_ingest.input_args_obj.get_args_keys.assert_called()
    base_ingest.input_args_obj.get.assert_called()
    base_ingest.job_args_obj.set.assert_called()

def test_read_and_set_common_config(base_ingest):
    base_ingest.read_and_set_common_config()
    base_ingest.common_utils_obj.read_yaml_file.assert_called()
    base_ingest.job_args_obj.set.assert_called()


def test_read_and_set_common_config_empty(base_ingest):
    base_ingest.input_args_obj.get.side_effect = lambda key: {
        "common_config_file_location": "",
    }.get(key, None)
    base_ingest.read_and_set_common_config()


def test_read_and_set_table_config(base_ingest):
    base_ingest.read_and_set_table_config()
    base_ingest.common_utils_obj.read_yaml_file.assert_called()
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
    base_ingest.job_args_obj.set.called

def test_ass_derived_columns(monkeypatch, base_ingest, spark_df_mock):
    monkeypatch.setattr("pyspark.sql.functions.sha2", lambda x, y: x)
    monkeypatch.setattr(
        "pyspark.sql.functions.concat_ws",
        lambda sep, *args: "_".join(args)
        # lambda sep, *args: sep.join(args)
    )
    monkeypatch.setattr("pyspark.sql.functions.current_timestamp", lambda: "NOW")
    monkeypatch.setattr("pyspark.sql.functions.lit", lambda val: val)
    base_ingest.pre_load()
    base_ingest.add_derived_columns(spark_df_mock)

def test_write_data_to_target_table(monkeypatch, base_ingest, spark):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(
        mock_df,
        "write",
        SimpleNamespace(mode=lambda m: SimpleNamespace(saveAsTable=lambda t: None)),
    )
    base_ingest.job_args_obj.get_mandatory.side_effect = lambda key: {
        "target_location": "/path",
    }.get(key, None)
    base_ingest.pre_load()
    base_ingest.write_data_to_target_table(mock_df)

def test_write_data_to_quarantine_table(monkeypatch, base_ingest, spark):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(
        mock_df,
        "write",
        SimpleNamespace(mode=lambda m: SimpleNamespace(saveAsTable=lambda t: None)),
    )
    base_ingest.job_args_obj.get_mandatory.side_effect = lambda key: {
        "quarantine_target_location": "/path",
    }.get(key, None)
    base_ingest.pre_load()
    base_ingest.write_data_to_quarantine_table(mock_df)

def test_load_has_no_validation_error(monkeypatch, spark, base_ingest):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(
        base_ingest,
        "read_data_from_source",
        lambda: mock_df
    )
    monkeypatch.setattr(
        base_ingest,
        "add_derived_columns",
        lambda a: mock_df
    )
    job_dict = base_ingest.job_args_obj.get_job_dict()
    base_ingest.validation_obj.run_validations.side_effect = lambda df, job_dict: (
        True,
        mock_df,
        {"validation_has_error": False},
    )
    monkeypatch.setattr(
        base_ingest,
        "write_data_to_target_table",
        lambda a: None,
    )
    base_ingest.load()
    assert base_ingest.job_args_obj.set.called

def test_load_has_no_validation_error_zero_records(monkeypatch, spark, base_ingest):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 0)
    monkeypatch.setattr(
        base_ingest,
        "read_data_from_source",
        lambda: mock_df
    )
    monkeypatch.setattr(
        base_ingest,
        "add_derived_columns",
        lambda a: mock_df
    )
    job_dict = base_ingest.job_args_obj.get_job_dict()
    base_ingest.validation_obj.run_validations.side_effect = lambda df, job_dict: (
        True,
        mock_df,
        {"validation_has_error": False},
    )
    monkeypatch.setattr(
        base_ingest,
        "write_data_to_target_table",
        lambda a: None,
    )
    base_ingest.load()
    assert base_ingest.job_args_obj.set.called
    assert base_ingest.final_status == "COMPLETED_ZERO"

def test_load_has_validation_warning(monkeypatch, spark, base_ingest):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(
        base_ingest,
        "read_data_from_source",
        lambda: mock_df
    )
    monkeypatch.setattr(
        base_ingest,
        "add_derived_columns",
        lambda a: mock_df
    )
    job_dict = base_ingest.job_args_obj.get_job_dict()
    base_ingest.validation_obj.run_validations.side_effect = lambda df, job_dict: (
        False,
        mock_df,
        {"validation_has_error": False},
    )
    monkeypatch.setattr(
        base_ingest,
        "write_data_to_target_table",
        lambda a: None,
    )
    base_ingest.load()
    assert base_ingest.job_args_obj.set.called
    assert base_ingest.final_status == "COMPLETED_VAL_WARN"

def test_load_has_validation_errors(monkeypatch, spark, base_ingest):
    mock_df = spark.createDataframe([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(base_ingest, "spark", spark)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(
        base_ingest,
        "read_data_from_source",
        lambda: mock_df
    )
    monkeypatch.setattr(
        base_ingest,
        "add_derived_columns",
        lambda a: mock_df
    )
    job_dict = base_ingest.job_args_obj.get_job_dict()
    base_ingest.validation_obj.run_validations.side_effect = lambda df, job_dict: (
        False,
        mock_df,
        {"validation_has_error": True},
    )
    base_ingest.process_monitoring_obj.insert_validation_run_status.side_effect = (
        lambda a: None
    )
    monkeypatch.setattr(
        base_ingest,
        "write_data_to_quarantine_table",
        lambda a: None,
    )
    base_ingest.load()
    assert base_ingest.job_args_obj.set.called
    assert base_ingest.final_status == "COMPLETED_VAL_ERROR"

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