import pytest
from unittest.mock import MagicMock
from types import SimpleNamespace

from ..tests.fixtures.mock_logger import MockLogger

from edap_ingest.ingest.csv_ingest import CsvIngest


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture(name="dbutils", autouse=True)
def fixture_dbutils():
    return MagicMock()


@pytest.fixture
def spark():
    return MagicMock()


@pytest.fixture
def mock_pyspark_functions(monkeypatch):

    def mock_lit(value):
        return f"mock_lit({value})"

    def mock_current_timestamp(value):
        return "mock_current_timestamp"

    def mock_sha2(col, num_bits):
        return f"mock_sha2({col}, {num_bits})"

    def mock_concat_ws(sep, *cols):
        return f"mock_concat_ws({sep}, {','.join(map(str, cols))})"

    monkeypatch.setattr("pyspark.sql.functions.lit", mock_lit)
    monkeypatch.setattr("pyspark.sql.functions.current_timestamp", mock_current_timestamp)
    monkeypatch.setattr("pyspark.sql.functions.sha2", mock_sha2)
    monkeypatch.setattr("pyspark.sql.functions.concat_ws", mock_concat_ws)


@pytest.fixture
def dummy_args(spark, dbutils):
    return {
        "input_args": SimpleNamespace(
            get=lambda key: "csv" if key == "ingest_type" else None,
            set_mandatory_input_params=lambda _: None,
            set_default_values_for_input_params=lambda _: None
        ),
        "job_args": SimpleNamespace(
            store={},
            get=lambda key: {"schema": {}, "dry_run": False}.get(key, "value"),
            set=lambda k, v: None
        ),
        "common_utils": SimpleNamespace(
            check_and_set_dbutils=dbutils,
            read_yaml=lambda loc: {"key": "value"},
            check_and_evaluate_str_to_bool=lambda val: val == "True",
            get_date_split=lambda date: ("2024", "01", "01")
        ),
        "process_monitoring": SimpleNamespace(
            insert_update_job_run_status=lambda status, passed_comments=None: None,
            check_and_get_job_id=lambda: None,
            check_already_processed=lambda: None,
        ),
        "validation_utils": SimpleNamespace(
            run_validations=lambda df: None
        ),
        # "dbutils": SimpleNamespace(notebook=SimpleNamespace(exit=lambda msg: (_ for _ in ()).throw(SystemExit(msg))))
    }


@pytest.fixture
def csv_ingest(lc, dummy_args, dbutils):
    return CsvIngest(
        lc,
        dummy_args["input_args"],
        dummy_args["job_args"],
        dummy_args["common_utils"],
        dummy_args["process_monitoring"],
        dummy_args["validation_utils"],
        dbutils,
    )


def test_init(csv_ingest, dummy_args, lc):
    assert csv_ingest.lc == lc
    assert csv_ingest.job_args_obj == dummy_args["job_args"]
    assert csv_ingest.common_utils_obj == dummy_args["common_utils"]
    assert csv_ingest.input_args_obj == dummy_args["input_args"]
    assert csv_ingest.process_monitoring_obj == dummy_args["process_monitoring"]
    assert csv_ingest.validation_obj == dummy_args["validation_utils"]


@pytest.mark.parametrize("method_name", [
    "read_and_set_input_args",
    "read_and_set_common_config",
    "read_and_set_table_config",
    "pre_load",
    "form_schema_from_dict",
    "form_source_and_target_locations",
    "collate_columns_to_add",
    "post_load"
])
def test_inherited_methods_success(csv_ingest, method_name):
    method = getattr(csv_ingest, method_name)
    method()  # Should not raise


def test_run_load_success(csv_ingest, monkeypatch):
    monkeypatch.setattr(csv_ingest, "pre_load", lambda: None)
    monkeypatch.setattr(csv_ingest, "load", lambda: None)
    monkeypatch.setattr(csv_ingest, "post_load", lambda: None)
    csv_ingest.run_load()


def test_run_load_failure(csv_ingest, monkeypatch):
    monkeypatch.setattr(csv_ingest, "pre_load", lambda: (_ for _ in ()).throw(Exception("pre_load failed")))
    with pytest.raises(Exception, match="pre_load failed"):
        csv_ingest.run_load()


def test_load_with_schema_and_dry_run_false(csv_ingest, monkeypatch, spark, mock_pyspark_functions):
    mock_df = spark.createDataFrame([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(csv_ingest, "spark", spark)
    monkeypatch.setattr(spark.read, "load", lambda *a, **kw: mock_df)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(mock_df, "write", SimpleNamespace(mode=lambda m: SimpleNamespace(saveAsTable=lambda t: None)))

    csv_ingest.job_args_obj.get = lambda k: {
        "schema_struct": "{'BookName': 'String'}",
        "dry_run": False,
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
        "source_location": "dummy_path",
        "target_location": "dummy_table",
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
        "1234_completed": False,
        "columns_to_be_added": [
            {
                "column_name": "edp_hash_key",
                "data_type": "STRING",
                "function_name": "hash",
                "hash_of": ["BOOKNAME"],
            },
            {
                "column_name": "edp_updated_timestamp",
                "data_type": "TIMESTAMP",
                "function_name": "current_timestamp",
            },
            {
                "column_name": "edp_source_system_name",
                "data_type": "STRING",
                "value": "obs",
            },
        ]
    }.get(k, None)
    csv_ingest.load()


def test_load_without_schema_and_dry_run_false(csv_ingest, monkeypatch, spark, mock_pyspark_functions):
    mock_df = spark.createDataFrame([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(csv_ingest, "spark", spark)
    monkeypatch.setattr(spark.read, "load", lambda *a, **kw: mock_df)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(mock_df, "write", SimpleNamespace(mode=lambda m: SimpleNamespace(saveAsTable=lambda t: None)))

    csv_ingest.job_args_obj.get = lambda k: {
        "schema_struct": None,
        "dry_run": False,
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
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
        "1234_completed": False,
        "columns_to_be_added": []
    }.get(k, None)
    csv_ingest.load()


def test_load_failure_read(monkeypatch, csv_ingest):
    monkeypatch.setattr(csv_ingest, "spark", SimpleNamespace(read=SimpleNamespace(load=lambda *a, **kw: (_ for _ in ()).throw(Exception("read error")))))
    csv_ingest.job_args_obj.get = lambda k: {
        "schema_struct": None,
        "dry_run": False,
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
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
        "1234_completed": False,
        "columns_to_be_added": []
    }.get(k, None)
    with pytest.raises(Exception, match="read error"):
        csv_ingest.load()


def test_load_with_validation_failure(monkeypatch, csv_ingest, spark):
    monkeypatch.setattr(csv_ingest, "spark", spark)
    mock_df = spark.createDataFrame([(1,)], ["a"])
    monkeypatch.setattr(spark.read, "load", lambda *a, **kw: mock_df)
    monkeypatch.setattr(mock_df, "count", lambda: 3)
    monkeypatch.setattr(csv_ingest.validation_obj, "run_validations", lambda df: (_ for _ in ()).throw(Exception("validation fail")))
    csv_ingest.job_args_obj.get = lambda k: {
        "schema_struct": None,
        "dry_run": False,
        "common_config_file_location": "/path/to/common.yaml",
        "table_config_file_location": "/path/to/table.yaml",
        "run_date": "2024-01-01",
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
        "1234_completed": False,
        "columns_to_be_added": []
    }.get(k, None)

    with pytest.raises(Exception, match="validation fail"):
        csv_ingest.load()
