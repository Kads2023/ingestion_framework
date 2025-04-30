import pytest
from types import SimpleNamespace
from pyspark.sql import SparkSession
from edap_ingest.ingest.csv_ingest import CsvIngest


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").appName("TestApp").getOrCreate()


@pytest.fixture
def dummy_args(spark):
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
            log_msg=lambda msg, passed_logger_type=None: print(msg),
            check_and_set_dbutils=lambda dbutils: dbutils or "dbutils",
            read_yaml=lambda loc: {"key": "value"},
            check_and_evaluate_str_to_bool=lambda val: val == "True",
            get_date_split=lambda date: ("2024", "01", "01")
        ),
        "process_monitoring": SimpleNamespace(
            insert_update_job_run_status=lambda status, passed_comments=None: None,
            check_and_get_job_id=lambda: None
        ),
        "validation_utils": SimpleNamespace(
            run_validations=lambda df: None
        ),
        "dbutils": SimpleNamespace(notebook=SimpleNamespace(exit=lambda msg: (_ for _ in ()).throw(SystemExit(msg))))
    }


@pytest.fixture
def csv_ingest(dummy_args):
    return CsvIngest(
        dummy_args["input_args"],
        dummy_args["job_args"],
        dummy_args["common_utils"],
        dummy_args["process_monitoring"],
        dummy_args["validation_utils"],
        dummy_args["dbutils"]
    )


def test_init(csv_ingest):
    assert csv_ingest.this_class_name == "CsvIngest"


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


def test_load_with_schema_and_dry_run_false(csv_ingest, monkeypatch, spark):
    mock_df = spark.createDataFrame([(1,)], ["a"])
    mock_df.schema = lambda: "mock_schema"
    monkeypatch.setattr(csv_ingest, "spark", spark)
    monkeypatch.setattr(spark.read, "load", lambda *a, **kw: mock_df)
    monkeypatch.setattr(mock_df, "count", lambda: 5)
    monkeypatch.setattr(mock_df, "write", SimpleNamespace(mode=lambda m: SimpleNamespace(saveAsTable=lambda t: None)))

    csv_ingest.job_args_obj.get = lambda k: {
        "schema_struct": None,
        "dry_run": False,
        "source_location": "dummy_path",
        "target_location": "dummy_table",
        "columns_to_be_added": ["a"]
    }.get(k, None)
    csv_ingest.load()


def test_load_failure_read(monkeypatch, csv_ingest):
    monkeypatch.setattr(csv_ingest, "spark", SimpleNamespace(read=SimpleNamespace(load=lambda *a, **kw: (_ for _ in ()).throw(Exception("read error")))))
    csv_ingest.job_args_obj.get = lambda k: {"schema_struct": None, "dry_run": False, "source_location": "path", "target_location": "table"}.get(k, None)
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
        "source_location": "dummy_path",
        "target_location": "dummy_table",
        "columns_to_be_added": []
    }.get(k, None)

    with pytest.raises(Exception, match="validation fail"):
        csv_ingest.load()
