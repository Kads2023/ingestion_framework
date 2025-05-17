import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.base_ingest import BaseIngest


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestBaseIngest").getOrCreate()


def test_init_sets_attributes(spark):
    config = {"k": "v"}
    ingest = BaseIngest(spark, config, "job1")
    assert ingest.spark == spark
    assert ingest.config == config
    assert ingest.job_name == "job1"
    assert ingest.utils is not None


@pytest.mark.parametrize("args, expected", [
    ({"test": "true", "num": "42"}, {"test": True, "num": "42"}),
    ({"active": "False"}, {"active": False}),
])
def test_read_and_set_input_args(spark, args, expected):
    ingest = BaseIngest(spark, {}, "job")
    ingest.read_and_set_input_args(args)
    assert ingest.config == expected


def test_form_schema_from_dict(spark):
    schema_dict = {"name": "string", "age": "string"}
    ingest = BaseIngest(spark, {}, "job")
    schema = ingest.form_schema_from_dict(schema_dict)
    assert isinstance(schema, StructType)
    assert schema.fieldNames() == ["name", "age"]


def test_exit_without_errors(monkeypatch, spark):
    mock_monitor = type("MockMonitor", (), {"exit_without_errors": lambda self: setattr(self, "called", True)})()
    monkeypatch.setattr("src.base_ingest.JobMonitor", lambda job_name: mock_monitor)
    ingest = BaseIngest(spark, {}, "job")
    ingest.exit_without_errors()
    assert hasattr(mock_monitor, "called")


def test_add_derived_columns_literal(spark):
    df = spark.createDataFrame([("a",)], ["name"])
    ingest = BaseIngest(spark, {}, "job")
    derived_cols = {
        "flag": {"type": "literal", "value": "static_value"}
    }
    result_df = ingest.add_derived_columns(df, derived_cols)
    assert "flag" in result_df.columns
    assert result_df.select("flag").first()[0] == "static_value"


def test_add_derived_columns_hash_function(spark):
    df = spark.createDataFrame([("alice",)], ["name"])
    ingest = BaseIngest(spark, {}, "job")
    derived_cols = {
        "hashed": {"type": "function", "function": "hash", "columns": ["name"]}
    }
    result_df = ingest.add_derived_columns(df, derived_cols)
    assert "hashed" in result_df.columns


def test_add_derived_columns_invalid_type(spark):
    df = spark.createDataFrame([("a",)], ["name"])
    ingest = BaseIngest(spark, {}, "job")
    with pytest.raises(ValueError, match="Unsupported derived column type"):
        ingest.add_derived_columns(df, {"col": {"type": "wrong", "value": "val"}})


def test_add_derived_columns_unknown_function(spark):
    df = spark.createDataFrame([("a",)], ["name"])
    ingest = BaseIngest(spark, {}, "job")
    with pytest.raises(ValueError, match="Unsupported function type: unknown_func"):
        ingest.add_derived_columns(df, {"col": {"type": "function", "function": "unknown_func", "columns": ["name"]}})
