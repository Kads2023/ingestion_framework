import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.csv_ingest import CsvIngest


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestCsvIngest").getOrCreate()


@pytest.fixture
def dummy_df(spark):
    return spark.createDataFrame([Row(name="alice", age="30")])


@pytest.fixture
def csv_config():
    return {
        "input_path": "path.csv",
        "header": True,
        "delimiter": ",",
        "inferSchema": True
    }


def test_read_data_from_source_success(monkeypatch, spark, dummy_df, csv_config):
    # Patch validate_input
    class DummyUtils:
        def validate_input(self, config):
            return True

        def get_schema(self, config):
            return dummy_df.schema

    monkeypatch.setattr("src.csv_ingest.CommonUtils", lambda: DummyUtils())

    # Patch spark.read.csv
    class DummyReader:
        def options(self, **kwargs):
            return self

        def schema(self, sch):
            return self

        def csv(self, path):
            return dummy_df

    monkeypatch.setattr(spark, "read", DummyReader())

    ingest = CsvIngest(spark, csv_config, "job1")
    df = ingest.read_data_from_source()
    assert df.count() == 1
    assert "name" in df.columns


def test_read_data_from_source_validation_fail(monkeypatch, spark, csv_config):
    class DummyUtils:
        def validate_input(self, config):
            return False

    monkeypatch.setattr("src.csv_ingest.CommonUtils", lambda: DummyUtils())

    ingest = CsvIngest(spark, csv_config, "job1")
    with pytest.raises(ValueError, match="Input validation failed"):
        ingest.read_data_from_source()


def test_read_data_from_source_read_failure(monkeypatch, spark, csv_config):
    class DummyUtils:
        def validate_input(self, config):
            return True

        def get_schema(self, config):
            return StructType([StructField("dummy", StringType(), True)])

    monkeypatch.setattr("src.csv_ingest.CommonUtils", lambda: DummyUtils())

    # Raise error on csv read
    class BadReader:
        def options(self, **kwargs):
            return self

        def schema(self, sch):
            return self

        def csv(self, path):
            raise IOError("Read failed")

    monkeypatch.setattr(spark, "read", BadReader())

    ingest = CsvIngest(spark, csv_config, "job1")
    with pytest.raises(IOError, match="Read failed"):
        ingest.read_data_from_source()
