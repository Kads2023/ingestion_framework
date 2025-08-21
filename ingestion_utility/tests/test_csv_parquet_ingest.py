import pytest
from types import SimpleNamespace
from pyspark.sql import DataFrame

from edap_ingest.ingest.csv_ingest import CsvIngest
from edap_ingest.ingest.parquet_ingest import ParquetIngest


class DummyLogger:
    def info(self, msg): pass
    def error(self, msg): pass


class DummyJobArgs:
    def __init__(self, args):
        self.args = args
    def get(self, k, default=None): return self.args.get(k, default)
    def get_mandatory(self, k): return self.args[k]


@pytest.fixture
def dummy_dependencies(monkeypatch):
    lc = SimpleNamespace(logger=DummyLogger())
    cu = object()
    pm = object()
    vu = object()
    spark = SimpleNamespace()
    monkeypatch.setattr("edap_ingest.ingest.base_ingest.BaseIngest.spark", spark)
    return lc, cu, pm, vu, spark


@pytest.mark.parametrize(
    "file_ext, delimiter, schema_struct",
    [
        ("csv", ",", None),
        ("txt", "|", None),
        ("tsv", "\t", "dummy_schema"),
    ],
)
def test_csv_ingest_success(monkeypatch, dummy_dependencies, file_ext, delimiter, schema_struct):
    lc, cu, pm, vu, spark = dummy_dependencies
    source_location = f"/mnt/data/sample.{file_ext}"
    target_location = "/mnt/output/"

    job_args = DummyJobArgs({
        "source_location": source_location,
        "target_location": target_location,
        "delimiter": delimiter,
        "schema_struct": schema_struct,
        "multi_line": False,
    })

    df_mock = SimpleNamespace(columns=["id", "name"])
    reader_mock = SimpleNamespace(
        option=lambda *a, **kw: reader_mock,
        schema=lambda s: reader_mock,
        csv=lambda s: df_mock,
    )

    monkeypatch.setattr(spark, "read", reader_mock)

    ci = CsvIngest(lc, {}, job_args, cu, pm, vu)
    result = ci.read_data_from_source()
    assert isinstance(result, type(df_mock))
    assert result.columns == ["id", "name"]


def test_csv_ingest_invalid_extension(monkeypatch, dummy_dependencies):
    lc, cu, pm, vu, spark = dummy_dependencies
    job_args = DummyJobArgs({
        "source_location": "/mnt/data/sample.bad",
        "target_location": "/mnt/output/",
    })

    ci = CsvIngest(lc, {}, job_args, cu, pm, vu)
    with pytest.raises(ValueError):
        ci.read_data_from_source()


def test_csv_ingest_read_failure(monkeypatch, dummy_dependencies):
    lc, cu, pm, vu, spark = dummy_dependencies
    job_args = DummyJobArgs({
        "source_location": "/mnt/data/sample.csv",
        "target_location": "/mnt/output/",
    })

    def fail_csv(*args, **kwargs):
        raise RuntimeError("Spark read failed")

    reader_mock = SimpleNamespace(
        option=lambda *a, **kw: reader_mock,
        schema=lambda s: reader_mock,
        csv=fail_csv,
    )
    monkeypatch.setattr(spark, "read", reader_mock)

    ci = CsvIngest(lc, {}, job_args, cu, pm, vu)
    with pytest.raises(RuntimeError):
        ci.read_data_from_source()
@pytest.mark.parametrize("schema_struct", [None, "dummy_schema"])
def test_parquet_ingest_success(monkeypatch, dummy_dependencies, schema_struct):
    lc, cu, pm, vu, spark = dummy_dependencies
    job_args = DummyJobArgs({
        "source_location": "/mnt/data/sample.parquet",
        "target_location": "/mnt/output/",
        "schema_struct": schema_struct,
    })

    df_mock = SimpleNamespace(columns=["id", "value"])
    reader_mock = SimpleNamespace(
        option=lambda *a, **kw: reader_mock,
        schema=lambda s: reader_mock,
        parquet=lambda s: df_mock,
    )
    monkeypatch.setattr(spark, "read", reader_mock)

    pi = ParquetIngest(lc, {}, job_args, cu, pm, vu)
    result = pi.read_data_from_source()
    assert isinstance(result, type(df_mock))
    assert result.columns == ["id", "value"]


def test_parquet_ingest_invalid_extension(monkeypatch, dummy_dependencies):
    lc, cu, pm, vu, spark = dummy_dependencies
    job_args = DummyJobArgs({
        "source_location": "/mnt/data/sample.txt",
        "target_location": "/mnt/output/",
    })
    pi = ParquetIngest(lc, {}, job_args, cu, pm, vu)
    with pytest.raises(ValueError):
        pi.read_data_from_source()


def test_parquet_ingest_read_failure(monkeypatch, dummy_dependencies):
    lc, cu, pm, vu, spark = dummy_dependencies
    job_args = DummyJobArgs({
        "source_location": "/mnt/data/sample.parquet",
        "target_location": "/mnt/output/",
    })

    def fail_parquet(*args, **kwargs):
        raise RuntimeError("Parquet read failed")

    reader_mock = SimpleNamespace(
        option=lambda *a, **kw: reader_mock,
        schema=lambda s: reader_mock,
        parquet=fail_parquet,
    )
    monkeypatch.setattr(spark, "read", reader_mock)

    pi = ParquetIngest(lc, {}, job_args, cu, pm, vu)
    with pytest.raises(RuntimeError):
        pi.read_data_from_source()
