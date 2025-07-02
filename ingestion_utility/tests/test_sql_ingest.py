import pytest
from pyspark.sql.utils import AnalysisException
from edap_ingest.ingest.sql_ingest import SqlIngest


class DummyLogger:
    def __init__(self):
        self.logs = []
    def info(self, msg): self.logs.append(("info", msg))
    def error(self, msg): self.logs.append(("error", msg))


class DummyJobArgs:
    def __init__(self, path):
        self.path = path
    def get_mandatory(self, key):
        if key == "sql_file_location":
            return self.path
        raise KeyError(key)


class DummyCommonUtils:
    def __init__(self, content=None, raise_exc=None):
        self.content = content
        self.raise_exc = raise_exc
    def read_file_as_string(self, file_path):
        if self.raise_exc:
            raise self.raise_exc
        return self.content


class DummySparkSession:
    def __init__(self, df=None, raise_ae=False, raise_exc=False):
        self.df = df
        self.raise_ae = raise_ae
        self.raise_exc = raise_exc
    def sql(self, sql_text):
        if self.raise_ae:
            raise AnalysisException("AnalysisException simulated")
        if self.raise_exc:
            raise Exception("Generic Exception simulated")
        return self.df


@pytest.fixture
def sql_ingest_base():
    # Setup base SqlIngest with dummy dependencies
    lc = type("LC", (), {"logger": DummyLogger()})
    input_args = None
    job_args = DummyJobArgs("file.sql")
    common_utils = DummyCommonUtils(content="SELECT * FROM tbl")
    process_monitoring = None
    validation_utils = None
    ingest = SqlIngest(lc, input_args, job_args, common_utils, process_monitoring, validation_utils)
    return ingest


def test_read_sql_from_file_valid_extension(sql_ingest_base):
    # Normal read with .sql extension
    sql_ingest_base.job_args_obj = DummyJobArgs("query.sql")
    sql_ingest_base.common_utils_obj = DummyCommonUtils(content="SELECT 1")
    content = sql_ingest_base.read_sql_from_file()
    assert content == "SELECT 1"
    assert any(".read_sql_from_file()" in msg for lvl, msg in sql_ingest_base.lc.logger.logs if lvl == "info")


def test_read_sql_from_file_invalid_extension(sql_ingest_base):
    sql_ingest_base.job_args_obj = DummyJobArgs("query.txt")
    with pytest.raises(ValueError, match="Invalid file extension"):
        sql_ingest_base.read_sql_from_file()


@pytest.mark.parametrize("keyword", ["delete", "drop", "truncate"])
def test_validate_sql_forbidden_keyword(sql_ingest_base, keyword):
    bad_sql = f"SELECT * FROM t; {keyword} FROM t2"
    with pytest.raises(ValueError, match=f"SQL contains forbidden keyword: {keyword.upper()}"):
        sql_ingest_base.validate_sql(bad_sql)


@pytest.mark.parametrize("valid_sql", [
    "SELECT * FROM t",
    "MERGE INTO t USING s",
    "INSERT INTO t VALUES (1)",
    "UPDATE t SET c=1"
])
def test_validate_sql_valid(sql_ingest_base, valid_sql):
    # Should not raise
    sql_ingest_base.validate_sql(valid_sql)


def test_validate_sql_no_supported_statement(sql_ingest_base):
    sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"  # no select/merge/insert/update outside?
    # Actually contains SELECT, so let's try a SQL with no valid keywords
    sql = "DROP TABLE t"
    with pytest.raises(ValueError, match="SQL must contain at least one supported operation"):
        sql_ingest_base.validate_sql(sql)


def test_validate_sql_with_parser_valid(sql_ingest_base):
    sql = "SELECT * FROM t; INSERT INTO t VALUES (1)"
    sql_ingest_base.validate_sql_with_parser(sql)  # should not raise


def test_validate_sql_with_parser_disallowed_statement(sql_ingest_base):
    sql = "DELETE FROM t"
    with pytest.raises(ValueError, match="Disallowed SQL operation detected: DELETE"):
        sql_ingest_base.validate_sql_with_parser(sql)


def test_validate_sql_with_parser_parse_error(sql_ingest_base, monkeypatch):
    def fake_parse(_):
        raise Exception("parse failure")
    monkeypatch.setattr("sqlglot.parse", fake_parse)
    with pytest.raises(ValueError, match="Failed to parse SQL"):
        sql_ingest_base.validate_sql_with_parser("SELECT 1")


def test_read_data_from_source_success(monkeypatch):
    dummy_df = object()  # mock spark DataFrame with dummy object
    lc = type("LC", (), {"logger": DummyLogger()})
    job_args = DummyJobArgs("query.sql")
    common_utils = DummyCommonUtils(content="SELECT 1")
    ingest = SqlIngest(lc, None, job_args, common_utils, None, None)

    # monkeypatch methods used internally
    monkeypatch.setattr(ingest, "read_sql_from_file", lambda: "SELECT 1")
    monkeypatch.setattr(ingest, "validate_sql_with_parser", lambda sql: None)
    ingest.spark = DummySparkSession(df=dummy_df)

    result = ingest.read_data_from_source()
    assert result is dummy_df
    assert any("Inside [SqlIngest.read_data_from_source()]" in msg for lvl, msg in ingest.lc.logger.logs if lvl == "info")


def test_read_data_from_source_analysis_exception(monkeypatch):
    lc = type("LC", (), {"logger": DummyLogger()})
    job_args = DummyJobArgs("query.sql")
    common_utils = DummyCommonUtils(content="SELECT 1")
    ingest = SqlIngest(lc, None, job_args, common_utils, None, None)

    monkeypatch.setattr(ingest, "read_sql_from_file", lambda: "SELECT 1")
    monkeypatch.setattr(ingest, "validate_sql_with_parser", lambda sql: None)
    ingest.spark = DummySparkSession(raise_ae=True)

    with pytest.raises(AnalysisException):
        ingest.read_data_from_source()
    assert any("Spark SQL analysis error" in msg for lvl, msg in ingest.lc.logger.logs if lvl == "error")


def test_read_data_from_source_generic_exception(monkeypatch):
    lc = type("LC", (), {"logger": DummyLogger()})
    job_args = DummyJobArgs("query.sql")
    common_utils = DummyCommonUtils(content="SELECT 1")
    ingest = SqlIngest(lc, None, job_args, common_utils, None, None)

    monkeypatch.setattr(ingest, "read_sql_from_file", lambda: "SELECT 1")
    monkeypatch.setattr(ingest, "validate_sql_with_parser", lambda sql: None)
    ingest.spark = DummySparkSession(raise_exc=True)

    with pytest.raises(Exception, match="Generic Exception simulated"):
        ingest.read_data_from_source()
    assert any("Unexpected error executing SQL" in msg for lvl, msg in ingest.lc.logger.logs if lvl == "error")
