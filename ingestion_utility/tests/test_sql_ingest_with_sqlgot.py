import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.utils import AnalysisException

from edap_ingest.ingest.sql_ingest import SqlIngest


@pytest.fixture
def sql_ingest():
    lc = MagicMock()
    lc.logger = MagicMock()

    input_args = {}
    job_args = MagicMock()
    job_args.get_mandatory = MagicMock(return_value="mock_file.sql")

    common_utils = MagicMock()
    process_monitoring = MagicMock()
    validation_utils = MagicMock()

    return SqlIngest(lc, input_args, job_args, common_utils, process_monitoring, validation_utils)


def test_read_sql_from_file_valid(sql_ingest):
    sql_ingest.common_utils_obj.read_file_as_string.return_value = "SELECT * FROM table"
    result = sql_ingest.read_sql_from_file()
    assert result == "SELECT * FROM table"


def test_read_sql_from_file_invalid_extension(sql_ingest):
    sql_ingest.job_args_obj.get_mandatory.return_value = "invalid_file.txt"
    with pytest.raises(ValueError, match="Invalid file extension for SQL file"):
        sql_ingest.read_sql_from_file()


@pytest.mark.parametrize("sql_text, expected_error", [
    ("DELETE FROM table", "SQL contains forbidden keyword: DELETE"),
    ("DROP TABLE table", "SQL contains forbidden keyword: DROP"),
    ("TRUNCATE TABLE table", "SQL contains forbidden keyword: TRUNCATE"),
    ("", "SQL must contain at least one supported operation"),
    ("ALTER TABLE mytable", "SQL must contain at least one supported operation"),
])
def test_validate_sql_invalid_cases(sql_ingest, sql_text, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        sql_ingest.validate_sql(sql_text)


@pytest.mark.parametrize("sql_text", [
    "SELECT * FROM table",
    "MERGE INTO target USING source ON condition",
    "INSERT INTO table VALUES (1)",
    "UPDATE table SET column = value",
])
def test_validate_sql_valid_cases(sql_ingest, sql_text):
    sql_ingest.validate_sql(sql_text)  # should not raise


def test_validate_sql_with_parser_valid(sql_ingest):
    stmt_mock = MagicMock()
    stmt_mock.key = "select"
    with patch("sqlglot.parse", return_value=[stmt_mock]):
        sql_ingest.validate_sql_with_parser("SELECT * FROM test")


def test_validate_sql_with_parser_invalid_parse(sql_ingest):
    with patch("sqlglot.parse", side_effect=Exception("syntax error")):
        with pytest.raises(ValueError, match="Failed to parse SQL:"):
            sql_ingest.validate_sql_with_parser("INVALID SQL")


def test_validate_sql_with_parser_disallowed_stmt(sql_ingest):
    stmt_mock = MagicMock()
    stmt_mock.key = "delete"
    with patch("sqlglot.parse", return_value=[stmt_mock]):
        with pytest.raises(ValueError, match="Disallowed SQL operation detected: DELETE"):
            sql_ingest.validate_sql_with_parser("DELETE FROM test")


def test_read_data_from_source_success(sql_ingest):
    sql_ingest.read_sql_from_file = MagicMock(return_value="SELECT * FROM test")
    sql_ingest.validate_sql_with_parser = MagicMock()
    spark_mock = MagicMock()
    result_df_mock = MagicMock()
    spark_mock.sql.return_value = result_df_mock

    sql_ingest.spark = spark_mock

    result = sql_ingest.read_data_from_source()
    assert result == result_df_mock
    spark_mock.sql.assert_called_once_with("SELECT * FROM test")


def test_read_data_from_source_analysis_exception(sql_ingest):
    sql_ingest.read_sql_from_file = MagicMock(return_value="SELECT * FROM test")
    sql_ingest.validate_sql_with_parser = MagicMock()

    spark_mock = MagicMock()
    spark_mock.sql.side_effect = AnalysisException("Analysis failed")

    sql_ingest.spark = spark_mock

    with pytest.raises(AnalysisException):
        sql_ingest.read_data_from_source()


def test_read_data_from_source_generic_exception(sql_ingest):
    sql_ingest.read_sql_from_file = MagicMock(return_value="SELECT * FROM test")
    sql_ingest.validate_sql_with_parser = MagicMock()

    spark_mock = MagicMock()
    spark_mock.sql.side_effect = Exception("Unexpected error")

    sql_ingest.spark = spark_mock

    with pytest.raises(Exception, match="Unexpected error"):
        sql_ingest.read_data_from_source()


# sqlglot.parse is mocked to return a list of stmt mocks or raise exceptions.
#
# Spark's sql method is mocked to test success, AnalysisException, and generic exceptions.
#
# Full validation, coverage, and branching for read_sql_from_file, validate_sql, validate_sql_with_parser, and read_data_from_source.
#
# Let me know if you want a pytest-cov report script or if you're using Databricks notebooks to test this!




import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.utils import AnalysisException

from edap_ingest.ingest.sql_ingest import SqlIngest

VALID_SQL = "SELECT * FROM dummy;"
INVALID_KEYWORD_SQL = "DROP TABLE dummy;"
INVALID_SYNTAX_SQL = "SELECT FROM WHERE;"
UNSUPPORTED_SQL = "ALTER TABLE dummy ADD col1 INT;"


@pytest.fixture
def sql_ingest_instance():
    mock_lc = MagicMock()
    mock_common_utils = MagicMock()
    mock_job_args = MagicMock()
    mock_validation_utils = MagicMock()
    mock_process_monitoring = MagicMock()

    ingest = SqlIngest(
        lc=mock_lc,
        input_args={},
        job_args=mock_job_args,
        common_utils=mock_common_utils,
        process_monitoring=mock_process_monitoring,
        validation_utils=mock_validation_utils
    )
    ingest.spark = MagicMock()
    return ingest


def test_read_sql_from_file_success(sql_ingest_instance):
    sql_ingest_instance.job_args_obj.get_mandatory.return_value = "query.sql"
    sql_ingest_instance.common_utils_obj.read_file_as_string.return_value = VALID_SQL
    result = sql_ingest_instance.read_sql_from_file()
    assert result == VALID_SQL


def test_read_sql_from_file_invalid_extension(sql_ingest_instance):
    sql_ingest_instance.job_args_obj.get_mandatory.return_value = "query.txt"
    with pytest.raises(ValueError, match="Invalid file extension"):
        sql_ingest_instance.read_sql_from_file()


@pytest.mark.parametrize("bad_sql", [
    "DELETE FROM table1",
    "DROP TABLE table1",
    "TRUNCATE TABLE table1"
])
def test_validate_sql_rejects_forbidden_keywords(sql_ingest_instance, bad_sql):
    with pytest.raises(ValueError, match="SQL contains forbidden keyword"):
        sql_ingest_instance.validate_sql(bad_sql)


def test_validate_sql_missing_supported_keywords(sql_ingest_instance):
    bad_sql = "ALTER TABLE x"
    with pytest.raises(ValueError, match="must contain at least one supported operation"):
        sql_ingest_instance.validate_sql(bad_sql)


def test_validate_sql_success(sql_ingest_instance):
    sql_ingest_instance.validate_sql("SELECT * FROM xyz")


@patch("sqlglot.parse")
def test_validate_sql_with_parser_valid(mock_parse, sql_ingest_instance):
    stmt = MagicMock()
    stmt.key.upper.return_value = "SELECT"
    mock_parse.return_value = [stmt]
    sql_ingest_instance.validate_sql_with_parser("SELECT * FROM test")  # Should not raise


@patch("sqlglot.parse")
def test_validate_sql_with_parser_disallowed(mock_parse, sql_ingest_instance):
    stmt = MagicMock()
    stmt.key.upper.return_value = "DROP"
    mock_parse.return_value = [stmt]
    with pytest.raises(ValueError, match="Disallowed SQL operation detected: DROP"):
        sql_ingest_instance.validate_sql_with_parser("DROP TABLE test")


@patch("sqlglot.parse", side_effect=Exception("Syntax error"))
def test_validate_sql_with_parser_error(mock_parse, sql_ingest_instance):
    with pytest.raises(ValueError, match="Failed to parse SQL:"):
        sql_ingest_instance.validate_sql_with_parser("bad sql")


@patch.object(SqlIngest, "read_sql_from_file", return_value=VALID_SQL)
@patch.object(SqlIngest, "validate_sql_with_parser")
def test_read_data_from_source_success(mock_validate, mock_read, sql_ingest_instance):
    dummy_df = MagicMock()
    sql_ingest_instance.spark.sql.return_value = dummy_df
    result = sql_ingest_instance.read_data_from_source()
    assert result == dummy_df
    sql_ingest_instance.spark.sql.assert_called_once_with(VALID_SQL)


@patch.object(SqlIngest, "read_sql_from_file", return_value=VALID_SQL)
@patch.object(SqlIngest, "validate_sql_with_parser")
def test_read_data_from_source_analysis_exception(mock_validate, mock_read, sql_ingest_instance):
    sql_ingest_instance.spark.sql.side_effect = AnalysisException("analysis failed", stackTrace=[])
    with pytest.raises(AnalysisException):
        sql_ingest_instance.read_data_from_source()


@patch.object(SqlIngest, "read_sql_from_file", return_value=VALID_SQL)
@patch.object(SqlIngest, "validate_sql_with_parser")
def test_read_data_from_source_unexpected_exception(mock_validate, mock_read, sql_ingest_instance):
    sql_ingest_instance.spark.sql.side_effect = Exception("Something broke")
    with pytest.raises(Exception, match="Something broke"):
        sql_ingest_instance.read_data_from_source()


