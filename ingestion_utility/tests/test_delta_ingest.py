import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from edap_ingest.ingest.delta_ingest import DeltaIngest
from pyspark.sql import DataFrame as Spark_Dataframe


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_lc(mock_logger):
    return SimpleNamespace(logger=mock_logger)


@pytest.fixture
def mock_job_args():
    mock = MagicMock()
    mock.get_job_dict.return_value = {"run_date": "2024-01-01"}
    mock.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": [],
        "dry_run": False,
        "write_mode": "append",
        "partition_by": None
    }.get(key, default)
    mock.get_mandatory.side_effect = lambda key: {
        "source_table": "dummy_table",
        "target_location": "target.db.table"
    }.get(key, None)
    mock.set = MagicMock()
    return mock


@pytest.fixture
def mock_spark(monkeypatch):
    mock_df = MagicMock(spec=Spark_Dataframe)
    mock_df.columns = ["id", "name", "order_date", "updated_at"]
    mock_df.select.return_value = mock_df
    mock_df.where.return_value = mock_df
    mock_df.withColumnRenamed.side_effect = lambda old, new: mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.count.return_value = 42

    mock_read = MagicMock()
    mock_read.format.return_value.table.return_value = mock_df

    mock_spark = MagicMock()
    mock_spark.read = mock_read
    mock_spark.sql.return_value = mock_df

    monkeypatch.setattr("edap_ingest.ingest.delta_ingest.SparkSession", MagicMock(getActiveSession=lambda: mock_spark))
    return mock_spark, mock_df


def test_init_logs_entry(mock_lc, mock_job_args):
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    assert ingest.this_class_name == "DeltaIngest"
    mock_lc.logger.info.assert_called()


def test_read_data_with_sql_query_success(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "SELECT * FROM tbl WHERE run_date = '{run_date}'",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": [],
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark[0]

    df = ingest.read_data_from_source()
    mock_lc.logger.info.assert_any_call(
        "[DeltaIngest.read_data_from_source()] - Executing sql_query --> SELECT * FROM tbl WHERE run_date = '2024-01-01'"
    )
    assert df == mock_spark[1]


def test_read_data_with_sql_query_failure(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get_job_dict.return_value = {}
    mock_job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "SELECT * FROM tbl WHERE run_date = '{missing_key}'",
        "where_clause": ""
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark[0]

    with pytest.raises(ValueError) as e:
        ingest.read_data_from_source()
    assert "Failed to render or execute sql_query" in str(e.value)


def test_read_data_with_table_and_columns_where_clause_and_dedup(mock_lc, mock_job_args, mock_spark):
    # Setup config for columns, where_clause and deduplication
    mock_job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "where_clause": "order_date = '{run_date}'",
        "columns": ["id", "order_date", "missing_col"],
        "deduplication": {
            "keys": ["id"],
            "order_by": [
                {"column": "updated_at", "direction": "desc"},
                {"column": "order_date", "direction": "asc"}
            ]
        },
        "column_mappings": {"order_date": "order_dt"},
        "custom_transformations": [],
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    spark, df = mock_spark
    ingest.spark = spark

    # The deduplication part will call withColumn, filter, drop — all mocked to return df itself
    result_df = ingest.read_data_from_source()

    # Check select called with existing columns only (missing_col ignored)
    df.select.assert_called_once_with("id", "order_date")
    # Check where clause applied
    df.where.assert_called_once_with("order_date = '2024-01-01'")
    # Check withColumnRenamed called for column_mappings
    df.withColumnRenamed.assert_called_once_with("order_date", "order_dt")
    # The returned dataframe is mocked df
    assert result_df == df
    # Logs should include deduplication info and rename info
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Applying deduplication on keys=['id'] order_by=[{'column': 'updated_at', 'direction': 'desc'}, {'column': 'order_date', 'direction': 'asc'}]")
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Renamed column order_date to order_dt")


def test_read_data_custom_transformations_success(mock_lc, mock_job_args, mock_spark):
    # Provide a transformation that adds a dummy column
    code = "df = df.withColumn('dummy_col', F.lit(1))"
    mock_job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": [code],
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    spark, df = mock_spark
    ingest.spark = spark

    df.withColumn.return_value = df  # chaining
    result_df = ingest.read_data_from_source()

    df.withColumn.assert_called_with('dummy_col', ANY := MagicMock())  # Check the transformation applied
    assert result_df == df
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Applied custom transformation #1")


def test_read_data_custom_transformations_failure(mock_lc, mock_job_args, mock_spark):
    # Provide transformation with syntax error
    bad_code = "df = df.withColumn('dummy_col', F.lit()"  # Missing closing parenthesis
    mock_job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "where_clause": "",
        "custom_transformations": [bad_code],
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark[0]

    with pytest.raises(ValueError) as e:
        ingest.read_data_from_source()

    assert "Failed to apply custom transformation #1" in str(e.value)


def test_write_data_to_target_table_append_partition(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda key, default=None: {
        "dry_run": False,
        "write_mode": "append",
        "partition_by": ["order_date", "region"],
        "target_location": "target.db.table"
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    _, df = mock_spark
    ingest.spark = MagicMock()  # not needed here

    writer = MagicMock()
    df.write = MagicMock(return_value=writer)
    writer.mode.return_value = writer
    writer.partitionBy.return_value = writer
    writer.saveAsTable.return_value = None

    ingest.write_data_to_target_table(df)

    df.count.assert_called_once()
    mock_job_args.set.assert_called_with("run_row_count", 42)
    writer.mode.assert_called_once_with("append")
    writer.partitionBy.assert_called_once_with("order_date", "region")
    writer.saveAsTable.assert_called_once_with("target.db.table")
    mock_lc.logger.info.assert_called_with("[DeltaIngest.write_data_to_target_table()] - Writing data to target.db.table mode=append partitionBy=['order_date', 'region']")


def test_write_data_to_target_table_dry_run(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda key, default=None: {
        "dry_run": True,
        "write_mode": "append",
        "partition_by": None,
        "target_location": "target.db.table"
    }.get(key, default)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    _, df = mock_spark

    df.write = MagicMock()
    ingest.write_data_to_target_table(df)
    df.write.mode.assert_not_called()
    mock_lc.logger.info.assert_called_with("[DeltaIngest.write_data_to_target_table()] - Dry run enabled - skipping write")
