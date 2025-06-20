
import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from pyspark.sql import DataFrame as Spark_Dataframe
from edap_ingest.ingest.delta_ingest import DeltaIngest

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_lc(mock_logger):
    return SimpleNamespace(logger=mock_logger)

@pytest.fixture
def mock_spark(monkeypatch):
    mock_df = MagicMock()
    mock_df.columns = ["id", "order_date", "updated_at"]
    mock_df.where.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_df.withColumnRenamed.return_value = mock_df

    mock_read = MagicMock()
    mock_read.format.return_value.table.return_value = mock_df

    mock_spark = MagicMock()
    mock_spark.read = mock_read
    mock_spark.sql.return_value = mock_df

    monkeypatch.setattr("pyspark.sql.SparkSession", MagicMock(getActiveSession=lambda: mock_spark))
    return mock_spark

def test_column_selection(mock_lc, mock_spark):
    job_args = MagicMock()
    job_args.get_job_dict.return_value = {}
    job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "columns": ["id", "non_existing_column"]
    }.get(key, "")
    job_args.get_mandatory.return_value = "dummy_table"

    ingest = DeltaIngest(mock_lc, None, job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()
    mock_spark.read.format.return_value.table.assert_called_once()
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Final columns --> ['id', 'order_date', 'updated_at']")

def test_column_renaming(mock_lc, mock_spark):
    job_args = MagicMock()
    job_args.get_job_dict.return_value = {}
    job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "column_mappings": {"id": "identifier"}
    }.get(key, "")
    job_args.get_mandatory.return_value = "dummy_table"

    ingest = DeltaIngest(mock_lc, None, job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Renamed column id to identifier")

def test_custom_transformation_success(mock_lc, mock_spark):
    job_args = MagicMock()
    job_args.get_job_dict.return_value = {}
    job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "custom_transformations": ["df = df.filter(F.col('id') > 10)"]
    }.get(key, "")
    job_args.get_mandatory.return_value = "dummy_table"

    ingest = DeltaIngest(mock_lc, None, job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Applied custom transformation #1")

def test_custom_transformation_failure(mock_lc, mock_spark):
    job_args = MagicMock()
    job_args.get_job_dict.return_value = {}
    job_args.get.side_effect = lambda key, default=None: {
        "sql_query": "",
        "custom_transformations": ["df = df.invalid_function()"]
    }.get(key, "")
    job_args.get_mandatory.return_value = "dummy_table"

    ingest = DeltaIngest(mock_lc, None, job_args, None, None, None)
    ingest.spark = mock_spark

    with pytest.raises(ValueError) as exc:
        ingest.read_data_from_source()

    assert "Failed to apply custom transformation" in str(exc.value)


def test_merge_data_to_target_table_success(monkeypatch, mock_lc, mock_job_args_table):
    mock_spark = MagicMock()
    mock_delta_table = MagicMock()
    mock_merge_builder = MagicMock()

    mock_job_args_table.get.return_value = {
        "target_table": "target_table",
        "merge_keys": ["id"],
        "update_columns": ["col1", "col2"]
    }

    monkeypatch.setattr("edap_ingest.ingest.delta_ingest.DeltaTable", MagicMock(forName=lambda spark, table: mock_delta_table))

    mock_delta_table.alias.return_value.merge.return_value = mock_merge_builder
    mock_merge_builder.whenMatchedUpdate.return_value.whenNotMatchedInsertAll.return_value = mock_merge_builder
    mock_merge_builder.execute.return_value = None

    ingest = DeltaIngest(mock_lc, None, mock_job_args_table, None, None, None)
    ingest.spark = mock_spark

    source_df = MagicMock()
    ingest.merge_data_to_target_table(source_df)

    mock_merge_builder.execute.assert_called_once()


def test_merge_data_missing_keys(monkeypatch, mock_lc, mock_job_args_table):
    mock_job_args_table.get.return_value = {
        "target_table": "target_table",
        "merge_keys": [],
        "update_columns": ["col1"]
    }

    ingest = DeltaIngest(mock_lc, None, mock_job_args_table, None, None, None)
    ingest.spark = MagicMock()
    with pytest.raises(ValueError, match="merge_keys must be specified"):
        ingest.merge_data_to_target_table(MagicMock())


def test_write_data_to_target_table_dry_run(mock_lc, mock_job_args_table):
    mock_job_args_table.get.return_value = True
    mock_job_args_table.get_mandatory.return_value = "target_table"

    ingest = DeltaIngest(mock_lc, None, mock_job_args_table, None, None, None)
    ingest.spark = MagicMock()
    ingest.write_data_to_target_table(MagicMock())

    mock_lc.logger.info.assert_any_call("[DeltaIngest.write_data_to_target_table()] - Dry run enabled - skipping write/merge")


def test_write_data_with_partition(mock_lc, mock_job_args_table):
    mock_job_args_table.get.return_value = "append"
    mock_job_args_table.get.side_effect = lambda k, d=None: {
        "dry_run": False,
        "merge": None,
        "write_mode": "append",
        "partition_by": ["col1"]
    }.get(k, d)
    mock_job_args_table.get_mandatory.return_value = "target_table"

    mock_df = MagicMock()
    mock_df.count.return_value = 100

    writer = MagicMock()
    mock_df.write.mode.return_value = writer
    writer.partitionBy.return_value = writer

    ingest = DeltaIngest(mock_lc, None, mock_job_args_table, None, None, None)
    ingest.spark = MagicMock()
    ingest.write_data_to_target_table(mock_df)

    writer.saveAsTable.assert_called_once_with("target_table")
