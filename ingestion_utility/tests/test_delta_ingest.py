import pytest
from unittest.mock import MagicMock, patch
from edap_ingest.ingest.delta_ingest import DeltaIngest


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_lc(mock_logger):
    return MagicMock(logger=mock_logger)


@pytest.fixture
def mock_job_args(monkeypatch):
    job_args = MagicMock()

    # Default get returns None
    job_args.get.return_value = None
    job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": [],
        "merge": None,
        "dry_run": False,
        "write_mode": "append",
        "partition_by": None,
        "target_location": "target_table"
    }.get(k, d)
    job_args.get_mandatory.side_effect = lambda k: {
        "source_table": "source_table",
        "target_location": "target_table"
    }[k]
    job_args.set = MagicMock()
    return job_args


@pytest.fixture
def mock_spark(monkeypatch):
    # Mock DataFrame
    mock_df = MagicMock()
    mock_df.columns = ["id", "order_date", "status"]

    # select returns a DataFrame
    mock_df.select.return_value = mock_df
    # where returns a DataFrame
    mock_df.where.return_value = mock_df
    # withColumn returns a DataFrame
    mock_df.withColumn.return_value = mock_df
    # filter returns a DataFrame
    mock_df.filter.return_value = mock_df
    # drop returns a DataFrame
    mock_df.drop.return_value = mock_df
    # withColumnRenamed returns a DataFrame
    mock_df.withColumnRenamed.return_value = mock_df
    # count returns int
    mock_df.count.return_value = 42

    # Mock read.format().table() chain returns mock_df
    mock_read = MagicMock()
    mock_read.format.return_value.table.return_value = mock_df

    # Mock SparkSession.sql() returns mock_df
    mock_spark = MagicMock()
    mock_spark.read = mock_read
    mock_spark.sql.return_value = mock_df

    return mock_spark


def test_init_logs_info(mock_lc, mock_job_args, mock_spark):
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark
    mock_lc.logger.info.assert_called()


def test_read_data_with_sql_query_success(mock_lc, mock_job_args, mock_spark):
    # Provide sql_query with format key
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "SELECT * FROM table WHERE id = '{id}'",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": []
    }.get(k, d)
    mock_job_args.get_job_dict.return_value = {"id": 123}

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()
    assert df == mock_spark.sql.return_value
    mock_lc.logger.info.assert_any_call(
        "[DeltaIngest.read_data_from_source()] - Executing sql_query --> SELECT * FROM table WHERE id = '123'"
    )


def test_read_data_with_sql_query_render_failure(mock_lc, mock_job_args, mock_spark):
    # sql_query with missing format key will fail
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "SELECT * FROM table WHERE id = '{missing_key}'",
        "where_clause": ""
    }.get(k, d)
    mock_job_args.get_job_dict.return_value = {}

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    with pytest.raises(ValueError, match="Failed to render or execute sql_query"):
        ingest.read_data_from_source()


def test_read_data_with_table_and_where_clause_success(mock_lc, mock_job_args, mock_spark):
    # Configure for table read with where_clause and columns
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "order_date = '{date}'",
        "columns": ["id", "order_date", "missing_col"],
        "deduplication": None,
        "column_mappings": None,
        "custom_transformations": []
    }.get(k, d)
    mock_job_args.get_job_dict.return_value = {"date": "2023-01-01"}
    mock_job_args.get_mandatory.side_effect = lambda k: {
        "source_table": "source_table"
    }[k]

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()

    # Should select only columns that exist
    mock_spark.read.format.return_value.table.return_value.select.assert_called_with("id", "order_date")

    mock_spark.read.format.return_value.table.return_value.where.assert_called_with("order_date = '2023-01-01'")
    assert df is not None


def test_read_data_where_clause_render_failure(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "date = '{missing_key}'",
        "columns": None
    }.get(k, d)
    mock_job_args.get_job_dict.return_value = {}
    mock_job_args.get_mandatory.side_effect = lambda k: {
        "source_table": "source_table"
    }[k]

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    with pytest.raises(ValueError, match="Failed to apply where_clause"):
        ingest.read_data_from_source()


@patch("edap_ingest.ingest.delta_ingest.Window")
@patch("edap_ingest.ingest.delta_ingest.F")
def test_deduplication_success(mock_F, mock_Window, mock_lc, mock_job_args, mock_spark):
    # Setup deduplication config
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "",
        "columns": None,
        "deduplication": {
            "keys": ["id"],
            "order_by": [{"column": "order_date", "direction": "desc"}]
        },
        "column_mappings": None,
        "custom_transformations": []
    }.get(k, d)
    mock_job_args.get_job_dict.return_value = {}

    # Mock order col asc/desc
    col_mock = MagicMock()
    col_mock.asc.return_value = "asc_col"
    col_mock.desc.return_value = "desc_col"
    mock_F.col.return_value = col_mock

    # Mock Window.partitionBy(...).orderBy(...)
    window_spec = MagicMock()
    mock_Window.partitionBy.return_value.orderBy.return_value = window_spec

    # mock F.row_number().over(window_spec)
    row_number_mock = MagicMock()
    row_number_mock.over.return_value = "row_num_col"
    mock_F.row_number.return_value = row_number_mock

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    # Call the method - should apply deduplication
    df = ingest.read_data_from_source()

    # Validate the calls to Window and F
    mock_Window.partitionBy.assert_called_with("id")
    mock_Window.partitionBy.return_value.orderBy.assert_called()
    mock_F.row_number.assert_called_once()
    mock_F.col.assert_called()

    # Should call withColumn, filter and drop on df
    mock_spark.read.format.return_value.table.return_value.withColumn.assert_called()
    mock_spark.read.format.return_value.table.return_value.filter.assert_called()
    mock_spark.read.format.return_value.table.return_value.drop.assert_called()
    assert df is not None


def test_column_renaming_logs_info(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "",
        "columns": None,
        "deduplication": None,
        "column_mappings": {"old_col": "new_col"},
        "custom_transformations": []
    }.get(k, d)

    # Mock df.columns to contain old_col
    mock_df = MagicMock()
    mock_df.columns = ["old_col", "other_col"]
    # Mock chain for withColumnRenamed returning df
    mock_df.withColumnRenamed.return_value = mock_df

    # Patch spark.read.format().table() to return mock_df
    mock_spark.read.format.return_value.table.return_value = mock_df

    mock_job_args.get_job_dict.return_value = {}

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()
    mock_df.withColumnRenamed.assert_called_with("old_col", "new_col")
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Renamed column old_col to new_col")
    assert df == mock_df


def test_custom_transformation_success(mock_lc, mock_job_args, mock_spark):
    # Provide one custom transformation that adds a column
    code = "df = df.withColumn('new_col', F.lit(1))"
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "",
        "custom_transformations": [code]
    }.get(k, d)

    mock_df = MagicMock()
    mock_df.columns = ["id"]
    # Setup withColumn to return df
    mock_df.withColumn.return_value = mock_df

    mock_spark.read.format.return_value.table.return_value = mock_df
    mock_job_args.get_job_dict.return_value = {}

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    df = ingest.read_data_from_source()

    mock_df.withColumn.assert_called_with("new_col", ANY)
    mock_lc.logger.info.assert_any_call("[DeltaIngest.read_data_from_source()] - Applied custom transformation #1")
    assert df is not None


def test_custom_transformation_failure_raises_value_error(mock_lc, mock_job_args, mock_spark):
    # Provide broken python code for transformation
    code = "df = df.non_existing_function()"
    mock_job_args.get.side_effect = lambda k, d=None: {
        "sql_query": "",
        "where_clause": "",
        "custom_transformations": [code]
    }.get(k, d)

    mock_df = MagicMock()
    mock_df.columns = ["id"]

    mock_spark.read.format.return_value.table.return_value = mock_df
    mock_job_args.get_job_dict.return_value = {}

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    with pytest.raises(ValueError, match="Failed to apply custom transformation #1"):
        ingest.read_data_from_source()


@patch("delta.tables.DeltaTable")
def test_merge_data_to_target_table_success(mock_delta_table_cls, mock_lc, mock_job_args, mock_spark):
    merge_conf = {
        "target_table": "target_table",
        "merge_keys": ["id"],
        "update_columns": ["order_date", "status"],
        "delete_condition": "source.is_deleted = true"
    }
    mock_job_args.get.side_effect = lambda k, d=None: {
        "merge": merge_conf,
        "dry_run": False
    }.get(k, d)

    # Setup mock DeltaTable instance and chained merge builder calls
    mock_delta_table = MagicMock()
    mock_merge_builder = MagicMock()
    mock_merge_builder.whenMatchedUpdate.return_value = mock_merge_builder
    mock_merge_builder.whenNotMatchedInsertAll.return_value = mock_merge_builder
    mock_merge_builder.whenMatchedDelete.return_value = mock_merge_builder
    mock_merge_builder.execute.return_value = None

    mock_delta_table.alias.return_value = mock_delta_table
    mock_delta_table.merge.return_value = mock_merge_builder

    mock_delta_table_cls.forName.return_value = mock_delta_table

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark
    dummy_df = MagicMock()

    ingest.merge_data_to_target_table(dummy_df)

    mock_lc.logger.info.assert_any_call("[DeltaIngest.merge_data_to_target_table()] - Starting merge into target_table on keys ['id']")
    mock_merge_builder.execute.assert_called_once()
    mock_lc.logger.info.assert_any_call("[DeltaIngest.merge_data_to_target_table()] - Merge completed successfully")


def test_merge_data_missing_config_raises(mock_lc, mock_job_args, mock_spark):
    # No merge config
    mock_job_args.get.side_effect = lambda k, d=None: None
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark
    with pytest.raises(ValueError, match="Merge configuration missing"):
        ingest.merge_data_to_target_table(MagicMock())


def test_merge_data_missing_target_or_keys_raises(mock_lc, mock_job_args, mock_spark):
    # merge config without target_table
    mock_job_args.get.side_effect = lambda k, d=None: {
        "merge": {"merge_keys": ["id"]}
    }.get(k, d)

    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    with pytest.raises(ValueError, match="target_table and merge_keys must be specified for merge"):
        ingest.merge_data_to_target_table(MagicMock())


def test_write_data_to_target_table_dry_run_skips_all(mock_lc, mock_job_args, mock_spark):
    mock_job_args.get.side_effect = lambda k, d=None: {
        "dry_run": True,
        "merge": {"target_table": "target_table", "merge_keys": ["id"]}
    }.get(k, d)
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark
    dummy_df = MagicMock()

    ingest.write_data_to_target_table(dummy_df)
    mock_lc.logger.info.assert_any_call("[DeltaIngest.write_data_to_target_table()] - Dry run enabled - skipping write/merge")


def test_write_data_to_target_table_calls_merge(mock_lc, mock_job_args, mock_spark, monkeypatch):
    # Setup merge config present
    mock_job_args.get.side_effect = lambda k, d=None: {
        "dry_run": False,
        "merge": {"target_table": "target_table", "merge_keys": ["id"], "update_columns": ["col1"]}
    }.get(k, d)
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark
    dummy_df = MagicMock()

    called = {}
    def fake_merge(df):
        called["merge_called"] = True
    monkeypatch.setattr(ingest, "merge_data_to_target_table", fake_merge)

    ingest.write_data_to_target_table(dummy_df)
    assert called.get("merge_called") is True


def test_write_data_to_target_table_calls_write(mock_lc, mock_job_args, mock_spark):
    # No merge config, normal write path
    mock_job_args.get.side_effect = lambda k, d=None: {
        "dry_run": False,
        "merge": None,
        "write_mode": "overwrite",
        "partition_by": ["col1", "col2"],
        "target_location": "target_table"
    }.get(k, d)
    mock_job_args.get_mandatory.side_effect = lambda k: "target_table"
    ingest = DeltaIngest(mock_lc, None, mock_job_args, None, None, None)
    ingest.spark = mock_spark

    dummy_df = MagicMock()
    # Mock .write.mode() chain
    mock_writer = MagicMock()
    dummy_df.write.mode.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer
    mock_writer.saveAsTable.return_value = None
    dummy_df.count.return_value = 10

    ingest.write_data_to_target_table(dummy_df)

    dummy_df.count.assert_called_once()
    mock_job_args.set.assert_called_with("run_row_count", 10)
    dummy_df.write.mode.assert_called_with("overwrite")
    mock_writer.partitionBy.assert_called_with("col1", "col2")
    mock_writer.saveAsTable.assert_called_with("target_table")
    mock_lc.logger.info.assert_any_call("[DeltaIngest.write_data_to_target_table()] - Writing data to target_table mode=overwrite partitionBy=['col1', 'col2']")


