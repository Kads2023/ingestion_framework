import pytest
from unittest.mock import MagicMock
import sqlglot
from pyspark.sql import DataFrame as SparkDataFrame
from edap_ingest.ingest.sql_batch_ingest import SqlBatchIngest

@pytest.mark.parametrize("step, parse_success, validation_success, validation_has_error, expect_write, expect_quarantine", [
    # Success case: SQL parses, validation passes, no error
    ({
        "name": "step_1",
        "sql_file_location": "/tmp/sql1.sql",
        "target_catalog": "cat",
        "target_schema": "sch",
        "target_table": "tbl",
        "validations": []
    }, True, True, False, True, False),

    # Quarantine case: SQL parses, validation fails with error
    ({
        "name": "step_2",
        "sql_file_location": "/tmp/sql2.sql",
        "target_catalog": "cat",
        "target_schema": "sch",
        "target_table": "tbl",
        "validations": []
    }, True, False, True, False, True),

    # Failure case: SQL parsing fails
    ({
        "name": "step_3",
        "sql_file_location": "/tmp/sql3.sql",
        "target_catalog": "cat",
        "target_schema": "sch",
        "target_table": "tbl",
        "validations": []
    }, False, False, False, False, False),
])
def test_run_batch(monkeypatch, step, parse_success, validation_success, validation_has_error, expect_write, expect_quarantine):
    # Create a test instance
    ingest = SqlBatchIngest(
        lc=MagicMock(),
        input_args=MagicMock(),
        job_args=MagicMock(),
        common_utils=MagicMock(),
        process_monitoring=MagicMock(),
        validation_utils=MagicMock()
    )

    ingest.pre_load = MagicMock()
    ingest.post_load = MagicMock()
    ingest.write_data_to_target_table = MagicMock()
    ingest.write_data_to_quarantine_table = MagicMock()

    # Mocks
    ingest.job_args_obj.get.return_value = [step]  # returns sqls list
    ingest.common_utils_obj.read_file.return_value = "SELECT 1"

    df_mock = MagicMock(spec=SparkDataFrame)
    df_mock.rdd.isEmpty.return_value = not expect_quarantine

    ingest.spark.sql.return_value = df_mock

    if parse_success:
        monkeypatch.setattr(sqlglot, "parse_one", lambda sql: MagicMock())
    else:
        monkeypatch.setattr(sqlglot, "parse_one", lambda sql: None)

    ingest.validation_obj.run_validations.return_value = (
        validation_success,
        df_mock,
        {"validation_has_error": validation_has_error}
    )

    # Run
    if not parse_success and ingest.raise_exception:
        with pytest.raises(Exception):
            ingest.run_batch()
    else:
        ingest.run_batch()

    # Assertions
    if expect_write:
        ingest.write_data_to_target_table.assert_called_once()
    else:
        ingest.write_data_to_target_table.assert_not_called()

    if expect_quarantine:
        ingest.write_data_to_quarantine_table.assert_called_once()
    else:
        ingest.write_data_to_quarantine_table.assert_not_called()
