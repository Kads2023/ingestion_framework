import pytest
from pyspark.sql.utils import AnalysisException
from unittest.mock import MagicMock
import traceback


class DummyLogger:
    def __init__(self):
        self.info = MagicMock()
        self.error = MagicMock()


class DummyLC:
    def __init__(self):
        self.logger = DummyLogger()


class DummySpark:
    def sql(self, query):
        return f"Result of [{query}]"


class DummyUtils:
    def __init__(self):
        self.lc = DummyLC()
        self.spark = DummySpark()
        self.this_class_name = "DummyUtils"

    def validate_function_param(self, this_module, param_dict):
        # Assume validation always passes
        pass

    def execute_sql_on_dataframe(self, input_df, sql_string):
        this_module = f"[{self.this_class_name}.execute_sql_on_dataframe()] -"

        self.validate_function_param(
            this_module,
            {
                "input_df": {
                    "input_value": input_df,
                    "data_type": "spark_dataframe",
                    "check_empty": True,
                },
                "sql_string": {
                    "input_value": sql_string,
                    "data_type": "str",
                    "check_empty": True,
                },
            }
        )

        try:
            temp_view_name = f"temp_view_{str(abs(hash(input_df)))}"

            self.lc.logger.info(
                f"{this_module} Registering temp view '{temp_view_name}' with columns: {input_df.columns}"
            )

            input_df.createOrReplaceTempView(temp_view_name)

            sql_to_execute = sql_string.format(source_data=temp_view_name)

            self.lc.logger.info(
                f"{this_module} Executing SQL: {sql_to_execute}"
            )

            result_df = self.spark.sql(sql_to_execute)

            self.lc.logger.info(
                f"{this_module} Successfully executed SQL on DataFrame"
            )

            return result_df

        except AnalysisException as e:
            error_msg = (
                f"{this_module} AnalysisException while executing SQL --> {sql_string} "
                f"\nException: {e}\nTRACEBACK:\n{traceback.format_exc()}"
            )
            self.lc.logger.error(error_msg)
            raise

        except Exception as e:
            error_msg = (
                f"{this_module} Exception while executing SQL --> {sql_string} "
                f"\nException: {e}\nTRACEBACK:\n{traceback.format_exc()}"
            )
            self.lc.logger.error(error_msg)
            raise


# ------------------- TESTS -----------------------

def test_execute_sql_success(monkeypatch):
    utils = DummyUtils()

    mock_df = MagicMock()
    mock_df.columns = ["id", "name"]

    monkeypatch.setattr(mock_df, "createOrReplaceTempView", MagicMock())

    result = utils.execute_sql_on_dataframe(mock_df, "SELECT * FROM {source_data}")
    assert "Result of [SELECT * FROM temp_view_" in result


def test_execute_sql_analysis_exception(monkeypatch):
    utils = DummyUtils()

    mock_df = MagicMock()
    mock_df.columns = ["id"]
    mock_df.createOrReplaceTempView = MagicMock()

    def mock_sql(*args, **kwargs):
        raise AnalysisException("Invalid SQL")

    utils.spark.sql = mock_sql

    with pytest.raises(AnalysisException):
        utils.execute_sql_on_dataframe(mock_df, "SELECT * FROM {source_data}")

    assert utils.lc.logger.error.called


def test_execute_sql_generic_exception(monkeypatch):
    utils = DummyUtils()

    mock_df = MagicMock()
    mock_df.columns = ["id"]
    mock_df.createOrReplaceTempView = MagicMock()

    def mock_sql(*args, **kwargs):
        raise Exception("Something went wrong")

    utils.spark.sql = mock_sql

    with pytest.raises(Exception):
        utils.execute_sql_on_dataframe(mock_df, "SELECT * FROM {source_data}")

    assert utils.lc.logger.error.called


def test_execute_sql_invalid_param(monkeypatch):
    utils = DummyUtils()

    # Raise an error from validate_function_param
    def mock_validate(this_module, param_dict):
        raise ValueError("Validation failed")

    utils.validate_function_param = mock_validate

    with pytest.raises(ValueError, match="Validation failed"):
        utils.execute_sql_on_dataframe("not_a_df", "SELECT * FROM {source_data}")


import pytest
from unittest.mock import MagicMock
from pyspark.sql.utils import AnalysisException


class DummyLogger:
    def __init__(self):
        self.error = MagicMock()
        self.info = MagicMock()


class DummyLC:
    def __init__(self):
        self.logger = DummyLogger()


class DummyUtils:
    def __init__(self):
        self.lc = DummyLC()
        self.this_class_name = "DummyUtils"

    def validate_function_param(self, *args, **kwargs):
        pass

    def select_columns(self, input_df, columns_to_select):
        # place real implementation here during actual run
        pass

    def drop_columns(self, input_df, columns_to_drop):
        pass

    def apply_where_clause(self, input_df, where_clause):
        pass

    def apply_column_transformations(self, input_df, column_mapping):
        pass


@pytest.fixture
def mock_df():
    df = MagicMock()
    df.columns = ["id", "name", "age"]
    return df


def test_select_columns_success(monkeypatch, mock_df):
    utils = DummyUtils()

    monkeypatch.setattr(mock_df, "select", lambda *args: f"Selected {args}")
    result = utils.select_columns(mock_df, ["id", "name"])
    assert result == "Selected ('id', 'name')"


def test_select_columns_missing_column(monkeypatch, mock_df):
    utils = DummyUtils()

    with pytest.raises(ValueError, match="Missing columns"):
        utils.select_columns(mock_df, ["id", "not_exist"])


def test_select_columns_invalid_type(monkeypatch, mock_df):
    utils = DummyUtils()

    with pytest.raises(TypeError):
        utils.select_columns(mock_df, "id")  # Not a list


def test_drop_columns_success(monkeypatch, mock_df):
    utils = DummyUtils()

    monkeypatch.setattr(mock_df, "drop", lambda *args: f"Dropped {args}")
    result = utils.drop_columns(mock_df, ["id"])
    assert result == "Dropped ('id',)"


def test_drop_columns_invalid_type(monkeypatch, mock_df):
    utils = DummyUtils()

    with pytest.raises(TypeError):
        utils.drop_columns(mock_df, {"id"})  # Set instead of list

def test_apply_where_clause_success(monkeypatch, mock_df):
    utils = DummyUtils()

    monkeypatch.setattr(mock_df, "where", lambda clause: f"Filtered by {clause}")
    result = utils.apply_where_clause(mock_df, "age > 18")
    assert result == "Filtered by age > 18"


def test_apply_where_clause_invalid_type(monkeypatch, mock_df):
    utils = DummyUtils()

    with pytest.raises(TypeError):
        utils.apply_where_clause(mock_df, {"id": 1})  # Invalid clause type
def test_apply_column_transformations_rename(monkeypatch, mock_df):
    utils = DummyUtils()

    monkeypatch.setattr(mock_df, "withColumnRenamed", lambda src, tgt: f"Renamed {src} to {tgt}")
    mock_df.withColumn = MagicMock()

    result = utils.apply_column_transformations(mock_df, [{"source_column": "id", "target_column": "user_id"}])
    assert "Renamed id to user_id" in str(result)


def test_apply_column_transformations_cast(monkeypatch, mock_df):
    utils = DummyUtils()

    from pyspark.sql import functions as F
    monkeypatch.setattr(F, "col", lambda c: MagicMock(name=f"col({c})"))
    monkeypatch.setattr(mock_df, "withColumn", lambda tgt, col_obj: f"Casted to {tgt}")

    result = utils.apply_column_transformations(mock_df, [
        {"source_column": "age", "target_column": "age_int", "target_data_type": "int"}
    ])
    assert result == "Casted to age_int"


def test_apply_column_transformations_invalid_mapping(monkeypatch, mock_df):
    utils = DummyUtils()

    # Not a list
    with pytest.raises(TypeError):
        utils.apply_column_transformations(mock_df, {"source_column": "id"})


def test_apply_column_transformations_missing_keys(monkeypatch, mock_df):
    utils = DummyUtils()

    with pytest.raises(KeyError):
        utils.apply_column_transformations(mock_df, [{"target_column": "new_id"}])
