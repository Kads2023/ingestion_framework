import sys
import types

import pytest
from unittest.mock import MagicMock, mock_open, Mock
from types import SimpleNamespace

from datetime import datetime

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.utils.constants import *

import edap_common.utils.common_utils


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture(name="common_utils", autouse=True)
def fixture_common_utils(lc):
    return CommonUtils(lc)


@pytest.fixture(name="dbutils", autouse=True)
def fixture_dbutils(lc):
    return MagicMock()


@pytest.fixture(name="sleep", autouse=True)
def fixture_sleep():
    return MagicMock()


@pytest.fixture
def dummy_args():
    return {
        "yaml": SimpleNamespace(
            safe_load=lambda loc: {"key", "value"},
        )
    }


@pytest.mark.parametrize(
    "input_str, expected_bool",
    [
        ("True", True),
        ("false", False),
        ("yes", True),
        ("no", False),
        ("1", True),
        ("0", False),
    ]
)
def test_check_and_evaluate_str_to_bool(common_utils, input_str, expected_bool):
    result = common_utils.check_and_evaluate_str_to_bool(input_str)
    assert result == expected_bool


@pytest.mark.parametrize(
    "input_str",
    [
        123,
        True,
        None,
    ]
)
def test_check_and_evaluate_str_to_bool_invalid_type(common_utils, input_str):
    with pytest.raises(TypeError):
        common_utils.check_and_evaluate_str_to_bool(input_str)


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("2024-04-30", ("2024", "04", "30")),
    ]
)
def test_get_date_split_success(common_utils, date_str, expected):
    result = common_utils.get_date_split(date_str)
    assert result == expected


@pytest.mark.parametrize(
    "date_str, error_type",
    [
        ("2024-0430", Exception),
        (2025, TypeError),
        ("", Exception),
    ]
)
def test_get_date_split_fail(common_utils, date_str, error_type):
    with pytest.raises(error_type):
        common_utils.get_date_split(date_str)


def test_check_and_set_dbutils_creates(monkeypatch, common_utils):
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]
    mock_dbutils = types.ModuleType("pyspark.dbutils")

    class MockDBUtils:
        def __init__(self, spark=None):
            self.spark = spark
            self.mocked = True

    mock_dbutils.DBUtils = MockDBUtils
    sys.modules["pyspark.dbutils"] = mock_dbutils
    common_utils.check_and_set_dbutils(dbutils=None)
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]


def test_check_and_set_dbutils_mnfe(monkeypatch, common_utils):
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]
    mock_dbutils_mnfe = types.ModuleType("pyspark.dbutils")

    class MockDBUtilsM:
        def __init__(self, spark=None):
            self.spark = spark
            self.mocked = True
            raise ModuleNotFoundError("mock error")

    mock_dbutils_mnfe.DBUtils = MockDBUtilsM
    sys.modules["pyspark.dbutils"] = mock_dbutils_mnfe
    with pytest.raises(ModuleNotFoundError):
        common_utils.check_and_set_dbutils(dbutils=None)
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]


def test_check_and_set_dbutils_exception(monkeypatch, common_utils):
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]
    mock_dbutils_excep = types.ModuleType("pyspark.dbutils")

    class MockDBUtilsE:
        def __init__(self, spark=None):
            self.spark = spark
            self.mocked = True
            raise Exception("mock error")

    mock_dbutils_excep.DBUtils = MockDBUtilsE

    sys.modules["pyspark.dbutils"] = mock_dbutils_excep
    with pytest.raises(Exception):
        common_utils.check_and_set_dbutils(dbutils=None)
    if "pyspark.dbutils" in sys.modules:
        del sys.modules["pyspark.dbutils"]


def test_check_and_set_dbutils_success(dbutils, common_utils):
    common_utils.check_and_set_dbutils(dbutils)


def test_get_current_time_success(common_utils, mock_logger):
    time_str = common_utils.get_current_time()
    datetime.strptime(time_str, default_datetime_format)
    mock_logger.logger.info.assert_called()


def test_read_yaml_file_success(monkeypatch, dummy_args, common_utils):
    mock_yaml_data = "key: value"

    monkeypatch.setattr(
        edap_common.utils.common_utils, "open", mock_open(read_data=mock_yaml_data)
    )

    monkeypatch.setattr(edap_common.utils.common_utils, "yaml", dummy_args["yaml"])
    result = common_utils.read_yaml_file("dummy.yaml")
    assert result == {"key": "value"}


@pytest.mark.parametrize(
    "file_path, error_type",
    [
        (2025, TypeError),
        ("", ValueError),
    ]
)
def test_read_yaml_file_fail(common_utils, file_path, error_type):
    with pytest.raises(error_type):
        common_utils.read_yaml_file(file_path)


def test_read_yaml_file_failure(common_utils):
    with pytest.raises(Exception):
        common_utils.read_yaml_file("/invalid/path.yaml")


@pytest.mark.parametrize(
    "params_dict",
    [
        {"param1": {"input_value": "hello", "data_type": "str", "check_empty": True}},
        {"param2": {"input_value": 123, "data_type": "int"}},
        {"param3": {"input_value": False, "data_type": "bool"}},
    ]
)
def test_validate_function_param_success(common_utils, params_dict):
    common_utils.validate_function_param("test_module", params_dict)


@pytest.mark.parametrize(
    "params_dict, error_type",
    [
        ({"param1": {"input_value": "", "data_type": "str", "check_empty": True}}, ValueError),  # empty check
        ({"param2": {"input_value": "abc", "data_type": "int"}}, TypeError),                    # wrong type
    ]
)
def test_validate_function_param_failure(common_utils, params_dict, error_type):
    with pytest.raises(error_type):
        common_utils.validate_function_param("test_module", params_dict)


def test_retry_decorator_success(monkeypatch, common_utils):
    # Create a function that will succeed
    def success_func():
        return "Success"

    # Apply the decorator
    decorated_func = common_utils.retry_on_exception(max_attempts=3, delay_seconds=1)(success_func)

    # Call the decorated function
    result = decorated_func()

    # Assert that the function was called only once, and it returned "Success"
    assert result == "Success"


def test_retry_decorator_failure(monkeypatch, common_utils, sleep):
    # Create a function that will always raise an exception
    def failing_func():
        raise Exception("Test Exception")

    # Apply the decorator
    decorated_func = common_utils.retry_on_exception(max_attempts=3, delay_seconds=1)(failing_func)

    # Mock time.sleep to avoid waiting in the test
    monkeypatch.setattr(edap_common.utils.common_utils.time, 'sleep', sleep)

    # Call the decorated function and check that it raises the exception after 3 retries
    with pytest.raises(Exception):
        decorated_func()

    # Assert that time.sleep was called 2 times (retry attempts)
    assert sleep.call_count == 2


def test_retry_decorator_retry_count(monkeypatch, common_utils, sleep):
    # Create a function that raises an exception in the first 2 calls and succeeds in the third
    retry_attempts = 0

    def retrying_func():
        nonlocal retry_attempts
        retry_attempts += 1
        if retry_attempts < 3:
            raise Exception("Test Exception")
        return "Success"

    # Apply the decorator
    decorated_func = common_utils.retry_on_exception(max_attempts=3, delay_seconds=1)(retrying_func)

    # Mock time.sleep to avoid delays during the test
    monkeypatch.setattr(edap_common.utils.common_utils.time, 'sleep', sleep)

    # Call the decorated function
    result = decorated_func()

    # Assert that the function eventually returns "Success"
    assert result == "Success"

    # Assert that time.sleep was called 2 times (after the first two failed attempts)
    assert sleep.call_count == 2


def test_retry_decorator_no_retry_on_success(monkeypatch, common_utils, sleep):
    # Create a function that will succeed
    def success_func():
        return "Success"

    # Apply the decorator
    decorated_func = common_utils.retry_on_exception(max_attempts=3, delay_seconds=1)(success_func)

    # Mock time.sleep to avoid delays
    monkeypatch.setattr(edap_common.utils.common_utils.time, 'sleep', sleep)

    # Call the decorated function
    result = decorated_func()

    # Assert that the function was called once and returned the correct result
    assert result == "Success"

    # Assert that time.sleep was never called because the function didn't fail
    assert sleep.call_count == 0

@pytest.fixture
def spark_df_mock():
    df = Mock()
    df.columns = ["col1", "col2"]
    df.withColumn = MagicMock(return_value=df)
    df.rdd.isRmpty = MagicMock(return_value=False)  # typo intentional for simulating original
    return df

@pytest.fixture
def monkey_data_type_defaults(monkeypatch):
    monkeypatch.setitem(
        __import__("edap_common.utils.constants").data_type_defaults,
        "spark_dataframe",
        {"type_name": "Spark DataFrame", "type": Mock},
    )
    monkeypatch.setitem(
        __import__("edap_common.utils.constants").data_type_defaults,
        "str",
        {"type_name": "String", "type": str},
    )
    monkeypatch.setitem(
        __import__("edap_common.utils.constants").data_type_defaults,
        "list",
        {"type_name": "List", "type": list},
    )

# ----------------------------
# validate_function_param
# ----------------------------
def test_validate_function_param_valid(common_utils, monkey_data_type_defaults):
    params_dict = {
        "input_df": {
            "input_value": Mock(),
            "data_type": "spark_dataframe",
            "check_empty": False,
        },
        "name": {
            "input_value": "test",
            "data_type": "str",
            "check_empty": True,
        }
    }
    common_utils.validate_function_param("test_module", params_dict)

def test_validate_function_param_invalid_type(common_utils, monkey_data_type_defaults):
    params_dict = {
        "param": {
            "input_value": 123,
            "data_type": "str",
        }
    }
    with pytest.raises(TypeError):
        common_utils.validate_function_param("mod", params_dict)

def test_validate_function_param_unsupported_type(common_utils):
    params_dict = {
        "param": {
            "input_value": "val",
            "data_type": "unsupported",
        }
    }
    with pytest.raises(ValueError):
        common_utils.validate_function_param("mod", params_dict)

# ----------------------------
# add_hash_column
# ----------------------------
def test_add_hash_column_success(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    monkeypatch.setattr("pyspark.sql.functions.sha2", lambda x, y: x)
    monkeypatch.setattr("pyspark.sql.functions.concat_ws", lambda sep, *args: "_".join(args))
    result_df = common_utils.add_hash_column(spark_df_mock, "hash_col", ["col1", "col2"])
    assert result_df == spark_df_mock

def test_add_hash_column_duplicate_col(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    spark_df_mock.columns.append("hash_col")
    with pytest.raises(ValueError):
        common_utils.add_hash_column(spark_df_mock, "hash_col", ["col1"])

# ----------------------------
# add_current_timestamp
# ----------------------------
def test_add_current_timestamp_success(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    monkeypatch.setattr("pyspark.sql.functions.current_timestamp", lambda: "NOW")
    result_df = common_utils.add_current_timestamp(spark_df_mock, "timestamp")
    assert result_df == spark_df_mock

def test_add_current_timestamp_duplicate_col(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    spark_df_mock.columns.append("timestamp")
    with pytest.raises(ValueError):
        common_utils.add_current_timestamp(spark_df_mock, "timestamp")

# ----------------------------
# add_literal_column
# ----------------------------
def test_add_literal_column_success(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    monkeypatch.setattr("pyspark.sql.functions.lit", lambda val: val)
    result_df = common_utils.add_literal_column(spark_df_mock, "lit_col", "value")
    assert result_df == spark_df_mock

def test_add_literal_column_duplicate_col(common_utils, spark_df_mock, monkeypatch, monkey_data_type_defaults):
    monkeypatch.setattr(common_utils, "validate_function_param", Mock())
    spark_df_mock.columns.append("lit_col")
    with pytest.raises(ValueError):
        common_utils.add_literal_column(spark_df_mock, "lit_col", "value")

def test_read_file_as_string_success(common_utils, monkeypatch):
    mock_file_data = "SELECT * FROM users;"
    m_open = mock_open(read_data=mock_file_data)

    monkeypatch.setattr("builtins.open", m_open)

    result = common_utils.read_file_as_string("/dbfs/tmp/test.sql")
    assert result == mock_file_data
    m_open.assert_called_once_with("/dbfs/tmp/test.sql", "r", encoding="utf-8")


def test_read_file_as_string_invalid_path(common_utils):
    with pytest.raises(ValueError, match="Invalid file path provided"):
        common_utils.read_file_as_string("   ")


def test_read_file_as_string_file_not_found(common_utils, monkeypatch):
    def mock_open_fn(*args, **kwargs):
        raise FileNotFoundError("No such file")

    monkeypatch.setattr("builtins.open", mock_open_fn)

    with pytest.raises(FileNotFoundError, match="does not exist"):
        common_utils.read_file_as_string("/dbfs/tmp/missing.sql")


def test_read_file_as_string_permission_denied(common_utils, monkeypatch):
    def mock_open_fn(*args, **kwargs):
        raise PermissionError("Permission denied")

    monkeypatch.setattr("builtins.open", mock_open_fn)

    with pytest.raises(PermissionError, match="Permission denied"):
        common_utils.read_file_as_string("/dbfs/tmp/restricted.sql")


def test_read_file_as_string_os_error(common_utils, monkeypatch):
    def mock_open_fn(*args, **kwargs):
        raise OSError("Disk error")

    monkeypatch.setattr("builtins.open", mock_open_fn)

    with pytest.raises(IOError, match="Disk error"):
        common_utils.read_file_as_string("/dbfs/tmp/broken.sql")
import pytest
from common_utils import CommonUtils

@pytest.mark.parametrize(
    "template, values, expected",
    [
        # Case 1: All keys present
        ("Hello {name}", {"name": "Srinivas"}, "Hello Srinivas"),
        # Case 2: Missing key should be preserved
        ("Hello {name}, meet {friend}", {"name": "Srinivas"}, "Hello Srinivas, meet {friend}"),
        # Case 3: Multiple placeholders with some missing
        ("{greet} {name}", {"greet": "Hi"}, "Hi {name}"),
        # Case 4: Empty values dict
        ("Hello {name}", {}, "Hello {name}"),
    ],
)
def test_safe_substitute_success(template, values, expected):
    assert CommonUtils.safe_substitute(template, values) == expected


def test_safe_substitute_with_monkeypatch(monkeypatch):
    """Test with monkeypatching SafeDict.__missing__ to return a fixed value"""
    def mock_missing(self, key):
        return "<MISSING>"

    monkeypatch.setattr(CommonUtils.SafeDict, "__missing__", mock_missing)

    result = CommonUtils.safe_substitute("Hello {name}", {})
    assert result == "Hello <MISSING>"
