import sys
import types

import pytest
from unittest.mock import MagicMock, mock_open
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
        ("", Exception),
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
    "params_dict",
    [
        {"param1": {"input_value": "", "data_type": "str", "check_empty": True}},  # empty check
        {"param2": {"input_value": "abc", "data_type": "int"}},                    # wrong type
    ]
)
def test_validate_function_param_failure(common_utils, params_dict):
    with pytest.raises(Exception):
        common_utils.validate_function_param("test_module", params_dict)
