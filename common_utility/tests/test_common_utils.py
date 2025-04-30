import pytest
from unittest.mock import Mock, patch, mock_open
from datetime import datetime

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.utils.constants import default_datetime_format, default_date_format


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture(name="common_utils", autouse=True)
def fixture_common_utils(lc):
    return CommonUtils(lc)


def test_get_current_time_success(common_utils, mock_logger):
    time_str = common_utils.get_current_time()
    datetime.strptime(time_str, default_datetime_format)
    mock_logger.logger.info.assert_called()


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
    "date_str",
    [
        "invalid-date",
        "",
    ]
)
def test_get_date_split_failure(common_utils, date_str):
    with pytest.raises(Exception):
        common_utils.get_date_split(date_str)


def test_read_yaml_file_success(common_utils):
    mock_yaml_data = "key: value"
    with patch("builtins.open", mock_open(read_data=mock_yaml_data)):
        with patch("yaml.safe_load", return_value={"key": "value"}):
            result = common_utils.read_yaml_file("dummy.yaml")
            assert result == {"key": "value"}


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
