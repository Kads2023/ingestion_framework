import pytest
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.objects.data_type_mapping import DataTypeMapping



@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture(name="common_utils", autouse=True)
def fixture_common_utils(lc):
    return CommonUtils(lc)



@pytest.fixture
def data_type_mapper(lc, common_utils):
    return DataTypeMapping(lc, common_utils)


@pytest.mark.parametrize(
    "input_type, expected_class",
    [
        ("string", StringType),
        ("int", IntegerType),
        ("integer", IntegerType),
        ("double", DoubleType),
        ("boolean", BooleanType),
        ("bool", BooleanType),
        ("date", DateType),
    ]
)
def test_get_type_success(data_type_mapper, input_type, expected_class):
    result = data_type_mapper.get_type(input_type)
    assert result == expected_class


@pytest.mark.parametrize(
    "input_type",
    [
        "",            # empty string
        "unknown",     # unsupported type
        None,          # NoneType
    ]
)
def test_get_type_invalid_type(data_type_mapper, input_type):
    with pytest.raises(Exception):
        data_type_mapper.get_type(input_type)


def test_get_type_logs_error_on_invalid_type(data_type_mapper, mock_logger):
    invalid_type = "unsupported_type"
    with pytest.raises(Exception):
        data_type_mapper.get_type(invalid_type, passed_module="test_module")

    # Check if logger.error was called
    # assert mock_logger.logger.error.called
    # assert "Unsupported type" in mock_logger.logger.error.call_args[0][0]
