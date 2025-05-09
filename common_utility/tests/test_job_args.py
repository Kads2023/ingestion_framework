import pytest

from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.objects.input_args import InputArgs
from edap_common.objects.job_args import JobArgs


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


@pytest.fixture(name="common_utils", autouse=True)
def fixture_common_utils(lc):
    return CommonUtils(lc)


@pytest.fixture(name="input_args", autouse=True)
def fixture_input_args(lc, common_utils):
    return InputArgs(
        lc,
        common_utils,
        job_name="trial_job",
        frequency="trial_frequency",
        load_type="trial_load",
        source_system="trial_source",
        source="trial_file",
        source_type="file"
    )


@pytest.fixture(name="job_args", autouse=True)
def fixture_job_args(lc, common_utils):
    return JobArgs(lc, common_utils)


@pytest.mark.parametrize(
    "passed_key, passed_value",
    [
        ("job_name", "trial_job"),
        ("frequency", "trial_frequency"),
        ("load_type", "trial_load"),
        ("source_system", "trial_source"),
        ("source", "trial_file"),
        ("source_type", "file"),
    ],
)
def test_set_success(job_args, passed_key, passed_value):
    job_args.set(passed_key, passed_value)
    assert job_args.job_dict[passed_key] == passed_value


@pytest.mark.parametrize(
    "passed_key, expected_value",
    [
        ("trial", ""),
        ("job_name", "trial_job"),
    ],
)
def test_get_success(job_args, passed_key, expected_value):
    job_args.job_dict = {"job_name": "trial_job"}
    actual_value = job_args.get(passed_key)
    assert actual_value == expected_value


@pytest.mark.parametrize(
    "passed_key, error_type",
    [
        ("", ValueError),
        (123, TypeError),
    ],
)
def test_get_success(job_args, passed_key, error_type):
    with pytest.raises(error_type):
        job_args.get(passed_key)


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
def test_get_type_success(job_args, input_type, expected_class):
    result = job_args.get_type(input_type)
    assert result == expected_class


@pytest.mark.parametrize(
    "passed_key, error_type",
    [
        ("", ValueError),
        (123, TypeError),
        ("string123", ValueError),
    ]
)
def test_get_type_success(job_args, passed_key, error_type):
    with pytest.raises(error_type):
        job_args.get_type(passed_key)


def test_get_job_dict(job_args):
    job_args.get_job_dict()
