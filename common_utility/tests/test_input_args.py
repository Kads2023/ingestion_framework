import pytest

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.objects.input_args import InputArgs


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


@pytest.mark.parametrize(
    "passed_key, expected_value",
    [
        ("job_name", "trial_job"),
        ("frequency", "trial_frequency"),
        ("load_type", "trial_load"),
        ("source_system", "trial_source"),
        ("source", "trial_file"),
        ("source_type", "file"),
    ],
)
def test_get_success(input_args, passed_key, expected_value):
    actual_value = input_args.get(passed_key)
    assert actual_value == expected_value
