import pytest

from ..tests.fixtures.mock_logger import MockLogger

from edap_common.utils.common_utils import CommonUtils
from edap_common.objects.input_args import InputArgs


default_mandatory_input_params = [
    "common_config_file_location",
    "table_config_file_location"
]

default_values_for_input_params = {
    "env": "dev",
    "ingest_type": "csv",
    "ingest_layer": "bronze",
    "run_date": "2025-04-30",
    "dry_run": "False"
}

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


def test_set_mandatory_input_params(input_args):
    input_args.set_mandatory_input_params(default_mandatory_input_params)
    assert input_args.mandatory_input_params == default_mandatory_input_params


def test_set_default_values_for_input_params(input_args):
    input_args.set_default_values_for_input_params(default_values_for_input_params)
    assert input_args.default_values_for_input_params == default_values_for_input_params


def test_get_value_error(lc, common_utils):
    now_input_args = InputArgs(lc, common_utils, common_config_file_location="")
    now_input_args.set_mandatory_input_params(default_mandatory_input_params)
    with pytest.raises(ValueError):
        now_input_args.get("common_config_file_location")


def test_get_key_error(lc, common_utils):
    now_input_args = InputArgs(lc, common_utils)
    now_input_args.set_mandatory_input_params(default_mandatory_input_params)
    with pytest.raises(KeyError):
        now_input_args.get("common_config_file_location")


@pytest.mark.parametrize(
    "passed_key, expected_value",
    [
        ("env", "dev"),
        ("ingest_type", "csv"),
        ("ingest_layer", "bronze"),
        ("run_date", "2025-04-30"),
        ("dry_run", "False"),
        ("trial", ""),
    ],
)
def test_get_default_value(lc, common_utils, passed_key, expected_value):
    now_input_args = InputArgs(lc, common_utils)
    now_input_args.set_default_values_for_input_params(default_values_for_input_params)
    actual_value = now_input_args.get(passed_key)
    assert expected_value == actual_value
