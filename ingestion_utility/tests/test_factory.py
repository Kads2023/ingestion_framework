import pytest
from unittest.mock import MagicMock
import types

from ..tests.fixtures.mock_logger import MockLogger

from edap_ingest.factory.ingest_factory import IngestFactory
from edap_ingest.ingest.base_ingest import BaseIngest


@pytest.fixture(name="lc", autouse=True)
def fixture_logger():
    return MockLogger()


class DummyIngest(BaseIngest):
    def run_load(self):
        self.lc.logger.info("DummyIngest.run_load() called")


@pytest.mark.parametrize("ingest_type", ["csv", "json", "parquet"])
def test_ingest_factory_success(monkeypatch, lc, ingest_type):
    dummy_utils = MagicMock()

    class DummyInputArgs:
        def get(self, key):
            return ingest_type if key == "ingest_type" else ""

    dummy_input_args = DummyInputArgs()

    # Mock importlib.import_module to return a dummy module
    dummy_module = types.SimpleNamespace()
    class_name = f"{ingest_type.capitalize()}Ingest"
    setattr(dummy_module, class_name, DummyIngest)
    monkeypatch.setattr("importlib.import_module", lambda path: dummy_module)

    factory = IngestFactory()
    factory.start_load(
        lc,
        dummy_input_args,
        passed_job_args={},
        passed_common_utils=dummy_utils,
        passed_process_monitoring=None,
        passed_validation_utils=None,
        passed_dbutils=None
    )


@pytest.mark.parametrize("scenario, import_error, class_missing", [
    ("import_error", True, False),
    ("class_missing", False, True)
])
def test_ingest_factory_failures(monkeypatch, scenario, import_error, class_missing):
    dummy_utils = MagicMock()

    class DummyInputArgs:
        def get(self, key):
            return "csv" if key == "ingest_type" else ""

    dummy_input_args = DummyInputArgs()

    if import_error:
        monkeypatch.setattr("importlib.import_module", lambda path: (_ for _ in ()).throw(ImportError("module not found")))
    elif class_missing:
        dummy_module = types.SimpleNamespace()
        monkeypatch.setattr("importlib.import_module", lambda path: dummy_module)

    factory = IngestFactory()

    with pytest.raises(Exception):
        factory.start_load(
            lc,
            dummy_input_args,
            passed_job_args={},
            passed_common_utils=dummy_utils,
            passed_process_monitoring=None,
            passed_validation_utils=None,
            passed_dbutils=None
        )


@pytest.mark.parametrize("scenario, import_error, class_missing", [
    ("import_error", True, False),
])
def test_ingest_factory_failures_mnf(
        monkeypatch, lc, scenario, import_error, class_missing
):
    dummy_utils = MagicMock()

    class DummyInputArgs:
        def get(self, key):
            return "csv" if key == "ingest_type" else ""

    dummy_input_args = DummyInputArgs()

    if import_error:
        monkeypatch.setattr("importlib.import_module", lambda path: (_ for _ in ()).throw(ModuleNotFoundError("module not found")))
    elif class_missing:
        dummy_module = types.SimpleNamespace()
        monkeypatch.setattr("importlib.import_module", lambda path: dummy_module)

    factory = IngestFactory()

    with pytest.raises(ModuleNotFoundError):
        factory.start_load(
            lc,
            dummy_input_args,
            passed_job_args={},
            passed_common_utils=dummy_utils,
            passed_process_monitoring=None,
            passed_validation_utils=None,
            passed_dbutils=None
        )
