import pytest
from edap_ingest.ingest.ingest_factory import IngestFactory
from edap_ingest.ingest.base_ingest import BaseIngest


class DummyIngest(BaseIngest):
    def run_load(self):
        self.common_utils.log_msg("DummyIngest.run_load called.")


@pytest.fixture
def dummy_common_utils():
    class DummyCommonUtils:
        def log_msg(self, msg):
            print(f"LOG: {msg}")
    return DummyCommonUtils()


@pytest.fixture
def dummy_process_monitoring():
    class DummyProcessMonitoring:
        pass
    return DummyProcessMonitoring()


@pytest.fixture
def dummy_validation_utils():
    class DummyValidationUtils:
        pass
    return DummyValidationUtils()


@pytest.fixture
def dummy_dbutils():
    class DummyDbUtils:
        pass
    return DummyDbUtils()


@pytest.mark.parametrize(
    "ingest_type, expect_success",
    [
        ("dummy", True),   # Should work fine
        ("invalid", False)  # Should fail (class not found)
    ]
)
def test_start_load(
    monkeypatch,
    ingest_type,
    expect_success,
    dummy_common_utils,
    dummy_process_monitoring,
    dummy_validation_utils,
    dummy_dbutils
):
    factory = IngestFactory()

    input_args = {"ingest_type": ingest_type}
    job_args = {"job_id": 123}

    # Setup dummy module
    class DummyModule:
        DummyIngest = DummyIngest

    # Monkeypatch importlib.import_module
    monkeypatch.setattr("importlib.import_module", lambda name: DummyModule)

    # Monkeypatch getattr differently based on test case
    def fake_getattr(module, name, default=None):
        if expect_success:
            return getattr(module, "DummyIngest", default)
        else:
            return None  # Simulate class not found

    monkeypatch.setattr("builtins.getattr", fake_getattr)

    if expect_success:
        # Act
        factory.start_load(
            input_args,
            job_args,
            dummy_common_utils,
            dummy_process_monitoring,
            dummy_validation_utils,
            dummy_dbutils
        )
    else:
        # Act + Assert
        with pytest.raises(TypeError):
            factory.start_load(
                input_args,
                job_args,
                dummy_common_utils,
                dummy_process_monitoring,
                dummy_validation_utils,
                dummy_dbutils
            )
# What's Happening Here:
# Single test function: handles both success and failure flows.
#
# Parametrized with @pytest.mark.parametrize.
#
# Monkeypatch both importlib.import_module and getattr.
#
# If expect_success is True → expect start_load() to work fine.
#
# If expect_success is False → expect it to raise TypeError because class_ref would be None.
#
# ✅ Full coverage: success + failure paths.
#
# ✅ No real imports (everything faked).
#
# ✅ Reusable fixtures for all dependencies.
#
# ⚡ Bonus Tip:
# If you want even stronger checks, you can capture logs using capsys and assert that "DummyIngest.run_load called." appears in the console!
#
# Example:
#
# python
# Copy
# Edit
def test_logging_output(capsys, monkeypatch, dummy_common_utils, dummy_process_monitoring, dummy_validation_utils, dummy_dbutils):
    factory = IngestFactory()

    input_args = {"ingest_type": "dummy"}
    job_args = {}

    class DummyModule:
        DummyIngest = DummyIngest

    monkeypatch.setattr("importlib.import_module", lambda name: DummyModule)
    monkeypatch.setattr("builtins.getattr", lambda module, name, default=None: getattr(module, "DummyIngest", default))

    factory.start_load(
        input_args,
        job_args,
        dummy_common_utils,
        dummy_process_monitoring,
        dummy_validation_utils,
        dummy_dbutils
    )

    captured = capsys.readouterr()
    assert "DummyIngest.run_load called." in captured.out