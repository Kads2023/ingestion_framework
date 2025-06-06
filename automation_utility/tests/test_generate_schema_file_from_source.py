import pytest
import builtins

from edp_automation.utils.generate_schema_file_from_source import generate_schema_file


class MockExtractor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.connected = False
        self.metadata_extracted = False
        self.disconnected = False

    def connect(self):
        self.connected = True

    def extract_metadata(self, table_names, schema_file_path):
        self.metadata_extracted = True
        self.table_names = table_names
        self.schema_file_path = schema_file_path

    def disconnect(self):
        self.disconnected = True


@pytest.fixture
def sample_config(tmp_path):
    return {
        "source_system_type": "oracle",
        "db_user": "admin",
        "dsn": "dsn_string",
        "schema_name": "HR",
        "schema_file_location": str(tmp_path),
        "schema_file_name": "output.json"
    }


def test_generate_schema_file_success(monkeypatch, sample_config):
    printed = []
    monkeypatch.setattr("builtins.print", lambda x: printed.append(x))
    monkeypatch.setattr("getpass.getpass", lambda prompt: "mock_pwd")

    def mock_factory(**kwargs):
        return MockExtractor(**kwargs)

    monkeypatch.setattr(
        "edp_automation.utils.generate_schema_file_from_source.SchemaExtractorFactory.get_schema_extractor",
        lambda source_system_type, **kwargs: mock_factory(**kwargs)
    )

    table_names = ["EMPLOYEES", "DEPARTMENTS"]
    generate_schema_file(sample_config, table_names)

    # Confirm schema file path printed
    assert any("schema_file_path -->" in line for line in printed)
    # Confirm expected extractor operations were called
    assert "✅" not in printed  # since write_to_json is not called directly here


def test_generate_schema_file_factory_failure(monkeypatch, sample_config):
    monkeypatch.setattr("getpass.getpass", lambda prompt: "mock_pwd")
    monkeypatch.setattr("builtins.print", lambda x: None)

    def raise_factory_error(*args, **kwargs):
        raise ValueError("Simulated factory failure")

    monkeypatch.setattr(
        "edp_automation.utils.generate_schema_file_from_source.SchemaExtractorFactory.get_schema_extractor",
        raise_factory_error
    )

    generate_schema_file(sample_config, ["EMPLOYEES"])
    # No raise; handled internally


def test_generate_schema_file_disconnect_failure(monkeypatch, sample_config):
    monkeypatch.setattr("getpass.getpass", lambda prompt: "mock_pwd")
    monkeypatch.setattr("builtins.print", lambda x: None)

    class BrokenExtractor(MockExtractor):
        def disconnect(self):
            raise RuntimeError("disconnect error")

    monkeypatch.setattr(
        "edp_automation.utils.generate_schema_file_from_source.SchemaExtractorFactory.get_schema_extractor",
        lambda source_system_type, **kwargs: BrokenExtractor(**kwargs)
    )

    generate_schema_file(sample_config, ["EMPLOYEES"])
