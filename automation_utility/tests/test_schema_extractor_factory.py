import pytest
from edp_automation.schema_extractor.schema_extractor_factory import SchemaExtractorFactory


def test_factory_invalid_type():
    with pytest.raises(TypeError):
        SchemaExtractorFactory.get_schema_extractor(123)

    with pytest.raises(ValueError):
        SchemaExtractorFactory.get_schema_extractor("")


def test_factory_module_not_found(monkeypatch):
    def dummy_import(name):
        raise ModuleNotFoundError("No module")

    monkeypatch.setattr("importlib.import_module", dummy_import)

    with pytest.raises(ModuleNotFoundError):
        SchemaExtractorFactory.get_schema_extractor("unknown", dummy_key="value")


def test_factory_class_not_found(monkeypatch):
    monkeypatch.setattr("importlib.import_module", lambda x: type("Dummy", (), {})())

    with pytest.raises(ImportError):
        SchemaExtractorFactory.get_schema_extractor("oracle", user="u", password="p", dsn="d", schema_name="s")
