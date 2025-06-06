import pytest
import builtins
from types import ModuleType
from edp_automation.schema_extractor.base_schema_extractor import BaseSchemaExtractor
from edp_automation.schema_extractor.schema_extractor_factory import SchemaExtractorFactory


# ======= BaseSchemaExtractor Tests =======

def test_write_to_json(monkeypatch, tmp_path):
    data = [{"table_name": "TEST", "column_name": "ID"}]
    file_path = tmp_path / "output.json"

    printed = []
    monkeypatch.setattr(builtins, "print", lambda x: printed.append(x))

    class DummyExtractor(BaseSchemaExtractor):
        def __init__(self): pass
        def connect(self): pass
        def disconnect(self): pass
        def extract_metadata(self, table_names, output_file): pass

    extractor = DummyExtractor()
    extractor.write_to_json(data, str(file_path))

    with open(file_path) as f:
        content = f.read()
        assert '"table_name": "TEST"' in content

    assert any("✅ Metadata written to" in line for line in printed)


# ======= OracleSchemaExtractor Tests =======

def test_oracle_schema_extractor(monkeypatch, tmp_path):
    from edp_automation.schema_extractor.oracle_schema_extractor import OracleSchemaExtractor

    mock_cursor = type("Cursor", (), {
        "description": [("TABLE_NAME",), ("COLUMN_NAME",)],
        "fetchall": lambda self: [("EMP", "ID")],
        "execute": lambda self, query: None
    })()

    mock_connection = type("Connection", (), {
        "cursor": lambda self: mock_cursor,
        "close": lambda self: None
    })()

    monkeypatch.setattr("oracledb.init_oracle_client", lambda lib_dir: None)
    monkeypatch.setattr("oracledb.connect", lambda **kwargs: mock_connection)

    kwargs = {"user": "u", "password": "p", "dsn": "d", "schema_name": "s"}
    extractor = OracleSchemaExtractor(**kwargs)

    extractor.connect()
    assert extractor.connection == mock_connection

    out_path = tmp_path / "oracle_output.json"
    extractor.extract_metadata(["emp"], str(out_path))
    assert out_path.read_text().strip().startswith("[")
    extractor.disconnect()


# ======= SQLServerSchemaExtractor Tests =======

def test_sql_server_schema_extractor(monkeypatch, tmp_path):
    from edp_automation.schema_extractor.sql_server_schema_extractor import SQLServerSchemaExtractor

    mock_cursor = type("Cursor", (), {
        "description": [("TABLE_NAME",), ("COLUMN_NAME",)],
        "fetchall": lambda self: [("EMP", "ID")],
        "execute": lambda self, query, param: None
    })()

    mock_connection = type("Connection", (), {
        "cursor": lambda self: mock_cursor,
        "close": lambda self: None
    })()

    monkeypatch.setattr("pyodbc.connect", lambda conn_str: mock_connection)

    kwargs = {"connection_string": "Driver={SQL Server}", "schema_name": "dbo"}
    extractor = SQLServerSchemaExtractor(**kwargs)

    extractor.connect()
    assert extractor.connection == mock_connection

    out_path = tmp_path / "sql_output.json"
    extractor.extract_metadata(["emp"], str(out_path))
    assert out_path.read_text().strip().startswith("[")
    extractor.disconnect()


# ======= SchemaExtractorFactory Tests =======

def test_schema_extractor_factory_success(monkeypatch):
    class DummyExtractor(BaseSchemaExtractor):
        def __init__(self, **kwargs): self.kwargs = kwargs
        def connect(self): pass
        def disconnect(self): pass
        def extract_metadata(self, table_names, output_file): pass

    dummy_module = ModuleType("dummy_module")
    setattr(dummy_module, "DummySchemaExtractor", DummyExtractor)

    def dummy_import(name):
        if name == "edp_automation.schema_extractor.dummy_schema_extractor":
            return dummy_module
        raise ImportError

    monkeypatch.setattr("importlib.import_module", dummy_import)

    extractor = SchemaExtractorFactory.get_schema_extractor("dummy", foo="bar")
    assert isinstance(extractor, DummyExtractor)
    assert extractor.kwargs["foo"] == "bar"


def test_schema_extractor_factory_module_not_found(monkeypatch):
    def raise_module_not_found(name):
        raise ModuleNotFoundError("module not found")

    monkeypatch.setattr("importlib.import_module", raise_module_not_found)

    with pytest.raises(ModuleNotFoundError):
        SchemaExtractorFactory.get_schema_extractor("invalid")


def test_schema_extractor_factory_class_not_found(monkeypatch):
    dummy_module = ModuleType("dummy_module")

    def dummy_import(name):
        return dummy_module

    monkeypatch.setattr("importlib.import_module", dummy_import)

    with pytest.raises(TypeError):  # trying to call None
        SchemaExtractorFactory.get_schema_extractor("dummy")
