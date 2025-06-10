import pytest
from edp_automation.schema_extractor.sql_server_schema_extractor import SQLServerSchemaExtractor


def test_sql_server_schema_extractor_init_validation():
    with pytest.raises(ValueError):
        SQLServerSchemaExtractor(connection_string="x")  # missing schema_name

    with pytest.raises(TypeError):
        SQLServerSchemaExtractor(connection_string=123, schema_name="s")

    with pytest.raises(ValueError):
        SQLServerSchemaExtractor(connection_string="", schema_name="s")


def test_sql_server_connect(monkeypatch):
    monkeypatch.setattr("pyodbc.connect", lambda cs: "dummy_connection")

    extractor = SQLServerSchemaExtractor(connection_string="conn_str", schema_name="s")
    extractor.connect()
    assert extractor.connection == "dummy_connection"


def test_sql_server_extract_metadata(monkeypatch, tmp_path):
    class DummyCursor:
        description = [("COLUMN1",), ("COLUMN2",)]
        def execute(self, query, params): pass
        def fetchall(self): return [("val1", "val2")]
    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass

    monkeypatch.setattr("pyodbc.connect", lambda cs: DummyConnection())

    extractor = SQLServerSchemaExtractor(connection_string="cs", schema_name="schema")
    extractor.connect()
    output_file = tmp_path / "sql_metadata.json"
    extractor.extract_metadata(["tbl"], str(output_file))
    assert output_file.exists()


def test_sql_server_extract_invalid_inputs():
    extractor = SQLServerSchemaExtractor(connection_string="cs", schema_name="schema")

    with pytest.raises(TypeError):
        extractor.extract_metadata("notalist", "out.json")

    with pytest.raises(ValueError):
        extractor.extract_metadata([""], "out.json")

    with pytest.raises(TypeError):
        extractor.extract_metadata(["valid"], 123)

    with pytest.raises(ValueError):
        extractor.extract_metadata(["valid"], " ")
