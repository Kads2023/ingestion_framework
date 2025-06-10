import pytest
from edp_automation.schema_extractor.oracle_schema_extractor import OracleSchemaExtractor


def test_oracle_schema_extractor_init_validation():
    with pytest.raises(ValueError):
        OracleSchemaExtractor(user="u", password="p", dsn="d")  # missing schema_name

    with pytest.raises(TypeError):
        OracleSchemaExtractor(user=123, password="p", dsn="d", schema_name="s")  # user not string

    with pytest.raises(ValueError):
        OracleSchemaExtractor(user=" ", password="p", dsn="d", schema_name="s")  # empty user


def test_oracle_connect(monkeypatch):
    class DummyConnect:
        def cursor(self): return DummyCursor()
        def close(self): pass

    def dummy_connect(*args, **kwargs):
        return DummyConnect()

    monkeypatch.setattr("oracledb.init_oracle_client", lambda lib_dir: None)
    monkeypatch.setattr("oracledb.connect", dummy_connect)

    extractor = OracleSchemaExtractor(user="u", password="p", dsn="d", schema_name="s")
    extractor.connect()
    assert extractor.connection is not None


def test_oracle_extract_metadata(monkeypatch, tmp_path):
    class DummyCursor:
        description = [("COLUMN1",), ("COLUMN2",)]
        def execute(self, query, params): pass
        def fetchall(self): return [("val1", "val2")]
    class DummyConnection:
        def cursor(self): return DummyCursor()
        def close(self): pass

    monkeypatch.setattr("oracledb.init_oracle_client", lambda lib_dir: None)
    monkeypatch.setattr("oracledb.connect", lambda *a, **kw: DummyConnection())

    extractor = OracleSchemaExtractor(user="u", password="p", dsn="d", schema_name="s")
    extractor.connect()
    output_file = tmp_path / "metadata.json"
    extractor.extract_metadata(["table1"], str(output_file))
    assert output_file.exists()


def test_oracle_extract_invalid_inputs():
    extractor = OracleSchemaExtractor(user="u", password="p", dsn="d", schema_name="s")

    with pytest.raises(TypeError):
        extractor.extract_metadata("notalist", "out.json")

    with pytest.raises(ValueError):
        extractor.extract_metadata([""], "out.json")

    with pytest.raises(TypeError):
        extractor.extract_metadata(["valid"], 123)

    with pytest.raises(ValueError):
        extractor.extract_metadata(["valid"], " ")
