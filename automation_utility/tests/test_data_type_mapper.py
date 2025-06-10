import pytest
from types import ModuleType
from edp_automation.data_type_mapper.oracle_data_type_mapper import OracleDataTypeMapper
from edp_automation.data_type_mapper.sql_server_data_type_mapper import SQLServerDataTypeMapper
from edp_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory
from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper

# =========================
# BaseDataTypeMapper Tests
# =========================

def test_base_data_type_mapper_not_implemented():
    mapper = BaseDataTypeMapper()
    with pytest.raises(NotImplementedError, match="Subclasses must implement this method"):
        mapper.map_type({"data_type": "NUMBER"})


# =========================
# OracleDataTypeMapper Tests
# =========================

@pytest.mark.parametrize("input_col, expected_type", [
    ({"data_type": "NUMBER", "data_precision": 10, "data_scale": 2}, "DECIMAL(10,2)"),
    ({"data_type": "NUMBER"}, "BIGINT"),
    ({"data_type": "VARCHAR2"}, "STRING"),
    ({"data_type": "CHAR"}, "STRING"),
    ({"data_type": "CLOB"}, "STRING"),
    ({"data_type": "DATE"}, "TIMESTAMP"),
    ({"data_type": "TIMESTAMP"}, "TIMESTAMP"),
    ({"data_type": "TIMESTAMP WITH TIME ZONE"}, "TIMESTAMP"),
    ({"data_type": "FLOAT"}, "DOUBLE"),
    ({"data_type": "BINARY_FLOAT"}, "FLOAT"),
    ({"data_type": "BINARY_DOUBLE"}, "DOUBLE"),
    ({"data_type": "BLOB"}, "BINARY"),
    ({"data_type": "RAW"}, "BINARY"),
    ({"data_type": "BOOLEAN"}, "BOOLEAN"),
    ({"data_type": "INTERVAL YEAR TO MONTH"}, "STRING"),
])
def test_oracle_data_type_mapper(input_col, expected_type):
    mapper = OracleDataTypeMapper()
    result = mapper.map_type(input_col)
    assert result == expected_type


def test_oracle_data_type_mapper_invalid_type():
    mapper = OracleDataTypeMapper()
    with pytest.raises(ValueError, match="Unsupported Oracle data type"):
        mapper.map_type({"data_type": "UNSUPPORTED"})


def test_oracle_data_type_mapper_missing_key():
    mapper = OracleDataTypeMapper()
    with pytest.raises(KeyError, match="Missing required key"):
        mapper.map_type({})


def test_oracle_data_type_mapper_wrong_type():
    mapper = OracleDataTypeMapper()
    with pytest.raises(TypeError, match="Expected 'column_metadata' to be a dictionary"):
        mapper.map_type("not_a_dict")


# =========================
# SQLServerDataTypeMapper Tests
# =========================

@pytest.mark.parametrize(
    "column_metadata, expected_output",
    [
        ({"data_type": "INT"}, "INT"),
        ({"data_type": "BIGINT"}, "BIGINT"),
        ({"data_type": "SMALLINT"}, "SMALLINT"),
        ({"data_type": "TINYINT"}, "TINYINT"),
        ({"data_type": "DECIMAL", "data_precision": 12, "data_scale": 3}, "DECIMAL(12,3)"),
        ({"data_type": "NUMERIC", "data_precision": 12, "data_scale": 3}, "DECIMAL(12,3)"),
        ({"data_type": "VARCHAR"}, "STRING"),
        ({"data_type": "NVARCHAR"}, "STRING"),
        ({"data_type": "CHAR"}, "STRING"),
        ({"data_type": "NCHAR"}, "STRING"),
        ({"data_type": "TEXT"}, "STRING"),
        ({"data_type": "NTEXT"}, "STRING"),
        ({"data_type": "DATETIME"}, "TIMESTAMP"),
        ({"data_type": "SMALLDATETIME"}, "TIMESTAMP"),
        ({"data_type": "DATE"}, "TIMESTAMP"),
        ({"data_type": "TIME"}, "TIMESTAMP"),
        ({"data_type": "DATETIME2"}, "TIMESTAMP"),
        ({"data_type": "BIT"}, "BOOLEAN"),
        ({"data_type": "FLOAT"}, "DOUBLE"),
        ({"data_type": "REAL"}, "DOUBLE"),
        ({"data_type": "VARBINARY"}, "BINARY"),
        ({"data_type": "BINARY"}, "BINARY"),
        ({"data_type": "IMAGE"}, "BINARY"),
    ]
)
def test_sqlserver_mapper_valid(column_metadata, expected_output):
    mapper = SQLServerDataTypeMapper()
    result = mapper.map_type(column_metadata)
    assert result == expected


def test_sql_server_data_type_mapper_invalid_type():
    mapper = SQLServerDataTypeMapper()
    with pytest.raises(ValueError, match="Unsupported SQL Server data type"):
        mapper.map_type({"data_type": "INVALID_TYPE"})


def test_sql_server_data_type_mapper_missing_key():
    mapper = SQLServerDataTypeMapper()
    with pytest.raises(KeyError, match="Missing required key"):
        mapper.map_type({})


def test_sql_server_data_type_mapper_wrong_type():
    mapper = SQLServerDataTypeMapper()
    with pytest.raises(TypeError, match="Expected 'column_metadata' to be a dictionary"):
        mapper.map_type(["not", "a", "dict"])


# =========================
# DataTypeMapperFactory Tests
# =========================

def test_factory_returns_correct_mapper(monkeypatch):
    class DummyMapper(BaseDataTypeMapper):
        def map_type(self, column): return "dummy"

    dummy_module = ModuleType("dummy_module")
    setattr(dummy_module, "DummyDataTypeMapper", DummyMapper)

    def dummy_import(name):
        if name == "edp_automation.data_type_mapper.dummy_data_type_mapper":
            return dummy_module
        raise ModuleNotFoundError(f"No module named '{name}'")

    monkeypatch.setattr("importlib.import_module", dummy_import)

    mapper = DataTypeMapperFactory.get_mapper("dummy")
    assert isinstance(mapper, DummyMapper)
    assert mapper.map_type({"data_type": "X"}) == "dummy"


def test_factory_module_not_found(monkeypatch):
    def raise_module_not_found(name):
        raise ModuleNotFoundError("Not found")

    monkeypatch.setattr("importlib.import_module", raise_module_not_found)

    with pytest.raises(ModuleNotFoundError):
        DataTypeMapperFactory.get_mapper("invalid")


def test_factory_class_not_found(monkeypatch):
    # valid module, missing class
    dummy_module = ModuleType("dummy_module")

    def dummy_import(name):
        return dummy_module

    monkeypatch.setattr("importlib.import_module", dummy_import)

    with pytest.raises(TypeError):  # trying to call NoneType
        DataTypeMapperFactory.get_mapper("dummy")

def test_data_type_mapper_factory_oracle(monkeypatch):
    from edp_automation.data_type_mapper import oracle_data_type_mapper
    monkeypatch.setattr(oracle_data_type_mapper, "OracleDataTypeMapper", OracleDataTypeMapper)
    mapper = DataTypeMapperFactory.get_mapper("oracle")
    assert isinstance(mapper, OracleDataTypeMapper)


def test_data_type_mapper_factory_sqlserver(monkeypatch):
    from edp_automation.data_type_mapper import sql_server_data_type_mapper
    monkeypatch.setattr(sql_server_data_type_mapper, "SQLServerDataTypeMapper", SQLServerDataTypeMapper)
    mapper = DataTypeMapperFactory.get_mapper("sql_server")
    assert isinstance(mapper, SQLServerDataTypeMapper)


def test_data_type_mapper_factory_invalid_module():
    with pytest.raises(ModuleNotFoundError):
        DataTypeMapperFactory.get_mapper("unknown_db")


def test_data_type_mapper_factory_empty_input():
    with pytest.raises(ValueError, match="Invalid source_system_type"):
        DataTypeMapperFactory.get_mapper("")


def test_data_type_mapper_factory_wrong_type():
    with pytest.raises(TypeError, match="Expected source_system_type"):
        DataTypeMapperFactory.get_mapper(123)
