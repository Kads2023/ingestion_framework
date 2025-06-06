import pytest
from types import ModuleType
from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper
from edp_automation.data_type_mapper.oracle_data_type_mapper import OracleDataTypeMapper
from edp_automation.data_type_mapper.data_type_mapper_factory import DataTypeMapperFactory


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
    ({"data_type": "UNKNOWN_TYPE"}, "STRING"),
])
def test_oracle_data_type_mapper(input_col, expected_type):
    mapper = OracleDataTypeMapper()
    result = mapper.map_type(input_col)
    assert result == expected_type


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
