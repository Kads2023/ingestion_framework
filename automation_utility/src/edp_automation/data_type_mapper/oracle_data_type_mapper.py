from edp_automation.data_type_mapper.base_data_type_mapper import (BaseDataTypeMapper)


class OracleDataTypeMapper(BaseDataTypeMapper):

    ORACLE_TYPE_MAPPING = {
        "NUMBER": lambda col: f"DECIMAL({col.get('data_precision', 10)},{col.get('data_scale', 0)})"
        if col.get("data_precision") is not None else "BIGINT",
        "VARCHAR2": "STRING",
        "CHAR": "STRING",
        "CLOB": "STRING",
        "LONG": "STRING",
        "DATE": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
        "TIMESTAMP WITH LOCAL TIME ZONE": "TIMESTAMP",
        "FLOAT": "DOUBLE",
        "BINARY_FLOAT": "FLOAT",
        "BINARY_DOUBLE": "DOUBLE",
        "BLOB": "BINARY",
        "RAW": "BINARY",
        "BOOLEAN": "BOOLEAN",
        "INTERVAL YEAR TO MONTH": "STRING",
        "INTERVAL DAY TO SECOND": "STRING",
    }

    def map_type(self, column):
        oracle_type = column['data_type'].upper()
        mapping = self.ORACLE_TYPE_MAPPING.get(oracle_type, "STRING")
        return mapping(column) if callable(mapping) else mapping
