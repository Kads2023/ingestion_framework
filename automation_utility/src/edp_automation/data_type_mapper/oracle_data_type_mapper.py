from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper


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

    def map_type(self, column_metadata):
        if not isinstance(column_metadata, dict):
            raise TypeError("Expected 'column_metadata' to be a dictionary")

        if 'data_type' not in column_metadata:
            raise KeyError("Missing required key: 'data_type' in column_metadata")

        oracle_type = column_metadata['data_type'].upper()
        if oracle_type not in self.ORACLE_TYPE_MAPPING:
            raise ValueError(f"Unsupported Oracle data type: '{oracle_type}'")

        mapping = self.ORACLE_TYPE_MAPPING[oracle_type]
        return mapping(column_metadata) if callable(mapping) else mapping
