from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper


class SQLServerDataTypeMapper(BaseDataTypeMapper):

    SQLSERVER_TYPE_MAPPING = {
        "INT": "INT",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "TINYINT",
        "DECIMAL": lambda col: f"DECIMAL({col.get('data_precision', 10)},{col.get('data_scale', 0)})",
        "NUMERIC": lambda col: f"DECIMAL({col.get('data_precision', 10)},{col.get('data_scale', 0)})",
        "VARCHAR": "STRING",
        "NVARCHAR": "STRING",
        "CHAR": "STRING",
        "NCHAR": "STRING",
        "TEXT": "STRING",
        "NTEXT": "STRING",
        "DATETIME": "TIMESTAMP",
        "SMALLDATETIME": "TIMESTAMP",
        "DATE": "TIMESTAMP",
        "TIME": "TIMESTAMP",
        "DATETIME2": "TIMESTAMP",
        "BIT": "BOOLEAN",
        "FLOAT": "DOUBLE",
        "REAL": "DOUBLE",
        "VARBINARY": "BINARY",
        "BINARY": "BINARY",
        "IMAGE": "BINARY"
    }

    def map_type(self, column_metadata):
        if not isinstance(column_metadata, dict):
            raise TypeError("Expected 'column_metadata' to be a dictionary")

        if 'data_type' not in column_metadata:
            raise KeyError("Missing required key: 'data_type' in column_metadata")

        sql_type = column_metadata['data_type'].upper()
        if sql_type not in self.SQLSERVER_TYPE_MAPPING:
            raise ValueError(f"Unsupported SQL Server data type: '{sql_type}'")

        mapping = self.SQLSERVER_TYPE_MAPPING[sql_type]
        return mapping(column_metadata) if callable(mapping) else mapping
