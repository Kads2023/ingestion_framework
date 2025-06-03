from edp_automation.data_type_mapper.base_data_type_mapper import (BaseDataTypeMapper)


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

    def map_type(self, column):
        sql_type = column['data_type'].upper()
        mapping = self.SQLSERVER_TYPE_MAPPING.get(sql_type, "STRING")
        return mapping(column) if callable(mapping) else mapping
