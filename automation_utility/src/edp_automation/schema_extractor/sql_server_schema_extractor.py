import pyodbc
from edp_automation.schema_extractor.base_schema_extractor import (BaseSchemaExtractor)


class SQLServerSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, connection_string, schema_owner='dbo'):
        self.connection_string = connection_string
        self.schema_owner = schema_owner
        self.connection = None

    def connect(self):
        self.connection = pyodbc.connect(self.connection_string)

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def extract_metadata(self, table_names, output_file='output_sqlserver_schema.json'):
        table_names_str = "', '".join([name.upper() for name in table_names])

        query = f"""
            SELECT 
                t.TABLE_NAME as table_name,
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.CHARACTER_MAXIMUM_LENGTH as data_length,
                c.NUMERIC_PRECISION as data_precision,
                c.NUMERIC_SCALE as data_scale,
                c.IS_NULLABLE as nullable,
                CASE 
                    WHEN ic.COLUMN_NAME IS NOT NULL THEN 'YES'
                    ELSE 'NO'
                END as identity_column,
                ep.value as comments
            FROM INFORMATION_SCHEMA.COLUMNS c
            JOIN INFORMATION_SCHEMA.TABLES t ON c.TABLE_NAME = t.TABLE_NAME
            LEFT JOIN sys.identity_columns ic ON ic.name = c.COLUMN_NAME
            LEFT JOIN sys.extended_properties ep 
                ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = ep.major_id
                AND ep.minor_id = c.ORDINAL_POSITION
            WHERE t.TABLE_SCHEMA = ?
              AND t.TABLE_NAME IN ('{table_names_str}')
        """

        cursor = self.connection.cursor()
        cursor.execute(query, self.schema_owner)
        columns = [desc[0].lower() for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        self.write_to_json(results, output_file)
