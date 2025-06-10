import pyodbc
from edp_automation.schema_extractor.base_schema_extractor import (BaseSchemaExtractor)


class SQLServerSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, **kwargs):
        self.connection_string = kwargs["connection_string"]
        self.schema_name = kwargs["schema_name"]
        self.connection = None

    def connect(self):
        self.connection = pyodbc.connect(self.connection_string)

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def extract_metadata(self, table_names, output_file_path):
        table_names_upper = [name.upper() for name in table_names]
        i = 0
        placeholders_list = []
        params = {
            "owner_name": self.schema_name
        }
        for each_table_name in table_names_upper:
            now_table_key = f":table_names_list_{i}"
            placeholders_list.append(now_table_key)
            params[now_table_key] = each_table_name
            i += 1

        placeholders = ', '.join(placeholders_list)

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
            WHERE t.TABLE_SCHEMA = :owner_name
              AND t.TABLE_NAME IN ({placeholders})
        """

        print(f"Executing query --> {query}")
        print(f"params --> {params}")

        cursor = self.connection.cursor()
        cursor.execute(query, params)
        columns = [desc[0].lower() for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"results --> {results}")

        self.write_to_json(results, output_file_path)
