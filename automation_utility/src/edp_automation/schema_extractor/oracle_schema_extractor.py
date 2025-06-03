import pyodbc
from edp_automation.schema_extractor.base_schema_extractor import (BaseSchemaExtractor)


class OracleSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, user, password, dsn, schema_owner):
        self.user = user
        self.password = password
        self.dsn = dsn  # This should be a valid ODBC DSN or connection string
        self.schema_owner = schema_owner
        self.connection = None

    def connect(self):
        self.connection = pyodbc.connect(
            f"DSN={self.dsn};UID={self.user};PWD={self.password}"
        )

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def extract_metadata(self, table_names, output_file='output_oracle_schema.json'):
        placeholders = ','.join(['?'] * len(table_names))
        table_names_upper = [name.upper() for name in table_names]

        query = f"""
            SELECT 
                c.table_name,
                c.column_name,
                c.data_type,
                c.data_length,
                c.data_precision,
                c.data_scale,
                c.nullable,
                c.identity_column,
                cm.comments
            FROM ALL_TAB_COLUMNS c
            LEFT JOIN ALL_COL_COMMENTS cm
                ON c.owner = cm.owner
                AND c.table_name = cm.table_name
                AND c.column_name = cm.column_name
            WHERE c.table_name IN ({placeholders})
              AND c.owner = ?
        """

        cursor = self.connection.cursor()
        cursor.execute(query, table_names_upper + [self.schema_owner])
        columns = [desc[0].lower() for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        self.write_to_json(results, output_file)
