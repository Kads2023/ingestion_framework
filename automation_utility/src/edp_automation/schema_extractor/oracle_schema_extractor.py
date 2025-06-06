import oracledb
from oracledb import ConnectParams
from edp_automation.schema_extractor.base_schema_extractor import (BaseSchemaExtractor)


class OracleSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, **kwargs):
        self.user = kwargs["user"]
        self.password = kwargs["password"]
        self.dsn = kwargs["dsn"]
        self.schema_name = kwargs["schema_name"]
        self.connection = None

    def connect(self):
        ld = r"C:\Apps\Oracle19\x64\Client\19.0\bin"
        oracledb.init_oracle_client(lib_dir=ld)
        self.connection = oracledb.connect(
            params=ConnectParams(user=self.user,password=self.password),
            dsn=self.dsn
        )

    def disconnect(self):
        if self.connection:
            self.connection.close()

    def extract_metadata(self, table_names, output_file_path):
        table_names_str = "', '".join([name.upper() for name in table_names])

        query = f"""
            SELECT 
                c.TABLE_NAME "table_name",
                c.COLUMN_NAME "column_name",
                c.DATA_TYPE "data_type",
                c.DATA_LENGTH "data_length",
                c.DATA_PRECISION "data_precision",
                c.DATA_SCALE "data_scale",
                c.NULLABLE "nullable",
                c.IDENTITY_COLUMN "identity_column",
                cm.comments "comments"
            FROM ALL_TAB_COLUMNS c
            LEFT JOIN ALL_COL_COMMENTS cm
                ON c.owner = cm.owner
                AND c.table_name = cm.table_name
                AND c.column_name = cm.column_name
            WHERE c.table_name IN ('{table_names_str}')
              AND c.owner = '{self.schema_name}'
        """

        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = [desc[0].lower() for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]

        self.write_to_json(results, output_file_path)
