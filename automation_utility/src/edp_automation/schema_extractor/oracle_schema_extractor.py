import oracledb
from oracledb import ConnectParams
from edp_automation.schema_extractor.base_schema_extractor import (BaseSchemaExtractor)


class OracleSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, **kwargs):
        required_keys = ['user', 'password', 'dsn', 'schema_name']
        for key in required_keys:
            if key not in kwargs:
                raise ValueError(f"Missing required parameter: {key}")
            if not isinstance(kwargs[key], str):
                raise TypeError(f"{key} must be a string")
            if not kwargs[key].strip():
                raise ValueError(f"{key} cannot be empty")

        self.user = kwargs["user"]
        self.password = kwargs["password"]
        self.dsn = kwargs["dsn"]
        self.schema_name = kwargs["schema_name"]
        self.connection = None

    def connect(self):
        try:
            oracledb.init_oracle_client(lib_dir=r"C:\Apps\Oracle19\x64\Client\19.0\bin")
            self.connection = oracledb.connect(
                params=ConnectParams(user=self.user, password=self.password),
                dsn=self.dsn
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Oracle DB: {e}")

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            raise ConnectionError(f"Error while closing Oracle connection: {e}")

    def extract_metadata(self, table_names, output_file_path):
        if not isinstance(table_names, list):
            raise TypeError("`table_names` must be a list of strings")
        if not all(isinstance(t, str) and t.strip() for t in table_names):
            raise ValueError("Each item in `table_names` must be a non-empty string")
        if not isinstance(output_file_path, str):
            raise TypeError("`output_file_path` must be a string")
        if not output_file_path.strip():
            raise ValueError("`output_file_path` must be a non-empty string")

        try:
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
                WHERE c.table_name IN ({placeholders})
                  AND c.owner = :owner_name
            """

            print(f"Executing query --> {query}")
            print(f"params --> {params}")

            cursor = self.connection.cursor()
            cursor.execute(query, params)
            columns = [desc[0].lower() for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            print(f"results --> {results}")

            self.write_to_json(results, output_file_path)
        except oracledb.Error as e:
            raise RuntimeError(f"Failed to execute query: {e}")
