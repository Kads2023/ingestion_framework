# Both audit and table columns provided with some overlaps.
#
# Only audit columns provided.
#
# Only table columns provided.
#
# Neither provided (empty lists).
#
# Ensures the job_args_obj is set correctly.

import pytest
from unittest.mock import MagicMock

class DummyJobArgs:
    def __init__(self, audit_cols=None, table_cols=None):
        self._store = {}
        self.audit_cols = audit_cols
        self.table_cols = table_cols

    def get(self, key):
        if key == "audit_columns_to_be_added":
            return self.audit_cols
        elif key == "table_columns_to_be_added":
            return self.table_cols
        return None

    def set(self, key, value):
        self._store[key] = value

    def get_set_value(self, key):
        return self._store.get(key)

class DummyLogger:
    def __init__(self):
        self.logger = MagicMock()

class DummyIngest:
    def __init__(self, audit_cols=None, table_cols=None):
        self.job_args_obj = DummyJobArgs(audit_cols, table_cols)
        self.lc = DummyLogger()
        self.this_class_name = "DummyIngest"

    def collate_columns_to_add(self):
        this_module = f"[{self.this_class_name}.collate_columns_to_add()] -"
        self.lc.logger.info(f"Inside {this_module}")

        audit_columns_to_be_added = self.job_args_obj.get("audit_columns_to_be_added") or []
        table_columns_to_be_added = self.job_args_obj.get("table_columns_to_be_added") or []

        table_columns_lookup = {col["column_name"]: col for col in table_columns_to_be_added}

        columns_to_be_added = []

        for audit_col in audit_columns_to_be_added:
            col_name = audit_col["column_name"]
            if col_name in table_columns_lookup:
                columns_to_be_added.append(table_columns_lookup[col_name])
            else:
                columns_to_be_added.append(audit_col)

        audit_col_names = {col["column_name"] for col in audit_columns_to_be_added}
        for table_col in table_columns_to_be_added:
            if table_col["column_name"] not in audit_col_names:
                columns_to_be_added.append(table_col)

        self.job_args_obj.set("columns_to_be_added", columns_to_be_added)


@pytest.mark.parametrize(
    "audit_cols,table_cols,expected_cols",
    [
        # Overlapping column: use table's version, order preserved, plus non-overlapping table cols appended
        (
            [
                {"column_name": "a", "data_type": "int"},
                {"column_name": "b", "data_type": "string"},
            ],
            [
                {"column_name": "b", "data_type": "float", "value": "override"},
                {"column_name": "c", "data_type": "bool"},
            ],
            [
                {"column_name": "a", "data_type": "int"},
                {"column_name": "b", "data_type": "float", "value": "override"},
                {"column_name": "c", "data_type": "bool"},
            ],
        ),
        # Only audit columns, no table columns
        (
            [
                {"column_name": "a", "data_type": "int"},
            ],
            [],
            [
                {"column_name": "a", "data_type": "int"},
            ],
        ),
        # Only table columns, no audit columns
        (
            [],
            [
                {"column_name": "b", "data_type": "float"},
            ],
            [
                {"column_name": "b", "data_type": "float"},
            ],
        ),
        # Both empty
        (
            [],
            [],
            [],
        ),
        # Audit columns, all overridden by table columns
        (
            [
                {"column_name": "x", "data_type": "int"},
                {"column_name": "y", "data_type": "string"},
            ],
            [
                {"column_name": "x", "data_type": "bigint"},
                {"column_name": "y", "data_type": "varchar"},
            ],
            [
                {"column_name": "x", "data_type": "bigint"},
                {"column_name": "y", "data_type": "varchar"},
            ],
        ),
    ],
)
def test_collate_columns_to_add(audit_cols, table_cols, expected_cols):
    ingest = DummyIngest(audit_cols=audit_cols, table_cols=table_cols)
    ingest.collate_columns_to_add()

    result = ingest.job_args_obj.get_set_value("columns_to_be_added")
    assert result == expected_cols
