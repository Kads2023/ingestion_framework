# DeltaIngest Documentation

## Overview

`DeltaIngest` is a data ingestion class designed to read data from Delta tables or SQL queries, apply transformations such as filtering, deduplication, column renaming, and optionally merge or write the final DataFrame to a Delta target table. It extends the `BaseIngest` class and supports complex ETL scenarios in a modular, configurable way.

---

## Class Signature
```python
class DeltaIngest(BaseIngest):
    def read_data_from_source(self) -> DataFrame
    def merge_data_to_target_table(self, source_df: DataFrame)
    def write_data_to_target_table(self, data_to_write: DataFrame)
```

---

## Configuration Options

The class consumes configuration via `job_args_obj`. Below are the supported configuration options with examples:

### 1. `sql_query` (optional)
Executes a parameterized SQL query.
```json
"sql_query": "SELECT * FROM sales_data WHERE sale_date = '{run_date}'"
```

### 2. `source_table` (required if `sql_query` not provided)
Name of the Delta table to read from.
```json
"source_table": "sales_db.sales_data"
```

### 3. `where_clause` (optional)
A string with placeholder variables used to filter data.
```json
"where_clause": "region = 'EU' AND sale_date = '{run_date}'"
```

### 4. `columns` (optional)
List of columns to select from the source.
```json
"columns": ["id", "sale_date", "amount"]
```

### 5. `deduplication` (optional)
Dictionary to enable deduplication logic.
```json
"deduplication": {
  "keys": ["customer_id"],
  "order_by": [
    {"column": "last_updated", "direction": "desc"}
  ]
}
```

### 6. `column_mappings` (optional)
Dictionary to rename columns.
```json
"column_mappings": {
  "cust_id": "customer_id",
  "amt": "amount"
}
```

### 7. `custom_transformations` (optional)
List of Python code snippets applied to the DataFrame.
```json
"custom_transformations": [
  "df = df.withColumn('amount_gbp', F.col('amount') * 0.85)"
]
```

### 8. `merge` (optional)
Enables Delta Lake `MERGE` instead of standard write.
```json
"merge": {
  "target_table": "sales_db.sales_target",
  "merge_keys": ["id"],
  "update_columns": ["amount", "sale_date"],
  "delete_condition": "target.status = 'cancelled'"
}
```

### 9. `write_mode` (optional)
Spark write mode (`append`, `overwrite`, etc.). Default is `append`.
```json
"write_mode": "overwrite"
```

### 10. `partition_by` (optional)
List of columns to partition the target table by.
```json
"partition_by": ["region"]
```

### 11. `dry_run` (optional)
Skips writing/merging if enabled.
```json
"dry_run": true
```

---

## Functional Flow

### 1. `read_data_from_source()`
- Executes SQL or reads from Delta table.
- Applies `where_clause`.
- Projects only the specified `columns`.
- Applies `deduplication` if provided.
- Renames columns using `column_mappings`.
- Executes any `custom_transformations`.

### 2. `merge_data_to_target_table(source_df)`
- Executes Delta Lake `MERGE INTO` using `merge_keys`.
- Applies update, insert, and optionally delete logic.

### 3. `write_data_to_target_table(data_to_write)`
- Writes DataFrame to Delta table using specified mode and partitioning.
- Executes `merge` if configured.

---

## Sample Full Configuration
```json
{
  "source_table": "sales_db.daily_sales",
  "where_clause": "sale_date = '{run_date}'",
  "columns": ["id", "sale_date", "region", "amount"],
  "deduplication": {
    "keys": ["id"],
    "order_by": [
      {"column": "last_updated", "direction": "desc"}
    ]
  },
  "column_mappings": {
    "amount": "sales_amount"
  },
  "custom_transformations": [
    "df = df.withColumn('amount_usd', F.col('sales_amount') * 1.1)"
  ],
  "merge": {
    "target_table": "sales_db.sales_snapshot",
    "merge_keys": ["id"],
    "update_columns": ["sales_amount", "sale_date"],
    "delete_condition": "target.is_active = false"
  },
  "write_mode": "overwrite",
  "partition_by": ["region"],
  "dry_run": false
}
```

---

## Notes
- Template strings in SQL and where clauses support dynamic rendering with `job_args_obj.get_job_dict()`.
- Errors in transformations or merging are raised with clear logger output.
- Ensure column names used in `deduplication`, `mappings`, and `merge_keys` exist in the source schema.

---

## Dependencies
- PySpark
- Delta Lake (`delta.tables`)

---

## Author
`DeltaIngest` is part of the `edap_ingest` module and intended for extensible, declarative ETL pipelines using Delta Lake.

