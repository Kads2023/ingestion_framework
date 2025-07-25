# SqlBatchIngest

## Overview

`SqlBatchIngest` is a Spark-based ingestion class designed to execute a **batch of SQL statements** from files in a configurable and monitored workflow. It builds on a shared ingestion base class (`BaseIngest`) and adds batch processing, validation, and monitoring capabilities.

---

## Class: `SqlBatchIngest`

```
from edap_ingest.ingest.base_ingest import BaseIngest
from pyspark.sql import SparkSession, DataFrame as Spark_DataFrame
import sqlglot
```

### Constructor

```
def __init__(self, lc, input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils=None)
```

- Initializes logging, job metadata, utilities, and validation components.
- Inherits from `BaseIngest`, which handles configuration and Spark context setup.

---

## Method: `run_batch()`

```
def run_batch(self):
```

Executes the batch ingestion pipeline:

1. Calls `pre_load()` to set up configs and monitor job start.
2. Iterates over a list of SQL steps defined in the job configuration.
3. For each step:
   - Reads SQL file path and metadata (catalog, schema, table, validations).
   - Parses SQL using `sqlglot` to ensure validity.
   - Executes SQL using Spark.
   - Runs data validation checks (schema, nulls, etc.).
   - Writes valid data to the target table.
   - If validation fails, writes invalid records to a quarantine table.
4. Errors are logged, and failed steps update monitoring.
5. After all steps, calls `post_load()` to mark job completion.

---

## Configuration Format

Example YAML or JSON config that feeds into `job_args["sqls"]`:

```yaml
sqls:
  - name: "step_1"
    sql_file_location: "/dbfs/path/to/query1.sql"
    target_catalog: "my_catalog"
    target_schema: "finance"
    target_table: "customers"
    validations:
      - name: "schema_check"

  - name: "step_2"
    sql_file_location: "/dbfs/path/to/query2.sql"
    target_catalog: "my_catalog"
    target_schema: "sales"
    target_table: "transactions"
    validations:
      - name: "null_check"
```

---

## Input Parameters

| Parameter                      | Description                                  |
|-------------------------------|----------------------------------------------|
| `sqls`                        | List of step dictionaries (see config format)|
| `run_date`                    | Runtime for dynamic path formatting          |
| `job_id`                      | Unique ID for tracking and monitoring        |
| `common_config_file_location`| Path to global config YAML file              |
| `table_config_file_location` | Path to table-specific config                |

---

## Validation Behavior

Each SQL step may define a list of validation rules:
- `schema_check`
- `null_check`
- Custom validations depending on your framework.

If validation fails:
- Data is written to the **quarantine table** (auto-derived or configured).
- Metadata and error counts are updated in the monitoring store.

---

## Monitoring & Status Tracking

Uses `process_monitoring_obj` methods to track:
- Job start/completion/failure
- Each step’s failure reason (SQL parse, execution, validation)
- Optional status insertions for each SQL step

---

## Testing

Use `pytest` with `monkeypatch` and `MagicMock` for mocking:

```bash
pytest test_sql_batch_ingest.py -v
```

Test cases include:
- All steps succeed ✅
- Validation fails and writes to quarantine ⚠️
- SQL parsing fails ❌

---

## Inline Documentation

### Class Docstring

```python
class SqlBatchIngest(BaseIngest):
    """
    SqlBatchIngest executes a list of SQL files as batch steps, with monitoring,
    validation, and conditional writes to target or quarantine tables.

    Inherits from:
        BaseIngest: Core class handling Spark session, monitoring, and config I/O.

    Methods:
        run_batch(): Iterates through configured SQL steps and executes them sequentially.
    """
```

### Method Docstring

```python
def run_batch(self):
    """
    Main execution method for batch SQL ingestion steps.

    For each SQL step in the config:
        - Reads SQL file.
        - Parses SQL using sqlglot.
        - Executes SQL on Spark.
        - Runs data validations.
        - Writes valid data to target table.
        - Writes invalid data to quarantine table (if validation fails).
        - Logs errors and updates monitoring status.

    Raises:
        Exception: if any step fails and raise_exception is set.
    """
```