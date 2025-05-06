
# 📘 Detailed Documentation: `ProcessMonitoring`

## 📍 Module Overview

The `ProcessMonitoring` class is a utility component for monitoring, tracking, and logging the execution metadata of ETL/ELT jobs running in a PySpark environment. It integrates with Azure SQL Database to store and retrieve job and run metadata for auditing, debugging, and orchestration.

It supports:
- Secure Azure AD-based DB authentication
- Execution of SQL queries via `pyodbc`
- Logging and validation
- Dynamic Spark DataFrame generation
- Insertion and updating of job/run details

---

## 🧱 Class: `ProcessMonitoring`

### 🔧 Constructor

```python
def __init__(self, lc, common_utils, job_args)
```

#### Arguments:
- **`lc`**: Logging context (object with a `.logger` supporting `info`, `error` methods).
- **`common_utils`**: Helper utilities (must have `validate_function_param` and `get_current_time` methods).
- **`job_args`**: Parameter management object supporting `.get()` and `.set()` for dynamic key-value pairs used in job execution.

---

### 🏗️ Attributes

- `self.spark`: Active `SparkSession` from the current context.
- `self.schema`: An empty Spark `StructType` schema.
- `self.empty_rdd`: An empty RDD used to create an empty DataFrame.
- `self.empty_df`: Empty Spark DataFrame with `self.schema`.
- `self.empty_dict`: Empty dictionary, reserved for extensibility.

---

## 🧪 Methods

### 1. `get_conn()`

```python
def get_conn(self) -> pyodbc.Connection
```

Establishes a secure connection to Azure SQL Database using Azure AD authentication.

#### How it works:
- Fetches `process_monitoring_conn_str` from `job_args`.
- Acquires a token using `DefaultAzureCredential`.
- Builds the token using `struct.pack` in the required format.
- Connects using `pyodbc` with access token.

#### Returns:
- `pyodbc.Connection` object for the database.

---

### 2. `execute_query_and_get_results(passed_query: str, fetch_results: bool = True) -> DataFrame`

Executes a SQL query on the connected Azure SQL DB.

#### Parameters:
- `passed_query`: SQL string.
- `fetch_results`: If `True`, returns Spark DataFrame of results.

#### Returns:
- Spark DataFrame (empty if `fetch_results=False` or no records).

#### Notes:
- Validates input using `common_utils.validate_function_param`.
- Skips execution in dry-run mode (`job_args["dry_run"]` = True).

---

### 3. `get_and_set_job_id(raise_exception=False)`

Fetches `job_id` from the `job_details` table and stores it in `job_args`.

#### Behavior:
- Constructs a SQL `SELECT` query using:
  - `source_system`, `source`, `source_type`, `load_type`
- If one matching record is found:
  - Sets `job_args["job_id"]`
- Else:
  - Logs an error and optionally raises an exception.

---

### 4. `insert_job_details()`

Inserts a new job record into `job_details` table using current configuration from `job_args`.

#### Inserted Fields:
- `_created`, `_modified`
- `env`, `job_name`, `frequency`, `load_type`
- `source_system`, `source`, `source_type`

#### Example SQL:
```sql
INSERT INTO job_details (...) VALUES (...)
```

---

### 5. `check_and_get_job_id()`

Checks whether a job ID is present. If not:
1. Inserts job details using `insert_job_details`.
2. Re-fetches job ID using `get_and_set_job_id(raise_exception=True)`.

#### Used in:
Startup routines of ETL jobs to ensure the job's identity is registered.

---

### 6. `check_already_processed(passed_job_id: str = "", passed_run_date: str = "")`

Checks if a job run (based on job ID and run date) has already been completed.

#### Behavior:
- Constructs SQL query to check `run_status='COMPLETED'`
- If one record is found, sets `job_args["<job_id>_completed"] = True`

#### Example Use Case:
Used before ingestion begins to avoid duplicate processing.

---

### 7. `get_and_set_run_id()`

Fetches `run_id` based on:
- `job_id`, `run_start_time`, `run_date`

#### Behavior:
- Uses SQL `SELECT` to retrieve the run ID.
- Stores it into `job_args["run_id"]`
- Raises exception if result count != 1.

---

### 8. `insert_update_job_run_status(passed_status: str, passed_comments: str = "")`

Inserts or updates a job's run status in `job_run_details` table.

#### Behavior:
- Validates inputs.
- Checks if `run_id` exists:
  - If not, performs `INSERT`.
  - If yes, performs `UPDATE`.
- Stores run start/end time, row count, status, and comments.

#### Fields Managed:
- `_created`, `_modified`
- `run_start_time`, `run_end_time`
- `run_status`, `run_error_detail`

---

## 🧩 Required `job_args` Keys

The following keys must be set in the `job_args` object:

| Key | Purpose |
|-----|---------|
| `process_monitoring_conn_str` | DB connection string |
| `job_details_table_name` | Table for job metadata |
| `job_run_details_table_name` | Table for run metadata |
| `env`, `job_name`, `load_type`, `frequency`, `source_system`, `source`, `source_type` | Job metadata |
| `run_date`, `run_start_time`, `run_row_count` | Runtime metadata |
| `dry_run` | Whether to execute DB operations |

---

## 🛠 Example Integration

```python
pm = ProcessMonitoring(lc, common_utils, job_args)

pm.check_and_get_job_id()
pm.check_already_processed()

if not job_args.get("123_completed"):
    # process logic
    job_args.set("run_row_count", processed_count)
    pm.insert_update_job_run_status("COMPLETED")
else:
    lc.logger.info("Job already completed.")
```

---

## 🔒 Security Consideration

- Uses **Azure Active Directory Token Authentication** (via `DefaultAzureCredential`) instead of embedding secrets.
- Avoids writing hardcoded credentials in connection strings.

---

## 🧪 Testing Suggestions

Unit testing this class requires:
- Monkeypatching `pyodbc.connect`, `DefaultAzureCredential`, and `SparkSession`
- Simulating `job_args` with a mock object
- Injecting mock return values for `pandas.read_sql`, `SparkSession.createDataFrame`
