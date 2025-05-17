# Detailed Low-Level Design (LLD) for Ingestion Framework

## 1. Overview

This design document provides a detailed explanation of the core components, their responsibilities, interactions, and internal workflows within the ingestion framework based on the `BaseIngest` class.

---

## 2. Core Components

### 2.1 BaseIngest Class

- **Responsibility:**
  Abstract base class that provides common ingestion functionalities such as configuration loading, schema formation, validation, derived columns addition, and writing data to target tables.

- **Key Attributes:**
  - `lc`: Logger instance for logging.
  - `input_args_obj`: Manages input parameters.
  - `job_args_obj`: Manages runtime/job-specific parameters.
  - `common_utils_obj`: Utility methods for common operations (e.g., YAML parsing, type conversions).
  - `process_monitoring_obj`: Monitors job status, handles status updates.
  - `validation_obj`: Runs validation logic on dataframes.
  - `dbutils`: Databricks utility object for notebook and file system operations.
  - `spark`: Active SparkSession instance.

- **Key Methods:**
  - `__init__`: Initializes components, logger, and Spark session.
  - `read_and_set_input_args`: Parses input args, converts types (e.g., strings to bools).
  - `read_and_set_common_config`: Reads shared/common YAML config files and populates job args.
  - `read_and_set_table_config`: Reads table-specific YAML configs and sets job args.
  - `pre_load`: Performs all pre-processing steps like validating inputs, reading configs, checking if job was already processed.
  - `form_schema_from_dict`: Creates Spark schema StructType from configuration dictionary.
  - `form_source_and_target_locations`: Constructs source file paths and target table locations dynamically based on input args and runtime parameters.
  - `collate_columns_to_add`: Combines audit and table-specific derived columns to be added to data.
  - `check_multi_line_file_option`: Determines if the input file supports multi-line records.
  - `read_data_from_source`: Abstract method; implemented in subclasses to read data from specific sources.
  - `add_derived_columns`: Adds literal, timestamp, or hash columns to the dataframe as configured.
  - `write_data_to_target_table`: Writes the validated dataframe to the target database table.
  - `write_data_to_quarantine_table`: Writes validation-failed records to quarantine table for auditing.
  - `load`: Main ingestion logic orchestrator — reads source data, adds columns, validates, writes output.
  - `post_load`: Marks the job as completed in monitoring system.
  - `run_load`: Top-level method that wraps the ingestion lifecycle with error handling.

---

## 3. Data Flow and Workflow

### 3.1 Initialization

- Logger and Spark session are initialized.
- Input arguments and job arguments objects are created.
- Utilities and monitoring objects are injected.
- Databricks utilities (`dbutils`) are set or created.

### 3.2 Pre-Load Phase

- Mandatory input parameters are enforced.
- Default values are assigned for missing inputs.
- Configuration YAMLs (common and table-specific) are parsed and their keys added to job arguments.
- Input arguments are converted and set in job arguments.
- Job monitoring status is updated to `Started`.
- Checks if the job was already processed for the given run date and exits early if true.

### 3.3 Load Phase

- Source and target locations are formed dynamically using runtime parameters like `run_date`.
- Schema is constructed from a dictionary, mapping column names to Spark data types.
- Audit and table-specific derived columns are merged for addition.
- Multi-line file option is checked and set.
- **Read source data:** This is delegated to subclasses implementing `read_data_from_source()`.
- Add derived columns such as hash, current timestamp, or literal values.
- Validate the enriched dataframe via `validation_obj`.
- If validation succeeds and no error flagged, write data to target table.
- If validation fails, write invalid rows to quarantine table.
- Track row counts for metrics.

### 3.4 Post-Load Phase

- Mark job as completed successfully in monitoring.
- Cleanup and resource release (e.g., unpersist Spark caches if any).

### 3.5 Exception Handling

- All stages wrapped in try-except block inside `run_load`.
- On exceptions, job status is marked as `Failed`.
- Errors are logged, and optionally re-raised depending on configuration.

---

## 4. Configuration and Schema Management

- **Input Arguments:** Command line or notebook parameters specifying runtime values.
- **Common Config YAML:** Contains shared parameters like database names, audit column specs, etc.
- **Table Config YAML:** Table-specific settings like source file details, schema, column data types.
- **Schema Definition:** Provided as nested dictionaries, parsed into Spark `StructType` with `StructField`s, dynamically supporting new data types.
- **Derived Columns:** Configurable list defining audit columns, literal values, hash columns, timestamp columns to be appended to ingested data.

---

## 5. Extensibility and Customization

- **Subclassing:** Extend `BaseIngest` and implement `read_data_from_source()` to support new source types (CSV, Parquet, JDBC, etc.).
- **Validation Logic:** Add or override validation rules in `validation_obj`.
- **Derived Columns:** Customize audit or computed columns by extending `collate_columns_to_add()` or `add_derived_columns()`.
- **Configuration:** YAML files can be extended to add more parameters without code changes.

---

## 6. Exception and Error Handling

- Validation failures write data to quarantine table for later inspection.
- All exceptions logged with stack traces.
- Job status updated appropriately to reflect success, failure, or early exit.
- Option to disable raising exceptions for graceful shutdown.

---

## 7. Key Data Structures

| Name               | Type          | Description                                |
|--------------------|---------------|--------------------------------------------|
| `input_args_obj`   | Object        | Holds and parses input runtime parameters.|
| `job_args_obj`     | Object        | Stores job-specific parameters dynamically.|
| `schema_dict`      | Dict          | Maps column names to their data types and metadata.|
| `schema_struct`    | `StructType`  | Spark schema derived from `schema_dict`.   |
| `Spark_Dataframe`  | Spark DataFrame| The main data structure holding ingested data.|

---

## 8. Performance Considerations

- Row count triggers are used minimally to avoid expensive actions.
- Data caching/unpersisting can be added around heavy transformations.
- Fail-fast design prevents unnecessary resource consumption.
- Partitioning and predicate pushdown rely on source configuration.

---

## 9. Logging and Monitoring

- Structured logging with method context tags.
- Informational logs for process milestones.
- Debug logs for dataframe schemas and sample data.
- Errors are captured with detailed messages.
- Process monitoring integrated with job lifecycle updates (Started, Completed, Failed).

---

## 10. Summary

The detailed design outlines a modular, configurable ingestion framework built for flexibility, scalability, and robustness. It facilitates easy integration of new data sources, enforces data quality via validation, and ensures operational visibility with process monitoring. By leveraging Spark and Databricks utilities, it targets big data ingestion needs with repeatability and maintainability.
