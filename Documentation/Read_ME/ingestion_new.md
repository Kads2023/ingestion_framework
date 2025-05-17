# Ingestion Framework — Detailed Documentation

---

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Concepts](#core-concepts)
4. [Class: BaseIngest](#class-baseingest)
   - Initialization
   - Method Descriptions
5. [Configuration Details](#configuration-details)
   - Input Arguments
   - Common Config File
   - Table Config File
6. [Job Lifecycle](#job-lifecycle)
7. [Extending the Framework](#extending-the-framework)
8. [Error Handling & Logging](#error-handling-logging)
9. [Example Usage](#example-usage)
10. [Frequently Asked Questions](#faq)

---

## Introduction

This ingestion framework is designed to streamline and standardize data ingestion into Spark-based environments, especially in Databricks. It facilitates:

- Reading configurable input parameters.
- Loading common and table-specific YAML configurations.
- Schema definition based on configuration.
- Reading data from various sources (via subclassing).
- Adding audit and derived columns.
- Data validation and quarantining invalid records.
- Writing data to target and quarantine tables.
- Monitoring job status with detailed lifecycle events.

The framework’s main entry point is the abstract `BaseIngest` class, which requires subclassing to implement the source-specific data reading logic.

---

## Architecture Overview

- **BaseIngest:** Abstract base class defining ingestion flow and utility functions.
- **Common Utilities:** Helpers for data operations, type conversions, and evaluation.
- **Validation Utilities:** Components that apply business rules and quality checks.
- **Process Monitoring:** Tracks job progress and status updates.
- **Configuration Files:** YAML files specifying input, common, and table-specific parameters.
- **Job Arguments:** Runtime parameters passed into and modified by the ingestion job.

---

## Core Concepts

- **Input Args:** User-provided parameters that define the ingestion run.
- **Job Args:** Combined parameters (input + config + runtime) driving ingestion logic.
- **Schema Struct:** Spark `StructType` schema dynamically built from config.
- **Derived Columns:** Extra columns added automatically for auditing or business logic.
- **Dry Run:** Mode where data is processed but not written to targets.
- **Quarantine Table:** Table to hold invalid records flagged during validation.

---

## Class: BaseIngest

### Initialization: `__init__(...)`

- Initializes core objects: logger, input/job args, utilities, monitoring, Spark session.
- Sets up `dbutils` for Databricks notebook interaction.
- Sets a flag `raise_exception` to control error propagation.

### Methods Overview

---

### `read_and_set_input_args()`

- Reads all keys from the input arguments.
- Converts specific keys from string to boolean if required.
- Sets these values into the `job_args_obj` for downstream use.

---

### `read_and_set_common_config()`

- Loads common configuration from YAML file specified in input args.
- Populates job arguments with values from the common config.

---

### `read_and_set_table_config()`

- Loads table-specific configuration from YAML file specified in input args.
- Updates job arguments accordingly.

---

### `exit_without_errors(passed_message)`

- Updates the process monitoring status to "Exited".
- Prevents exceptions from being raised.
- Exits the Databricks notebook with a message (graceful job termination).

---

### `pre_load()`

- Sets mandatory and default input parameters.
- Loads common and table configurations.
- Reads and applies input arguments.
- Initializes job monitoring and checks for job existence.
- Checks if the job is already processed for the given run date; if yes, exits gracefully.

---

### `form_schema_from_dict()`

- Reads schema dictionary from job args.
- Builds a Spark `StructType` schema based on column definitions.
- Supports skipping columns marked as derived.

---

### `form_source_and_target_locations()`

- Constructs the source file path using base location, date patterns, prefixes, and extensions.
- Constructs fully qualified target and quarantine table locations.
- Uses the run date to interpolate folder and file naming patterns dynamically.

---

### `collate_columns_to_add()`

- Combines audit columns and table-specific columns to be added during ingestion.
- Sets the combined list in the job arguments.

---

### `check_multi_line_file_option()`

- Reads the `multi_line` option from configuration.
- Converts it to a normalized boolean string ("true"/"false").
- Used to handle multi-line JSON or CSV files during reading.

---

### `read_data_from_source() -> Spark_Dataframe`

- **Abstract method:** Must be implemented by subclasses.
- Expected to read source data and return a Spark DataFrame.

---

### `add_derived_columns(data_to_add_columns) -> Spark_Dataframe`

- Adds columns as specified in the combined `columns_to_be_added` list.
- Supports:
  - Adding a hash column (hash of specified columns).
  - Adding current timestamp column.
  - Adding literal value columns.
- Throws a `ValueError` for unknown function names.

---

### `write_data_to_target_table(data_to_write)`

- Writes the final DataFrame to the configured target table.
- Honors dry run mode to skip actual writes.
- Sets the processed row count into job args.

---

### `write_data_to_quarantine_table(validation_issues_data)`

- Writes invalid records to the quarantine table for further analysis.
- Skips write if in dry run mode.

---

### `load()`

- Orchestrates the main loading process:
  - Builds source and target paths.
  - Forms the schema.
  - Prepares columns to add.
  - Reads source data.
  - Adds derived columns.
  - Runs validations.
  - Writes data to target and quarantine tables depending on validation results.

---

### `post_load()`

- Marks the ingestion job as "Completed" in process monitoring.

---

### `run_load()`

- Wraps the full ingestion lifecycle (`pre_load` → `load` → `post_load`).
- Catches exceptions and updates job status to "Failed".
- Raises exceptions unless suppressed.

---

## Configuration Details

### Input Arguments

- Runtime parameters controlling the ingestion execution.
- Examples: `run_date`, `common_config_file_location`, `table_config_file_location`.

### Common Config File

- YAML file containing parameters shared across jobs (e.g., default formats, locations).

### Table Config File

- YAML file defining table-specific settings, such as:
  - Target catalog/schema/table.
  - Schema details with data types.
  - File naming conventions.
  - Columns to add (audit and table-specific).

---

## Job Lifecycle

| Step          | Description                               |
|---------------|-------------------------------------------|
| Initialization| Setup utilities, logging, spark session. |
| Pre-load      | Load input args, configs, check already processed. |
| Load          | Read source, add columns, validate, write data. |
| Post-load     | Mark job as completed.                     |
| Run Load      | Execute all above with error handling.    |

---

## Extending the Framework

- Implement `read_data_from_source()` for new file types or data sources.
- Extend validation utilities to add business-specific rules.
- Customize column derivation by overriding `add_derived_columns`.
- Add new config parameters and update loading logic accordingly.

---

## Error Handling & Logging

- Extensive logging at INFO and DEBUG levels.
- Logs all key steps with contextual messages.
- Uses a `raise_exception` flag to decide whether to propagate errors.
- Writes job status updates for success, failure, and exit events.
- Quarantines invalid records for troubleshooting.

---

## Example Usage

```python
from ingestion.csv_ingest import CsvIngest

ingest_job = CsvIngest(
    lc=logger_container,
    input_args=input_args_obj,
    job_args=job_args_obj,
    common_utils=common_utils_obj,
    process_monitoring=process_monitoring_obj,
    validation_utils=validation_utils_obj,
    dbutils=dbutils
)

ingest_job.run_load()


Frequently Asked Questions
Q: How do I add new audit columns?
A: Update the audit_columns_to_be_added list in the table config YAML.

Q: Can I run a dry run to test without data writes?
A: Yes, set the dry_run input argument to true.

Q: How is schema mismatch handled?
A: The schema is formed from config; if source data mismatches, Spark will throw errors during reading.

Q: How do I stop a job if already processed?
A: The framework checks if a job is marked completed for the run_date and exits gracefully.