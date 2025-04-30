# EDAP Ingestion Framework

## Overview

This modular ingestion framework supports dynamic loading and execution of different ingestion types (e.g., CSV, Parquet) using a factory design pattern. It is designed for use within Databricks and includes support for validation, logging, monitoring, and configuration management.

---

## 📁 Directory Structure

```bash
edap_ingest/
├── ingest/
│   ├── base_ingest.py          # Abstract ingestion logic
│   ├── csv_ingest.py           # CSV-specific ingestion logic
├── utils/
│   └── ingestion_defaults.py   # Default values and configurations
```

---

## 🔧 Core Components

### `IngestFactory`

Responsible for dynamically loading the appropriate ingestion class based on the `ingest_type` provided in the input arguments.

```python
class_file_name = f"{ingest_type}_ingest"
class_name = f"{ingest_type.capitalize()}Ingest"
```

### `BaseIngest`

Provides core functionality for ingestion workflows:

- **Input Param Handling**
- **Config Reading (common & table)**
- **Schema Construction**
- **Source/Target Path Generation**
- **Pre- & Post-Load Hooks**
- **Column Aggregation**

### `CsvIngest`

Extends `BaseIngest` and implements CSV-specific logic. Adds:

- Schema inference if not provided
- Reading CSV into Spark DataFrame
- Optional dry run logic
- Writing to a Delta table (Databricks)

---

## 🚀 Execution Flow

### Diagram: Execution Pipeline

```mermaid
flowchart TD
    A[IngestFactory.start_load] --> B[Import CSVIngest dynamically]
    B --> C[Instantiate CSVIngest]
    C --> D[CSVIngest.run_load()]
    D --> E[pre_load()]
    E --> F[load()]
    F --> G[post_load()]
```

### Diagram: BaseIngest Flow

```mermaid
flowchart TD
    A[run_load()] --> B[pre_load()]
    B --> C[read_and_set_input_args()]
    B --> D[read_and_set_common_config()]
    B --> E[read_and_set_table_config()]
    C --> F[check_already_processed]
    F -->|If Processed| G[Exit Gracefully]
    F -->|Else| H[Continue to load()]
    H --> I[form_source_and_target_locations()]
    I --> J[form_schema_from_dict()]
    J --> K[collate_columns_to_add()]
    K --> L[Execute Validations and Load to Delta Table]
    L --> M[post_load()]
```

---

## ⚙️ Parameters & Configs

### Input Parameters (via YAML / notebook widget)

- `ingest_type`: Type of file to ingest (e.g., `csv`)
- `run_date`: Date for partitioning and sourcing files
- `source_base_location`
- `target_catalog`, `target_schema`, `target_table`

### Common Config

- Contains environment-level configurations like source references.

### Table Config

- Contains schema definitions, column mapping, and metadata.

---

## ✅ Validation

Validation logic is executed before writing data, allowing checks for schema conformity or business rules.

```python
self.validation_obj.run_validations(source_data)
```

---

## 💡 Features

- Modular design with class extension capability
- Plug-and-play ingestion types
- Parameter-driven configuration
- Spark and Databricks native
- Structured logging and monitoring support

---

## 📦 Adding New Ingestion Types

1. Create a new file: `json_ingest.py` (example).
2. Inherit from `BaseIngest`.
3. Implement/override `load()` method.
4. Use the factory by specifying `ingest_type=json`.

```python
class JsonIngest(BaseIngest):
    def load(self):
        # Implement JSON-specific logic
        pass
```

---

## 🧪 Dry Run Mode

If `dry_run` is enabled, the pipeline reads and validates data but skips writing to the target table.

---

## 🔒 Error Handling

Errors are logged and job status is updated with:

```python
self.process_monitoring_obj.insert_update_job_run_status("Failed", passed_comments=str(e))
```

---

## 📈 Logging

All major steps include logging for traceability using the provided `common_utils` object:

```python
self.common_utils_obj.log_msg("Inside load()")
```

---

## 🧩 Dependencies

- PySpark
- Databricks Runtime
- YAML

---

## 📝 License

Proprietary. For internal use only.

---

## 📞 Support

Please reach out to the data platform team for any integration or ingestion-related queries.
