# Ingestion Framework — High Level Design

---

## 1. Overview

The ingestion framework is designed to provide a scalable, configurable, and reusable solution for ingesting data into Spark-based environments, primarily on Databricks. It supports multiple data sources and targets with standardized processes for validation, auditing, and error handling.

---

## 2. Goals

- **Configurable:** Minimal code changes needed for new tables or data sources; driven by YAML configurations and runtime parameters.  
- **Extensible:** Easy to extend for new file formats or validation rules via subclassing and modular utilities.  
- **Robust:** Handles errors gracefully with logging, quarantine, and job lifecycle monitoring.  
- **Auditable:** Adds audit columns automatically and tracks job status for traceability.  
- **Reusable:** Common logic centralized in a base class with clear extension points.

---

## 3. Key Components

### 3.1 BaseIngest (Abstract Class)

- Core ingestion logic and flow.  
- Reads configs and input arguments.  
- Defines abstract method for source data reading.  
- Implements schema formation, audit column addition, validation orchestration, and writing to target/quarantine.  

### 3.2 Subclassed Ingestors

- Implement source-specific reading (e.g., CSV, JSON, Parquet).  
- Extend or override base behavior if needed.

### 3.3 Configuration Files

- **Common Config:** Global parameters shared across ingestion jobs (e.g., base file locations, default formats).  
- **Table Config:** Table-specific parameters such as schema, audit columns, target tables, and file naming conventions.

### 3.4 Utilities

- **CommonUtils:** Spark DataFrame operations and transformations.  
- **ValidationUtils:** Business rule validations and quality checks.  
- **ProcessMonitoring:** Tracks job lifecycle states (Running, Completed, Failed, Exited).  

---

## 4. Data Flow

```mermaid
flowchart TD
    Start[Start Job with Input Args]
    LoadConfigs[Load Common & Table Configurations]
    CheckJobStatus{Is Job Already Processed?}
    Exit[Exit Gracefully if Processed]
    BuildSchema[Build Schema from Config]
    FormPaths[Construct Source and Target Paths]
    ReadSource[Read Data from Source (Subclass)]
    AddColumns[Add Audit and Derived Columns]
    Validate[Validate Data]
    WriteTarget[Write Valid Data to Target]
    WriteQuarantine[Write Invalid Data to Quarantine]
    MarkComplete[Mark Job Completed]
    End[End Job]

    Start --> LoadConfigs --> CheckJobStatus
    CheckJobStatus -->|Yes| Exit
    CheckJobStatus -->|No| BuildSchema
    BuildSchema --> FormPaths --> ReadSource --> AddColumns --> Validate
    Validate --> WriteTarget
    Validate --> WriteQuarantine
    WriteTarget --> MarkComplete --> End
    WriteQuarantine --> MarkComplete


5. Ingestion Lifecycle

| Phase               | Description                                         |
|---------------------|-----------------------------------------------------|
| Initialization      | Setup logger, Spark session, input/job args.        |
| Pre-Load            | Load configs, set defaults, check job status.       |
| Load                | Read source data, add columns, validate, write data.|
| Post-Load           | Update job status, cleanup resources.               |
| Exception Handling  | Capture errors, update status, optionally raise.    |

---

6. Extension Points

- Implement `read_data_from_source()` in subclasses for new data formats.  
- Add validation rules in `ValidationUtils` or by subclassing.  
- Extend audit columns and derived columns logic as needed.  
- Customize configuration schema and parsing to support additional parameters.

---

7. Design Principles

- **Separation of Concerns:** Configuration, data reading, transformation, validation, and writing are modular.  
- **Fail Fast:** Early validation and job status checks avoid wasted compute.  
- **Idempotency:** Jobs can be safely rerun without duplication due to processed status checks.  
- **Configurability over Hardcoding:** YAML-driven configurations allow flexibility.  
- **Observability:** Logging and process monitoring provide transparency and traceability.

---

8. Technology Stack

- **Apache Spark (PySpark):** Distributed data processing engine.  
- **Databricks:** Execution platform with notebooks and `dbutils`.  
- **YAML:** Configuration file format for readability and flexibility.  
- **Python:** Framework implementation language.  
- **Logging Framework:** For runtime information and error tracking.

---

9. Assumptions and Constraints

- Input data is accessible from specified file paths or databases.  
- Schema definitions in config match the source data structure.  
- Job run dates are passed consistently for partitioning and tracking.  
- Target tables exist and have appropriate permissions for write operations.

---

10. Summary

This framework standardizes ingestion pipelines across datasets by abstracting common functionality and enabling customization through subclassing and configuration. It ensures data quality, traceability, and operational reliability in a big data environment.
