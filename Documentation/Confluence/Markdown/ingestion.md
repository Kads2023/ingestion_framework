Here’s the Confluence Markdown for the edap_ingest ingestion framework design documentation:

markdown
Copy
Edit
# EDAP Ingestion Framework - Design Documentation

## 🧭 Purpose

This document outlines the design of the `edap_ingest` framework used for modular, scalable ingestion of structured data (e.g., CSV) into the enterprise data platform. It supports dynamic class-based ingestion and integrates with Databricks.

---

## 🏗️ High-Level Architecture

| Layer           | Component               | Description |
|----------------|-------------------------|-------------|
| **Interface**  | IngestFactory            | Entry point for ingestion jobs. Dynamically loads classes based on input parameters. |
| **Core Logic** | BaseIngest               | Defines reusable ingestion flow and utilities (config, paths, schema, pre/post hooks). |
| **Extension**  | CsvIngest                | Implements CSV-specific loading logic by extending BaseIngest. |
| **Utilities**  | ingestion_defaults.py    | Provides defaults and helper functions. |

---

## 📌 Key Design Principles

- **Factory Pattern**: Used for dynamic instantiation of ingestion classes.
- **Object-Oriented Design**: Promotes reuse and extension via inheritance and polymorphism.
- **Separation of Concerns**: Each module has a well-defined role (e.g., config management, schema handling).
- **Plug-and-Play**: Easily extendible for additional formats (e.g., JSON, Parquet).

---

## ⚙️ Ingestion Flow

```mermaid
flowchart TD
    A[User Trigger: IngestFactory.start_load()] --> B[Dynamic Import: ingest_type_ingest.py]
    B --> C[Class Instantiation: CsvIngest()]
    C --> D[CsvIngest.run_load()]
    D --> E[pre_load()]
    E --> F[load()]
    F --> G[post_load()]
Each step delegates to standardized methods in BaseIngest, which can be overridden or extended.

🧬 BaseIngest Responsibilities
Reads input parameters

Fetches common and table-level configurations

Validates source files and schema

Prepares source and target paths

Calls validation modules

Logs status to monitoring systems

🧪 CsvIngest: CSV-Specific Logic
python
Copy
Edit
class CsvIngest(BaseIngest):
    def load(self):
        # Reads CSV using Spark
        # Applies schema or infers it
        # Writes data to Delta
Adds support for header, delimiter, and quote options.

Supports dry-run mode for test validation without persistence.

🗂️ Configuration Structure
Input Parameters (from YAML/notebook widget):

ingest_type, run_date, source_base_location, etc.

Common Config: Environment-level variables.

Table Config: Schema definition, column mapping, etc.

🔍 Validation
Executed before the write operation. Supports schema, null check, type, and custom business validations.

📤 Extending to Other Formats
Steps to add new ingestion logic:

Create a new file: <type>_ingest.py.

Inherit from BaseIngest.

Implement load() with type-specific logic.

Trigger using ingest_type=<type>.

💡 Features
Dynamic ingestion type resolution

Configurable via YAML

Spark/Delta-native

Dry-run mode

Rich logging and error handling

Easy integration into Databricks jobs

🛡️ Error Handling & Monitoring
Errors logged via common_utils.

Job statuses updated in monitoring tables.

Graceful exits if already processed.

🔁 Reusability & Maintainability
Minimal code duplication

Common logic abstracted in base class

Easy to test individual components

Changes isolated to specific ingestion type files

📎 References
Python importlib

Databricks Runtime Documentation

Spark DataFrame Reader Options

vbnet
Copy
Edit

This markdown is ready for direct upload to Confluence. Let me know if you need any adjustments!