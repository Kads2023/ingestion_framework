Design Document: Dynamic Ingestion Framework
1. Overview
The ingestion framework is designed to dynamically load and execute specific ingestion logic (e.g., CSV ingestion) based on configuration parameters. It uses Python class inheritance and dynamic module loading to support extensible data ingestion from various sources.

2. Key Components
2.1. IngestFactory
Purpose: Acts as the entry point for ingestion jobs. Dynamically imports and invokes the appropriate ingestion class (CsvIngest, etc.) based on input.

Key Responsibilities:

Parses ingest_type from input arguments.

Dynamically loads module and class using importlib.

Instantiates and executes the .run_load() method of the ingestion class.

python
Copy
Edit
ingest_type = str(passed_input_args.get("ingest_type")).strip().lower()
class_file_name = f"{ingest_type}_ingest"
class_name = f"{ingest_type.capitalize()}Ingest"
2.2. BaseIngest
Purpose: Serves as a base class encapsulating common logic shared by all ingestion types.

Key Responsibilities:

Parameter setup (input, job, schema, etc.).

Configuration file loading (common_config, table_config).

Job monitoring integration (Started, Completed, Failed).

Source-target path generation, schema construction.

Error handling and exit management.

Main Methods:
read_and_set_input_args()

read_and_set_common_config()

read_and_set_table_config()

form_schema_from_dict()

form_source_and_target_locations()

collate_columns_to_add()

pre_load(), load(), post_load(), run_load()

2.3. CsvIngest (inherits from BaseIngest)
Purpose: Implements logic specific to ingesting CSV files.

Key Responsibilities:

Reads source CSV data using Spark.

Applies schema (defined or inferred).

Performs validations.

Writes data to Delta Lake (if not a dry run).

Overrides & Enhancements:
Extends logging across overridden methods.

Uses Spark's CSV read and conditional write to Delta Lake:

python
Copy
Edit
source_data = self.spark.read.load(source_location, format="csv", ...)
source_data.write.mode("append").saveAsTable(target_location)
3. Data Flow
Trigger Ingestion: IngestFactory.start_load() is called with input and job configs.

Class Loading: Ingest type is parsed and corresponding class (CsvIngest) is dynamically loaded.

Job Execution:

run_load() → pre_load() → load() → post_load()

Each phase handles distinct responsibilities:

Pre-load: Sets up environment and configs.

Load: Reads data, applies schema, transforms, validates, writes.

Post-load: Marks job as complete.

4. Extensibility
To support new ingestion types:

Create a new module under edap_ingest.ingest (e.g., json_ingest.py).

Define a class JsonIngest(BaseIngest) with overridden methods as needed.

IngestFactory will pick it automatically if ingest_type=json is passed.

5. Error Handling & Monitoring
Integrated with process_monitoring_obj:

Tracks job states: Started, Completed, Failed, Exited.

Uses structured logging via common_utils_obj.log_msg(...).

Errors captured and cleaned for SQL safety before logging and raising.

6. Dependencies
SparkSession: Active session required for all data reads/writes.

Common Utils: Logging, DBUtils setup, file reading, type evaluation.

Validation Utils: Business or schema validation.

YAML Files: Configuration loaded from common/table config locations.

7. Sample Ingest Types Supported
Ingest Type	Class Name	Module File
csv	CsvIngest	csv_ingest.py
parquet	ParquetIngest	parquet_ingest.py
json	JsonIngest	json_ingest.py

8. Security & Safety
Uses runtime module loading securely with explicit getattr().

Avoids SQL injection-like risks by sanitizing error messages.

Supports dry runs to avoid unintentional writes.

9. Logging Example
text
Copy
Edit
[CsvIngest.load()] - dry_run --> False,
source_location --> /mnt/data/2025/05/06/source.csv,
target_location --> catalog.schema.table,
schema_struct --> StructType([...])
10. Future Enhancements
Add support for partitioned writes and optimized reads.

Support for streaming ingestion (e.g., Kafka, Autoloader).

Schema evolution and validation frameworks integration.

Metrics collection for ingestion performance.

