Documentation for IngestFactory, BaseIngest, and CsvIngest
Overview
This ingestion framework provides a flexible and extensible approach for ingesting data into a Spark environment, primarily built on top of Databricks SDK and PySpark. It supports a pluggable factory design pattern to select and run ingestion pipelines for different data formats and configurations.

Components
1. IngestFactory
Purpose:
The IngestFactory is a factory design pattern implementation that centralizes the creation of ingestion pipeline instances. Based on the data type or configuration, it returns the appropriate subclass instance for handling the ingestion.

Usage:

Provide the ingestion type or other identifier (e.g., "csv").

The factory instantiates and returns the corresponding ingestion class (e.g., CsvIngest).

This allows easy extension for additional formats without changing client code.

Example:

python
Copy
Edit
factory = IngestFactory()
ingest_pipeline = factory.get_ingest_pipeline("csv", input_args, job_args, common_utils, process_monitoring, validation_utils, dbutils)
ingest_pipeline.run_load()
2. BaseIngest
Purpose:
BaseIngest is an abstract/general base class that defines common ingestion operations, including:

Reading and setting input arguments

Loading common and table-specific configuration files

Preparing schema and source/target locations

Monitoring and updating job status

Managing lifecycle methods: pre_load(), load(), post_load(), and run_load()

Key Responsibilities:

Centralize ingestion control flow logic.

Handle common configuration parsing and validation.

Manage Spark session access.

Provide hooks for child classes to extend or override specific behavior.

Main Methods:


Method	Description
__init__	Initialize with utility and job argument objects
read_and_set_input_args	Parse mandatory and default input parameters
read_and_set_common_config	Load common config YAML file and apply to job args
read_and_set_table_config	Load table-specific config YAML file and apply to job args
exit_without_errors	Exit the job gracefully with a success message
pre_load	Perform checks like job status and parameter setup
form_schema_from_dict	Construct Spark schema from dictionary configs
form_source_and_target_locations	Build source file paths and target table names
collate_columns_to_add	Aggregate audit and table columns that must be added
load	Abstract method for actual ingestion logic (to be extended)
post_load	Update job status to completed
run_load	Orchestrates the ingestion lifecycle (calls pre_load, load, post_load)
3. CsvIngest
Purpose:
CsvIngest is a concrete subclass of BaseIngest tailored specifically for CSV file ingestion.

Specializations over BaseIngest:

Reads CSV files from the configured source location.

Supports both schema-defined and schema-inferred CSV reading.

Uses Spark CSV reader with options like header detection.

Runs validation on the loaded DataFrame.

Writes data into a configured Spark table unless running in dry-run mode.

Logs detailed progress and key parameter values.

Additional Methods/Overrides:


Method	Description
load	Loads CSV data with/without schema, validates, and writes table.
Example Usage:

python
Copy
Edit
csv_ingest = CsvIngest(input_args, job_args, common_utils, process_monitoring, validation_utils)
csv_ingest.run_load()
Design Highlights
Extensibility: Easily add new ingest types by subclassing BaseIngest and registering in the factory.

Modularity: Common ingestion logic is encapsulated in the base class; format-specific logic is in subclasses.

Monitoring Integration: Job status updates and validation hooks are integrated to ensure robust processing.

Configuration Driven: Reads YAML configuration files for dynamic and flexible ingestion parameters.

README
Data Ingestion Framework
Overview
This framework provides a scalable, modular, and extensible data ingestion solution designed for cloud-native Spark environments such as Databricks. It supports different file formats and ingestion requirements via a pluggable factory and inheritance design.

Features
Factory pattern to instantiate ingestion pipelines dynamically.

Base class BaseIngest providing common ingestion lifecycle management.

CsvIngest subclass for CSV-specific ingestion handling.

Configuration-driven setup with YAML files for common and table-specific parameters.

Integration with job monitoring and validation utilities.

Supports schema inference and explicit schema definition.

Dry-run mode to validate ingestion steps without data writes.

Installation
Install dependencies via pip (example):

bash
Copy
Edit
pip install pyspark databricks-sdk pyyaml
Place the ingestion modules and utilities in your project directory or package as needed.

Usage
Step 1: Setup input and job arguments
Define your ingestion input parameters and job arguments (via config files or dicts).

Step 2: Create ingestion pipeline via factory
python
Copy
Edit
from edap_ingest.ingest.factory import IngestFactory

factory = IngestFactory()
ingest_pipeline = factory.get_ingest_pipeline(
    "csv",
    input_args,
    job_args,
    common_utils,
    process_monitoring,
    validation_utils,
    dbutils
)
Step 3: Run ingestion
python
Copy
Edit
ingest_pipeline.run_load()
Extending the Framework
To add support for a new file format (e.g., JSON, Parquet):

Create a new subclass of BaseIngest and implement/override required methods.

Register the new class in IngestFactory.

Provide corresponding configuration files and validation logic.

Project Structure
markdown
Copy
Edit
edap_ingest/
├── ingest/
│   ├── base_ingest.py
│   ├── csv_ingest.py
│   ├── factory.py
│   └── ...
├── utils/
│   ├── ingestion_defaults.py
│   └── ...
└── tests/
    ├── test_csv_ingest.py
    ├── test_base_ingest.py
    └── ...
