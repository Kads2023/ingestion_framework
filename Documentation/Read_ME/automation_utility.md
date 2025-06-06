Data Type Mapping Pattern

What is it?

Data type mapping handles translating source database-specific data types (e.g., Oracle’s VARCHAR2, NUMBER) into target system data types (e.g., Databricks types like STRING, DECIMAL).

This ensures the generated DDL is compatible with the target platform.

How does it work in your project?

You have separate mapper classes for each source system:

OracleDataTypeMapper

SqlServerDataTypeMapper

These inherit from a base class BaseDataTypeMapper which defines a method signature like map_data_type(...).

Each mapper contains the logic (usually a dictionary or method) that maps source types to target types.

The DataTypeMapperFactory acts as a factory that returns the appropriate mapper class instance:

class DataTypeMapperFactory:
    @staticmethod
    def get_mapper(source_system: str) -> BaseDataTypeMapper:
        if source_system.lower() == "oracle":
            return OracleDataTypeMapper()
        elif source_system.lower() == "sql_server":
            return SqlServerDataTypeMapper()
        else:
            raise ValueError(f"Unsupported source system: {source_system}")

During config and DDL generation, the appropriate mapper is obtained from the factory and used to translate each column’s data type.


## UML Class Diagram

```plaintext
                  +-----------------------------+
                  | DataTypeMapperFactory       |
                  +-----------------------------+
                  | + get_mapper(system)        |
                  +-----------------------------+
                              |
           +------------------+-------------------+
           |                                      |
+----------------------------+     +-------------------------------+
| OracleDataTypeMapper       |     | SqlServerDataTypeMapper       |
+----------------------------+     +-------------------------------+
| + map_data_type(metadata)  |     | + map_data_type(metadata)     |
+----------------------------+     +-------------------------------+
           \________________________________________/
                           |
             +-------------------------------+
             |      BaseDataTypeMapper       |
             +-------------------------------+
             | + map_data_type(metadata)     |
             +-------------------------------+
```

Extensibility

To support a new source system (e.g., PostgreSQL):

Create a class PostgresDataTypeMapper implementing the base interface BaseDataTypeMapper.

Implement the map_data_type method with PostgreSQL-to-Databricks mappings.

Add the class to the DataTypeMapperFactory.get_mapper() logic.

Why use these patterns?

Separation of concerns: Each DB’s data type logic is encapsulated in its own class.

Flexibility: Add or modify source systems independently.

Maintainability: All DB-specific translation is centralized.

Scalability: Adapts to expanding target platforms and new source DBs.



# Schema Extractor Factory Pattern

## What is it?

Schema extraction involves connecting to the source database and retrieving table and column metadata such as:

* Column names
* Data types
* Nullable flags
* Precision and scale
* Identity columns
* Comments

Your project uses a **factory pattern** to dynamically instantiate the appropriate schema extractor class based on the source database type.

---

## How does it work in your project?

* You define a base schema extractor class `BaseSchemaExtractor` which contains the shared interface and method definitions:

  ```python
  class BaseSchemaExtractor:
      def extract_schema(self, tables: List[str]) -> List[Dict]:
          raise NotImplementedError
  ```

* For each supported source database, you create a specific implementation that inherits from the base class:

  * `OracleSchemaExtractor`
  * `SqlServerSchemaExtractor`

* These subclasses implement the `extract_schema` method with database-specific logic to query system tables or metadata views.

* A factory class `SchemaExtractorFactory` takes the source system type as input and returns the appropriate extractor instance:

  ```python
  class SchemaExtractorFactory:
      @staticmethod
      def get_extractor(source_system: str) -> BaseSchemaExtractor:
          if source_system.lower() == "oracle":
              return OracleSchemaExtractor(...)
          elif source_system.lower() == "sql_server":
              return SqlServerSchemaExtractor(...)
          else:
              raise ValueError(f"Unsupported source system: {source_system}")
  ```

* The rest of your pipeline interacts only with the base extractor interface, staying agnostic of the specific database backend.

---

## UML Class Diagram

```plaintext
                 +-----------------------------+
                 | SchemaExtractorFactory      |
                 +-----------------------------+
                 | + get_extractor(system)     |
                 +-----------------------------+
                             |
            +----------------+----------------+
            |                                 |
+--------------------------+     +-----------------------------+
| OracleSchemaExtractor    |     | SqlServerSchemaExtractor    |
+--------------------------+     +-----------------------------+
| + extract_schema(tables) |     | + extract_schema(tables)    |
+--------------------------+     +-----------------------------+
            \____________________________________/
                          |
              +----------------------------+
              |    BaseSchemaExtractor     |
              +----------------------------+
              | + extract_schema(tables)   |
              +----------------------------+
```

---

## Extensibility

To add support for a new source database (e.g., PostgreSQL):

1. Create a new class `PostgresSchemaExtractor` inheriting from `BaseSchemaExtractor`.
2. Implement the `extract_schema` method using PostgreSQL system views.
3. Update `SchemaExtractorFactory.get_extractor()` to handle `'postgres'`:

   ```python
   elif source_system.lower() == "postgres":
       return PostgresSchemaExtractor(...)
   ```

No other parts of the pipeline need to be modified because they rely only on the base class interface.

---

## Why use these patterns?

* **Separation of concerns**: Each DB’s extraction logic is encapsulated within its own class.
* **Flexibility**: New DBs can be added without modifying existing extractor classes.
* **Maintainability**: Centralized logic makes updates and debugging easier.
* **Scalability**: Easy to extend as more databases or extraction targets are introduced.


# Artifact Generation Script

## Overview

`generate_artifacts.py` is the main entry-point script that invokes the code from the `edp_automation` Python wheel. It generates Databricks-compatible DDL and config YAML files for specified tables by:

1. Extracting schema from the source database.
2. Mapping data types using a configurable mapper.
3. Rendering output files using Jinja templates.

---

## Project Directory Structure


edp_automation/
├── data_type_mapper/
│ ├── oracle_data_type_mapper.py
│ ├── sql_server_data_type_mapper.py
│ ├── base_data_type_mapper.py
│ └── data_type_mapper_factory.py
├── schema_extractor/
│ ├── oracle_schema_extractor.py
│ ├── sql_server_schema_extractor.py
│ ├── base_schema_extractor.py
│ └── schema_extractor_factory.py
├── utils/
│ ├── generate_schema_file_from_source.py
│ └── generate_config_and_ddl_from_schema.py
├── resources/
│ └── templates/
│ ├── ddl.sql.j2
│ └── config.yaml.j2

project/
├── generate_artifacts.py
├── resources/
│ ├── templates/
│ │ ├── ddl.sql.j2
│ │ └── config.yaml.j2
│ ├── schemas/
│ │ ├── source_schema_1.json
│ │ └── source_schema_2.json
│ └── output/
│ ├── employees/
│ │ ├── EMPLOYEES.sql
│ │ └── employees.yaml
│ └── departments/
│ │ ├── DEPARTMENTS.sql
│ │ └── departments.yaml


---

## Input Requirements

### Command-line Inputs

| Argument | Description |
|----------|-------------|
| `--config` | Path to a YAML configuration file |
| `--tables` | Comma-separated list of table names to process |

### Configuration YAML (Sample)

```yaml
source_system: oracle
data_source_name: MY_ORACLE_DB
schema_file_location: ./resources/inputs
schema_file_name: source_schema_1.json
template_location: ./resources/templates
ddl_template_name: ddl.sql.j2  # Optional
config_template_name: config.yaml.j2  # Optional
output_location: ./resources/output


💡 If template names are not provided, the script will use default templates from the Python wheel.

How It Works
Prompts for the database password at runtime (not stored).

Connects to the source DB and extracts schema for specified tables.

Writes the schema JSON to schema_file_location.

Loads the schema JSON, maps the data types using the appropriate mapper.

Uses Jinja templates to generate:

.sql DDL file

.yaml config file

Outputs them into a folder named after each table under output_location.

Usage
bash
Copy
Edit
python generate_artifacts.py \
  --config ./resources/edp_config.yaml \
  --tables EMPLOYEES,DEPARTMENTS
⛔ Ensure the config file and schema output folders exist beforehand.

Outputs
For each table (e.g., EMPLOYEES), the script generates:

pgsql
Copy
Edit
resources/output/employees/
├── EMPLOYEES.sql   # Databricks-compatible CREATE TABLE statement
└── EMPLOYEES.yaml  # Table metadata in YAML format
Extensibility
Add new DB support: Implement new extractor + mapper classes and plug them into the respective factory.

Add new templates: Provide ddl_template_name and config_template_name in the config YAML.

Change output structure: Modify Jinja templates to meet custom target formats.

Security Note
The database password is prompted securely at runtime and is not stored in config or code, helping avoid credential leakage.

## 🚀 Execution Flow

### Diagram: Artifact Generation Pipeline

```mermaid
flowchart TD
    A[generate_artifacts.py] --> B[Load config YAML]
    B --> C[Parse source_system and schema input path]
    C --> D[Invoke SchemaExtractorFactory]
    D --> E[Call extract_schema() for tables]
    E --> F[Write extracted metadata to JSON schema file]
    F --> G[Invoke generate_config_and_ddl_from_schema.py]
    G --> H[Load templates (ddl.sql.j2, config.yaml.j2)]
    H --> I[Invoke DataTypeMapperFactory]
    I --> J[Generate .sql and .yaml using Jinja2 templates]
    J --> K[Write output to folders by table name]
```

## Description of Steps

1. **`generate_artifacts.py` Entry Point**
   - Accepts a config YAML and comma-separated table list.
   - Loads config parameters like source system, schema file name, paths, templates, etc.

2. **Schema Extraction**
   - Uses `SchemaExtractorFactory` to load the correct class (e.g., `OracleSchemaExtractor`).
   - Extracts metadata from source DB using `.extract_schema(tables)`.

3. **Schema File Generation**
   - Writes metadata into a schema JSON file (e.g., `source_schema_1.json`).

4. **Config and DDL Generation**
   - Invokes `generate_config_and_ddl_from_schema.py`.
   - Uses `DataTypeMapperFactory` to map types.
   - Loads Jinja2 templates and renders `.sql` and `.yaml` for each table.

5. **Output Writing**
   - Writes generated artifacts to `output/<table_name>/TABLE.sql` and `TABLE.yaml`.
