# PySpark Utility Classes

## Overview

This module provides utility classes for managing input arguments, data type mappings, and job-level metadata in PySpark data pipelines. These abstractions help streamline validation, logging, and type safety in distributed data processing.

---

## 📦 Modules & Classes

### 1. `DataTypeMapping`

**Purpose:** Maps config-friendly string names to `pyspark.sql.types` classes.

#### Data Type Mapping Diagram

```
"string"    --> StringType
"int"       --> IntegerType
"float"     --> FloatType
"boolean"   --> BooleanType
"date"      --> DateType
... and more
```

#### Constructor:
```python
DataTypeMapping(lc, common_utils)
```

#### Method:
```python
get_type(type_name: str, passed_module: str = "") -> DataType
```
- Returns corresponding PySpark type.
- Logs and raises errors on invalid input.

#### Example:
```python
dtm = DataTypeMapping(logger, utils)
dtm.get_type("string")  # returns StringType
```

---

### 2. `InputArgs`

**Purpose:** Manage, validate, and default job parameters from `**kwargs`.

#### Key Responsibilities Diagram

```
+-------------------------+
|      InputArgs          |
+-------------------------+
| - Mandatory Params      |
| - Default Values        |
| - Runtime Args (kwargs) |
+-------------------------+
| + set_mandatory_params  |
| + set_defaults          |
| + get(key)              |
+-------------------------+
```

#### Constructor:
```python
InputArgs(lc, common_utils, **kwargs)
```

#### Key Methods:
```python
set_mandatory_input_params(list)
set_default_values_for_input_params(dict)
get(key: str)
```

#### Example:
```python
args = InputArgs(logger, utils, file_path="data.csv")
args.set_mandatory_input_params(["file_path"])
args.set_default_values_for_input_params({"mode": "overwrite"})

path = args.get("file_path")  # "data.csv"
mode = args.get("mode")       # "overwrite"
```

---

### 3. `JobArgs`

**Purpose:** Centralized job context manager for Spark jobs.

#### Class Diagram
```
+---------------------------+
|         JobArgs           |
+---------------------------+
| - job_dict                |
| - run_start_time          |
| - data_type_mapping       |
+---------------------------+
| + set(key, value)         |
| + get(key, default)       |
| + get_type(type_string)   |
+---------------------------+
```

#### Constructor:
```python
JobArgs(lc, common_utils)
```

#### Example:
```python
job_args = JobArgs(logger, utils)
job_args.set("source", "S3")
print(job_args.get("source"))
print(job_args.get("run_start_time"))
```

---

## 🔧 Dependencies

Make sure the following are implemented and available:

- `pyspark`
- `common_utils` with methods:
  - `validate_function_param(module, param_dict)`
  - `get_current_time()`
- `lc`: logger exposing `.logger.info()` and `.logger.error()`

---

## 📁 Suggested Directory Structure

```
your_project/
│
├── edap_common/
│   └── objects/
│       ├── data_type_mapping.py
│       ├── input_args.py
│       ├── job_args.py
│
├── utils/
│   ├── logger.py
│   └── common_utils.py
│
└── main.py
```

---

## ✅ Usage Example

```python
from edap_common.objects.input_args import InputArgs
from edap_common.objects.job_args import JobArgs
from utils.logger import Logger
from utils.common_utils import CommonUtils

lc = Logger()
utils = CommonUtils()

args = InputArgs(lc, utils, input_path="data/input/")
args.set_mandatory_input_params(["input_path"])
args.set_default_values_for_input_params({"mode": "overwrite"})

input_path = args.get("input_path")
mode = args.get("mode")

job = JobArgs(lc, utils)
print(job.get("run_start_time"))
print(job.get_type("boolean"))  # Returns BooleanType
```

---

## 🧠 Best Practices

- Always define mandatory parameters first.
- Use `JobArgs` as a centralized config handler.
- Leverage `DataTypeMapping` for all schema type resolutions.

---

## ✍️ Author

Generated and documented using OpenAI's ChatGPT.

---

## 📜 License

MIT License (or your project-specific license)

