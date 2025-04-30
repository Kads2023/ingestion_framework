# CommonUtils Module

The `CommonUtils` class provides a reusable utility set for PySpark-based data pipelines. It encapsulates standard operations such as:
- Date formatting
- Parameter validation
- Type checking and boolean evaluation
- Reading YAML files
- DBUtils handling in Databricks
- Logging

## 📦 Module Overview

```text
edap_common/
├── utils/
    ├── common_utils.py
    └── constants.py
```

## 🔧 Class: `CommonUtils`

### Constructor
```python
CommonUtils(lc)
```
- **lc**: Logger class instance.

---

### 📘 Methods

#### `get_current_time(datetime_format=default_datetime_format)`
Returns current UTC time in specified format.

#### `check_and_evaluate_str_to_bool(passed_str)`
Converts string to a boolean safely.

#### `get_date_split(passed_date, date_format=default_date_format)`
Splits a date string into year, month, and day.

#### `check_and_set_dbutils(dbutils=None)`
Returns a DBUtils object if provided or initializes from the current SparkSession.

#### `read_yaml_file(passed_file_path)`
Reads a YAML configuration file and returns it as a Python dictionary.

#### `validate_function_param(passed_module, params_dict)`
Validates parameters passed into functions using a consistent type and empty check.

---

## 🧩 Constants
Defined in `constants.py`:

```python
default_date_format = "%Y-%m-%d"
default_datetime_format = "%Y-%m-%d %H:%M:%S"

default_true_values_list = ["True", "true", "Yes", "yes", "1"]

data_type_defaults = {
    "str":  {"default_value": "", "type_name": "string"},
    "int":  {"default_value": 0,  "type_name": "integer"},
    "list": {"default_value": [], "type_name": "list"},
    "dict": {"default_value": {}, "type_name": "dictionary"},
    "bool": {"default_value": False, "type_name": "boolean"},
}
```

---

## 🛠️ Example Usage
```python
from edap_common.utils.common_utils import CommonUtils

logger = get_logger()
cu = CommonUtils(logger)

# Get time
print(cu.get_current_time())

# YAML load
config = cu.read_yaml_file("config.yaml")

# Validate param
cu.validate_function_param("[module]", {
    "param1": {"input_value": "value", "data_type": "str", "check_empty": True}
})
```

---

## 🖼️ Diagram

### Utility Flow Overview

```mermaid
graph TD
    A[Function Entry] --> B[Validate Parameters]
    B -->|Success| C{Function Type}
    C --> D[get_current_time]
    C --> E[check_and_evaluate_str_to_bool]
    C --> F[get_date_split]
    C --> G[read_yaml_file]
    C --> H[check_and_set_dbutils]
    D --> I[Return Formatted Time]
    E --> J[Return Bool]
    F --> K[Return (Year, Month, Day)]
    G --> L[Return Dict]
    H --> M[Return DBUtils Obj]
```

---

## ✅ Best Practices
- Always validate inputs using `validate_function_param`.
- Leverage `check_and_set_dbutils()` for DBFS operations in Databricks.
- Use `read_yaml_file()` for config-driven pipeline design.

---

## 🧪 Unit Testing Tips
- Mock `logger` and `SparkSession` where needed.
- Validate exceptions for bad inputs in `validate_function_param`.
- Test YAML parsing and date split using known values.

---

## 📄 License
This module is part of the internal `edap_common` library. Intended for internal use in ETL pipelines and job orchestration systems.

