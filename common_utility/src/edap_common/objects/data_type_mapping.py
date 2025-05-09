from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType, BooleanType,
    DateType, TimestampType, BinaryType,
    ArrayType, MapType, StructType, DataType
)

class DataTypeMapping:
    type_mapping = {
        "string": StringType,
        "int": IntegerType,
        "integer": IntegerType,
        "long": LongType,
        "short": ShortType,
        "byte": ByteType,
        "float": FloatType,
        "double": DoubleType,
        "decimal": DecimalType,
        "boolean": BooleanType,
        "bool": BooleanType,
        "date": DateType,
        "timestamp": TimestampType,
        "binary": BinaryType,
        "array": ArrayType,
        "map": MapType,
        "struct": StructType,
    }

    def __init__(self, lc, common_utils):
        """
        Args:
            lc: Logger class instance
            common_utils: CommonUtils class instance
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.common_utils = common_utils
        self.lc.logger.info(f"Inside {this_module}")

    def get_type(self, type_name, passed_module=""):
        this_module = f"[{self.this_class_name}.get_type()] - {passed_module} -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "type_name": {
                    "input_value": type_name,
                    "data_type": "str",
                    "check_empty": True,
                },
                "passed_module": {
                    "input_value": passed_module,
                    "data_type": "str",
                },
            }
        )
        final_type_name = type_name.strip().lower()
        if final_type_name in self.type_mapping.keys():
            ret_value = self.type_mapping[final_type_name]
            return ret_value
        else:
            error_msg = (
                f"{this_module} "
                f"Unsupported type: {type_name}"
            )
            self.lc.logger.error(error_msg)
            raise ValueError(error_msg)
