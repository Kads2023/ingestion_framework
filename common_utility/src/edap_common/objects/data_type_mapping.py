from pyspark.sql.types import (
    StringType, IntegerType, LongType, ShortType, ByteType,
    FloatType, DoubleType, DecimalType, BooleanType,
    DateType, TimestampType, BinaryType,
    ArrayType, MapType, StructType
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
        "decimal": DecimalType,  # will be handled specially
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

    def get_type(self, type_name, precision=None, scale=None, passed_module=""):
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
            type_class = self.type_mapping[final_type_name]

            # Special handling for DecimalType
            if type_class == DecimalType:
                if precision is not None and scale is not None:
                    return DecimalType(precision, scale)
                elif precision is not None:
                    # Spark requires scale when precision is given
                    return DecimalType(precision, 0)
                else:
                    return DecimalType()  # default (10, 0)

            return type_class()
        else:
            error_msg = (
                f"{this_module} "
                f"Unsupported type: {type_name}"
            )
            self.lc.logger.error(error_msg)
            raise ValueError(error_msg)
