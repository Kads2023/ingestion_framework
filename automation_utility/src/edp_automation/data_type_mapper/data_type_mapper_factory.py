import importlib
from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper


class DataTypeMapperFactory:
    @staticmethod
    def get_mapper(source_system_type: str) -> BaseDataTypeMapper:
        this_module = "[DataTypeMappingFactory.get_mapper()] -"
        final_source_system_type = source_system_type.strip().lower()
        try:
            print(
                f"{this_module} "
                f"final_source_system_type --> {final_source_system_type}"
            )
            class_file_name = f"{final_source_system_type}_data_type_mapper"
            class_name = f"{final_source_system_type.capitalize()}DataTypeMapper"
            class_module = importlib.import_module(
                f"edp_automation.data_type_mapper.{class_file_name}"
            )
            class_ref = getattr(class_module, class_name, None)
            print(
                f"{this_module} "
                f"class_name --> {class_name}, "
                f"type of class_ref --> {type(class_ref)}"
            )
            mapper_obj: BaseDataTypeMapper = class_ref()
            print(
                f"{this_module} "
                f"type of mapper_obj --> {type(mapper_obj)}"
            )
            return mapper_obj
        except ModuleNotFoundError as ex:
            error_msg = (
                f"{this_module} UNKNOWN: "
                f"final_source_system_type --> {final_source_system_type}, "
                f"Implementation available for "
                f"ORACLE / SQL SERVER, ({ex})"
            )
            print(error_msg)
            raise
        except Exception as ex:
            error_msg = (
                f"{this_module} "
                f"final_source_system_type --> {final_source_system_type}, "
                f"({ex})"
            )
            print(error_msg)
            raise
