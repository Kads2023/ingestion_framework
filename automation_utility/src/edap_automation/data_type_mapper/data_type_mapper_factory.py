import importlib
from edap_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper


class DataTypeMapperFactory:
    @staticmethod
    def get_mapper(source_system: str) -> BaseDataTypeMapper:
        this_module = "[DataTypeMappingFactory.get_mapper()] -"
        final_source_system = source_system.strip().lower()
        try:
            print(
                f"{this_module} "
                f"final_source_system --> {final_source_system}"
            )
            class_file_name = f"{final_source_system}_data_type_mapper"
            class_name = f"{final_source_system.capitalize()}DataTypeMapper"
            class_module = importlib.import_module(
                f"edap_automation.data_type_mapper.{class_file_name}"
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
                f"final_source_system --> {final_source_system}, "
                f"Implementation available for "
                f"ORACLE / SQL SERVER, ({ex})"
            )
            print(error_msg)
            raise
        except Exception as ex:
            error_msg = (
                f"{this_module} "
                f"final_source_system --> {final_source_system}, "
                f"({ex})"
            )
            print(error_msg)
            raise
