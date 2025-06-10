import importlib
from edp_automation.data_type_mapper.base_data_type_mapper import BaseDataTypeMapper


class DataTypeMapperFactory:
    @staticmethod
    def get_mapper(source_system_type: str) -> BaseDataTypeMapper:
        this_module = "[DataTypeMapperFactory.get_mapper()] -"

        if not isinstance(source_system_type, str):
            raise TypeError(f"{this_module} Expected source_system_type: {source_system_type} to be a String")

        if not source_system_type:
            raise ValueError(f"{this_module} Invalid source_system_type: {source_system_type}")

        final_source_system_type = source_system_type.strip().lower()
        try:
            print(
                f"{this_module} "
                f"final_source_system_type --> {final_source_system_type}"
            )
            class_file_name = f"{final_source_system_type}_data_type_mapper"
            class_name = f"{final_source_system_type.capitalize()}DataTypeMapper"
            module_path = f"edp_automation.data_type_mapper.{class_file_name}"

            class_module = importlib.import_module(module_path)
            class_ref = getattr(class_module, class_name, None)
            if class_ref is None:
                raise ImportError(f"Class {class_name} not found in module {module_path}")
            mapper_obj: BaseDataTypeMapper = class_ref()
            print(
                f"{this_module} "
                f"type of mapper_obj --> {type(mapper_obj)}"
            )
            if not isinstance(mapper_obj, BaseDataTypeMapper):
                raise TypeError(f"{class_name} is not a subclass of BaseDataTypeMapper")

            return mapper_obj
        except ModuleNotFoundError as ex:
            error_msg = (
                f"{this_module} UNKNOWN: "
                f"final_source_system_type --> {final_source_system_type}, "
                f"Implementation available for "
                f"ORACLE / SQL SERVER, ({ex})"
            )
            print(error_msg)
            raise ModuleNotFoundError(error_msg) from ex
        except Exception as ex:
            error_msg = (
                f"{this_module} "
                f"final_source_system_type --> {final_source_system_type}, "
                f"({ex})"
            )
            print(error_msg)
            raise RuntimeError(error_msg) from ex
