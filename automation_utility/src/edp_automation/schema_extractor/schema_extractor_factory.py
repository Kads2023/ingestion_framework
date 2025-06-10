import importlib
from edp_automation.schema_extractor.base_schema_extractor import BaseSchemaExtractor


class SchemaExtractorFactory:
    @staticmethod
    def get_schema_extractor(source_system_type: str, **kwargs) -> BaseSchemaExtractor:
        this_module = "[SchemaExtractorFactory.get_mapper()] -"
        if not isinstance(source_system_type, str):
            raise TypeError(f"{this_module} Expected source_system_type: {source_system_type} to be a String")

        if not source_system_type:
            raise ValueError(f"{this_module} Invalid source_system_type: {source_system_type}")

        final_source_system_type = source_system_type.strip().lower()
        try:
            print(
                f"{this_module} "
                f"final_source_system_type --> {final_source_system_type}, "
                f"**kwargs --> {kwargs}"
            )
            class_file_name = f"{final_source_system_type}_schema_extractor"
            class_name = f"{final_source_system_type.capitalize()}SchemaExtractor"
            module_path = f"edp_automation.schema_extractor.{class_file_name}"
            class_module = importlib.import_module(module_path)

            class_ref = getattr(class_module, class_name, None)
            print(
                f"{this_module} "
                f"class_name --> {class_name}, "
                f"type of class_ref --> {type(class_ref)}"
            )
            if class_ref is None:
                raise ImportError(f"Class {class_name} not found in module {module_path}")

            mapper_obj: BaseSchemaExtractor = class_ref(**kwargs)
            print(
                f"{this_module} "
                f"type of mapper_obj --> {type(mapper_obj)}"
            )
            if not isinstance(mapper_obj, BaseSchemaExtractor):
                raise TypeError(f"{class_name} is not a subclass of BaseSchemaExtractor")

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
