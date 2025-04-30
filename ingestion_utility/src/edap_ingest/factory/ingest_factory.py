import importlib
from edap_ingest.ingest.base_ingest import BaseIngest


class IngestFactory:
    """
    Factory class to dynamically load and start ingestion processes based on the given ingest type.

    Attributes:
        ingest_class_base (str): Base module path where specific ingest classes are located.
    """

    ingest_class_base = "edap_ingest.ingest"

    def start_load(
            self,
            passed_input_args,
            passed_job_args,
            passed_common_utils,
            passed_process_monitoring,
            passed_validation_utils,
            passed_dbutils=None
    ):
        """
        Dynamically loads and executes the ingestion class based on the `ingest_type` provided in input arguments.

        Args:
            passed_input_args (dict): Input arguments containing ingest configuration, including `ingest_type`.
            passed_job_args (dict): Job-specific arguments required by the ingestion process.
            passed_common_utils (object): Utility object providing common functionalities like logging.
            passed_process_monitoring (object): Object responsible for monitoring the ingestion process.
            passed_validation_utils (object): Utility object for validating ingestion data.
            passed_dbutils (object, optional): Database utility object for database-related operations (default is None).

        Raises:
            ModuleNotFoundError: If the ingestion module corresponding to the `ingest_type` is not found.
            AttributeError: If the ingestion class corresponding to the `ingest_type` is not found within the module.

        """
        this_module = "[IngestFactory.start_load()] -"
        ingest_type = str(passed_input_args.get("ingest_type")).strip().lower()
        passed_common_utils.log_msg(
            f"{this_module} "
            f"ingest_type --> {ingest_type}"
        )
        class_file_name = f"{ingest_type}_ingest"
        class_name = f"{ingest_type.capitalize()}Ingest"
        class_module = importlib.import_module(
            f"{self.ingest_class_base}.{class_file_name}"
        )
        class_ref = getattr(class_module, class_name, None)
        passed_common_utils.log_msg(
            f"{this_module} "
            f"class_name --> {class_name}, "
            f"type of class_ref --> {type(class_ref)}"
        )
        ingest_obj: BaseIngest = class_ref(
            passed_input_args,
            passed_job_args,
            passed_common_utils,
            passed_process_monitoring,
            passed_validation_utils,
            passed_dbutils
        )
        passed_common_utils.log_msg(
            f"{this_module} "
            f"type of ingest_obj --> {type(ingest_obj)}"
        )
        ingest_obj.run_load()
