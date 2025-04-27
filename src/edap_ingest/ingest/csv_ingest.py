from edap_ingest.ingest.base_ingest import BaseIngest


class CsvIngest(BaseIngest):
    def __init__(
            self,
            input_args,
            job_args,
            common_utils,
            process_monitoring,
            validation_utils,
            dbutils=None
    ):
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        super().__init__(
            input_args,
            job_args,
            common_utils,
            process_monitoring,
            validation_utils,
            dbutils
        )
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def read_and_set_input_args(self):
        this_module = f"[{self.this_class_name}.read_and_set_input_args()] -"
        super().read_and_set_input_args()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def read_and_set_common_config(self):
        this_module = f"[{self.this_class_name}.read_and_set_common_config()] -"
        super().read_and_set_common_config()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def read_and_set_table_config(self):
        this_module = f"[{self.this_class_name}.read_and_set_table_config()] -"
        super().read_and_set_table_config()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def pre_load(self):
        this_module = f"[{self.this_class_name}.pre_load()] -"
        super().pre_load()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def form_schema_from_dict(self):
        this_module = f"[{self.this_class_name}.form_schema_from_dict()] -"
        super().form_schema_from_dict()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def form_source_and_target_locations(self):
        this_module = f"[{self.this_class_name}.form_source_and_target_locations()] -"
        super().form_source_and_target_locations()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def collate_columns_to_add(self):
        this_module = f"[{self.this_class_name}.collate_columns_to_add()] -"
        super().collate_columns_to_add()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def load(self):
        this_module = f"[{self.this_class_name}.load()] -"
        super().load()
        dry_run = self.job_args_obj.get("dry_run")
        source_location = self.job_args_obj.get("source_location")
        target_location = self.job_args_obj.get("target_location")
        schema_struct = self.job_args_obj.get("schema_struct")
        self.common_utils_obj.log_msg(
            f"Inside {this_module} "
            f"dry_run --> {dry_run}, "
            f"source_location --> {source_location}, "
            f"target_location --> {target_location}, "
            f"schema_struct --> {schema_struct}"
        )
        if schema_struct:
            source_data = self.spark.read.load(
                source_location,
                format="csv",
                header="true"
            ).schema(schema_struct)
        else:
            source_data = self.spark.read.load(
                source_location,
                format="csv",
                inferSchema="true",
                header="true"
            )
        source_row_counts = source_data.count()
        self.job_args_obj.set("source_row_count", source_row_counts)
        self.job_args_obj.set("run_row_count", source_row_counts)
        columns_to_be_added = self.job_args_obj.get(
            "columns_to_be_added"
        )
        self.common_utils_obj.log_msg(
            f"Inside {this_module} "
            f"columns_to_be_added --> {columns_to_be_added}"
        )
        self.validation_obj.run_validations(source_data)
        if not dry_run:
            source_data.write.mode("append").saveAsTable(target_location)

    def post_load(self):
        this_module = f"[{self.this_class_name}.post_load()] -"
        super().post_load()
        self.common_utils_obj.log_msg(f"Inside {this_module}")

    def run_load(self):
        this_module = f"[{self.this_class_name}.run_load()] -"
        super().run_load()
        self.common_utils_obj.log_msg(f"Inside {this_module}")
