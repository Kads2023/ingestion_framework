from edap_common.objects.data_type_mapping import DataTypeMapping


class JobArgs:

    def __init__(self, lc, common_utils):
        """
        Args:
            lc: Logger class instance
            common_utils: CommonUtils class instance
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.job_dict = {}
        self.lc = lc
        self.common_utils = common_utils
        self.data_type_mapping = DataTypeMapping(lc, common_utils)
        self.job_run_start_time = self.common_utils.get_current_time()
        self.set("run_start_time", self.job_run_start_time)

    def get_type(self, passed_type_str, passed_module=""):
        this_module = f"[{self.this_class_name}.get_type()] - {passed_module} -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_type_str": {
                    "input_value": passed_type_str,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        return self.data_type_mapping.get_type(passed_type_str, passed_module)

    def get(self, passed_key, passed_default=""):
        this_module = f"[{self.this_class_name}.get()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_key": {
                    "input_value": passed_key,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        return_val = passed_default
        if passed_key in self.job_dict.keys():
            return_val = self.job_dict.get(passed_key)
        return return_val

    def get_mandatory(self, passed_key, passed_default=""):
        this_module = f"[{self.this_class_name}.get_mandatory()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_key": {
                    "input_value": passed_key,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        if passed_key in self.job_dict.keys():
            return_val = self.job_dict.get(passed_key)
            return return_val
        else:
            error_msg = (
                f"{this_module} KEyError: "
                f"passed_key --> {passed_key} "
                f"NOT FOUND"
            )
            self.lc.logger.error(error_msg)
            raise KeyError(error_msg)

    def set(self, passed_key, passed_value):
        this_module = f"[{self.this_class_name}.set()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_key": {
                    "input_value": passed_key,
                    "data_type": "str",
                    "check_empty": True,
                },
            },
        )
        self.job_dict[passed_key] = passed_value

    def get_job_dict(self):
        this_module = f"[{self.this_class_name}.get_job_dict()] -"
        self.lc.logger.info(
            f"{this_module} "
            f"job_dict --> "
            f"{self.job_dict}"
        )
        return self.job_dict
