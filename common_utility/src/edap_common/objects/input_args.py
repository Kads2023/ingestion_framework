import traceback


class InputArgs:
    default_values_for_input_params = {}
    mandatory_input_params = []

    def __init__(self, lc, common_utils, **kwargs):
        """
        Args:
            lc: Logger class instance
            common_utils: CommonUtils class instance
            **kwargs: Args
        """
        self.this_class_name = f"{type(self).__name__}"
        this_module = f"[{self.this_class_name}.__init__()] -"
        self.lc = lc
        self.common_utils = common_utils
        self.args = kwargs
        self.lc.logger.info(
            f"{this_module} "
            f"self.args --> {self.args}"
        )

    def set_mandatory_input_params(self, passed_mandatory_input_params):
        this_module = f"[{self.this_class_name}.set_mandatory_input_params()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_mandatory_input_params": {
                    "input_value": passed_mandatory_input_params,
                    "data_type": "list",
                },
            },
        )
        for each_key in passed_mandatory_input_params:
            self.mandatory_input_params.append(each_key)

    def set_default_values_for_input_params(
            self, passed_default_values_for_input_params
    ):
        this_module = f"[{self.this_class_name}.set_mandatory_input_params()] -"
        self.common_utils.validate_function_param(
            this_module,
            {
                "passed_default_values_for_input_params": {
                    "input_value": passed_default_values_for_input_params,
                    "data_type": "dict",
                },
            },
        )
        for each_key in passed_default_values_for_input_params.keys():
            each_value = passed_default_values_for_input_params[each_key]
            self.default_values_for_input_params[each_key] = each_value

    def set_default_value(self, passed_key, passed_value=""):
        this_module = f"[{self.this_class_name}.set_default_value()] -"
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
        if passed_key in self.default_values_for_input_params.keys():
            ret_value = self.default_values_for_input_params[passed_key]
        else:
            ret_value = passed_value
        return ret_value

    def get(self, passed_key):
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
        try:
            ret_value = self.args[passed_key]
            if ret_value == "":
                if passed_key in self.mandatory_input_params:
                    error_msg = (
                        f"{this_module} ValueError "
                        f"parameter --> `{passed_key}` "
                        f"which is MANDATORY "
                        f"has empty value --> `{ret_value}`"
                    )
                    self.lc.logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    ret_value = self.set_default_value(passed_key, ret_value)
        except KeyError as e:
            if passed_key in self.mandatory_input_params:
                error_msg = (
                    f"{this_module} KeyError "
                    f"parameter --> `{passed_key}` "
                    f"MANDATORY, ({e}) "
                    f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
                )
                self.lc.logger.error(error_msg)
                raise KeyError(error_msg)
            else:
                ret_value = self.set_default_value(passed_key)
        except Exception as e:
            error_msg = (
                f"{this_module} "
                f"Exception: passed_key --> `{passed_key}`, "
                f"({e}) "
                f"\nTRACEBACK --> \n{traceback.format_exc()}\n"
            )
            self.lc.logger.error(error_msg)
            raise
        return ret_value

    def get_args_keys(self):
        this_module = f"[{self.this_class_name}.get()] -"
        args_keys = list(self.args.keys())
        default_keys = list(self.default_values_for_input_params.keys())
        all_keys = []
        all_keys.extend(args_keys)
        all_keys.extend(default_keys)
        final_keys = list(set(all_keys))
        return final_keys
