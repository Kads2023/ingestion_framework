class LogWrapper:
    debug_list = ["debug", "d"]
    info_list = ["info", "information", "i"]
    warn_list = ["warn", "warning", "w"]
    error_list = ["error", "exception", "e"]

    def __init__(self, lc):
        self.this_class_name = f"{type(self).__name__}"
        self.lc = lc

    def log_or_print(self, passed_log_string, passed_logger_type="info"):
        this_module = f"[{self.this_class_name}.log_or_print()] -"
        if passed_logger_type is not str:
            passed_logger_type = "info"
        try:
            logger_type = passed_logger_type.strip().lower()
            if logger_type in self.debug_list:
                self.lc.logger.debug(f"{passed_log_string}")
            elif logger_type in self.info_list:
                self.lc.logger.info(f"{passed_log_string}")
            elif logger_type in self.warn_list:
                self.lc.logger.warn(f"{passed_log_string}")
            elif logger_type in self.error_list:
                self.lc.logger.error(f"{passed_log_string}")
            else:
                self.lc.logger.info(f"{passed_log_string}")
        except Exception as e:
            error_msg = (
                f"{this_module} "
                f"passed_log_string --> "
                f"{passed_log_string} "
                f"error while logging ({e})"
            )
            try:
                self.lc.logger.error(error_msg)
                raise Exception(error_msg)
            except Exception as e:
                print(error_msg)
                raise Exception(error_msg)
