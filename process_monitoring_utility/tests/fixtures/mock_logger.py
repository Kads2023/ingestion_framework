from unittest.mock import MagicMock


class MockLogController:
    class MockLogHandler(MagicMock):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.debug = MagicMock(name="LogHandler.debug")
            self.info = MagicMock(name="LogHandler.info")
            self.warn = MagicMock(name="LogHandler.warn")
            self.error = MagicMock(name="LogHandler.error")
            self.critical = MagicMock(name="LogHandler.critical")

    def __init__(self):
        self.logger = self.MockLogHandler()


class MockLogger:
    class MockLogHandler:
        def debug(self, log_string):
            print(log_string)
            pass

        def info(self, log_string):
            print(log_string)
            pass

        def warn(self, log_string):
            print(log_string)
            pass

        def error(self, log_string):
            print(log_string)
            pass

    def __init__(self):
        self.logger = self.MockLogHandler()


