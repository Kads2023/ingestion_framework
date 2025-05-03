from unittest.mock import MagicMock


class MockPyODBCCursor(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MockPyODBCConnection(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.connection_string = kwargs.get("connection_string")
        self.attrs_before = kwargs.get("attrs_before")

        # .cursor() is defined within a unit test function, returning a MockPyODBCCursor()
