from unittest.mock import MagicMock

stub_az_token = "sample_token"
stub_az_token_utf16le = b""


class MockDefaultAzureCredential(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        return_token_obj = MagicMock(name="return_token_obj")
        return_token_obj.token = stub_az_token

        self.get_token = MagicMock(
            name="DefaultAzureCredential.get_token",
            return_value=return_token_obj
        )