from unittest.mock import MagicMock


stub_current_datetime = "2025-11-30 12:34:56"


class MockCommonUtils(MagicMock):
    def __init__(self, *args, return_map=None, **kwargs):
        super().__init__(*args, **kwargs)

        self.get_current_time = MagicMock(
            name="CommonUtils.get_current_time",
            return_value=stub_current_datetime
        )
