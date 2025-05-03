from unittest.mock import MagicMock


class MockJobArgs(MagicMock):
    def __init__(self, *args, return_map=None, **kwargs):
        super().__init__(*args, **kwargs)
        return_map = return_map or {}

        def _side_effect_get(passed_key):
            return return_map.get(passed_key, "<<no such jobargs key>>")

        self.get = MagicMock(
            name="JobArgs.get",
            side_effect=_side_effect_get
        )
