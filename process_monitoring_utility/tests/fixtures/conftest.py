import sys
from unittest.mock import MagicMock


def pytest_configure():
    #### Create the empty mock modules ####
    sys.modules["azure"] = MagicMock()
    sys.modules["azure.identity"] = MagicMock()

    sys.modules["pyodbc"] = MagicMock()

    sys.modules["pandas"] = MagicMock()

    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.types"] = MagicMock()
