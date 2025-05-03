import sys
from unittest.mock import MagicMock


def pytest_configure():
    #### Create the empty mock modules ####
    sys.modules["databricks"] = MagicMock()
    sys.modules["databricks.sdk"] = MagicMock()
    sys.modules["databricks.sdk.runtime"] = MagicMock()

    sys.modules["pyspark"] = MagicMock()
    sys.modules["pyspark.sql"] = MagicMock()
    sys.modules["pyspark.sql.functions"] = MagicMock()
    sys.modules["pyspark.sql.types"] = MagicMock()
    sys.modules["pyspark.dbutils"] = MagicMock()
