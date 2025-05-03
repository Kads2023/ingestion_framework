from fixtures.mock_pyspark_sql import *


def mock_execute_query_and_get_results_return1(*args, **kwargs) -> StubSparkDataFrame:
    return stub_spark_df_jobdetails


def mock_execute_query_and_get_results_return2(*args, **kwargs) -> StubSparkDataFrame:
    return stub_spark_df_jobdetails_error


def mock_execute_query_and_get_results_return01(*args, **kwargs) -> StubSparkDataFrame:
    return stub_spark_df_empty
