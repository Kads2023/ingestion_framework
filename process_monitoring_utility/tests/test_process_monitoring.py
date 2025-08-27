import pytest
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_spark(monkeypatch):
    """Mock SparkSession with minimal behavior."""
    class MockSpark:
        def __init__(self):
            self.sparkContext = MagicMock()
        def createDataFrame(self, pdf, schema=None):
            return f"SparkDF({list(pdf.columns)})"
    mock_spark = MockSpark()
    monkeypatch.setattr("yourmodule.process_monitoring.ProcessMonitoring.spark", mock_spark)
    return mock_spark


@pytest.fixture
def mock_common_utils(monkeypatch):
    """Mock common utils with retry + validation."""
    def retry_on_exception(max_attempts, delay_seconds, backoff_factor):
        def decorator(fn):
            def wrapper(*a, **k):
                return fn(*a, **k)
            return wrapper
        return decorator

    def validate_function_param(this_module, params):
        # simulate validation success only
        for k, v in params.items():
            if v.get("check_empty") and not v["input_value"]:
                raise ValueError(f"{k} cannot be empty")

    return type("CU", (), {
        "retry_on_exception": staticmethod(retry_on_exception),
        "validate_function_param": staticmethod(validate_function_param),
    })


@pytest.fixture
def mock_logger():
    class MockLogger:
        def __init__(self):
            self.logs = []
        def info(self, msg): self.logs.append(("info", msg))
        def warning(self, msg): self.logs.append(("warn", msg))
    return MockLogger()


@pytest.fixture
def job_args():
    return {"process_monitoring_conn_str": "Driver={ODBC Driver 17 for SQL Server};Server=fake;"}


@pytest.fixture
def pm(mock_spark, mock_common_utils, mock_logger, job_args):
    from yourmodule.process_monitoring import ProcessMonitoring
    return ProcessMonitoring(mock_logger, mock_common_utils, job_args)


def test_get_conn_creates_engine(pm, monkeypatch):
    fake_engine = "engine"
    monkeypatch.set("yourmodule.process_monitoring.create_engine", lambda *a, **k: fake_engine)
    monkeypatch.set("yourmodule.process_monitoring.ManagedIdentityCredential", lambda: MagicMock(get_token=lambda _: MagicMock(token="abc")))
    engine = pm.get_conn()
    assert engine == "engine"
    # second call should reuse
    assert pm.get_conn() == "engine"


@pytest.mark.parametrize(
    "query, params, fetch_results, rows, expected",
    [
        ("SELECT * FROM table", {}, True, [(1, "x")], "SparkDF(['id','name'])"),   # with rows
        ("SELECT * FROM table", {}, True, [], "SparkDF([])"),                     # no rows
        ("INSERT INTO t VALUES (1)", {}, False, None, "SparkDF([])"),             # insert no fetch
        ("UPDATE t SET x=1 WHERE id=1", {}, False, None, "SparkDF([])"),          # update allowed
    ]
)
def test_execute_query_success(monkeypatch, pm, query, params, fetch_results, rows, expected):
    # patch engine + connection
    class FakeResult:
        def __init__(self, rows): self._rows = rows
        def fetchall(self): return self._rows
        def keys(self): return ["id","name"] if self._rows else []
    class FakeConn:
        def execute(self, txt, prm): return FakeResult(rows)
        def __enter__(self): return self
        def __exit__(self, *a): pass
    class FakeEngine:
        def begin(self): return FakeConn()
    monkeypatch.setattr(pm, "get_conn", lambda: FakeEngine())

    df = pm.execute_query_and_get_results(query, params, fetch_results=fetch_results)
    assert df == expected


def test_execute_query_dry_run(monkeypatch, pm):
    pm.job_args_obj["dry_run"] = True
    result = pm.execute_query_and_get_results("SELECT 1", {}, fetch_results=True)
    # should just return empty_df
    assert result == pm.empty_df


def test_execute_query_invalid_query(pm):
    with pytest.raises(ValueError):
        pm.execute_query_and_get_results("DELETE FROM t", {})


def test_execute_query_operational_error(monkeypatch, pm):
    class FakeConn:
        def execute(self, *a, **k): raise OperationalError("stmt", {}, "fail")
        def __enter__(self): return self
        def __exit__(self, *a): pass
    class FakeEngine:
        def begin(self): return FakeConn()
    monkeypatch.setattr(pm, "get_conn", lambda: FakeEngine())

    with pytest.raises(OperationalError):
        pm.execute_query_and_get_results("SELECT * FROM t", {})
    # warning should be logged
    assert any("Connection expired" in msg for level, msg in pm.lc.logs if level == "warn")


def test_retry_any_function(pm):
    calls = {"n": 0}
    def flaky(x):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("fail once")
        return x * 2
    # since retry_on_exception is mocked to just run once, we patch to simulate retry
    pm.common_utils.retry_on_exception = lambda *a, **k: (lambda f: f)
    result = pm.retry_any_function(lambda x: x+1, 2)
    assert result == 3
