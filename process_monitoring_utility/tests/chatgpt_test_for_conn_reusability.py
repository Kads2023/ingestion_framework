import pytest
import pandas as pd
from sqlalchemy.exc import OperationalError

import process_monitoring as pm  # adjust import if needed


class DummyConn:
    def __init__(self):
        self.closed = False
        self.executed_queries = []
        self.should_fail = False
        self.returns_rows = True
        self.fail_once = False
        self.begin_called = False
        self.trans = DummyTrans(self)

    def begin(self):
        self.begin_called = True
        return self.trans

    def execute(self, query, params):
        self.executed_queries.append((query, params))
        if self.fail_once:
            self.fail_once = False
            raise OperationalError("stmt", "params", "orig")
        if self.should_fail:
            raise Exception("Unexpected failure")
        if self.returns_rows:
            return DummyResult()
        return DummyNoRowsResult()

    def close(self):
        self.closed = True


class DummyTrans:
    def __init__(self, conn):
        self.conn = conn
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class DummyResult:
    def returns_rows(self):
        return True

    def fetchall(self):
        return [(1, "a"), (2, "b")]

    def keys(self):
        return ["id", "val"]


class DummyNoRowsResult:
    def returns_rows(self):
        return False

    def fetchall(self):
        return []

    def keys(self):
        return []


class DummyEngine:
    def __init__(self):
        self.connected = False
        self.disposed = False
        self.conn = DummyConn()

    def connect(self):
        self.connected = True
        return self.conn

    def dispose(self):
        self.disposed = True


@pytest.fixture
def pm_obj(monkeypatch):
    obj = pm.ProcessMonitoring("fake_url", "db", "user", "pwd")
    dummy_engine = DummyEngine()

    def fake_create_engine(*args, **kwargs):
        return dummy_engine

    monkeypatch.setattr(pm, "create_engine", lambda *a, **k: dummy_engine)
    obj._engine = dummy_engine
    obj._conn = dummy_engine.conn
    return obj


def test_create_engine_with_user_password(monkeypatch):
    called = {}

    def fake_engine(url, **kwargs):
        called["url"] = url
        called.update(kwargs)
        return "engine"

    monkeypatch.setattr(pm, "create_engine", fake_engine)
    obj = pm.ProcessMonitoring("url", "db", "u", "p")
    result = obj._create_engine()
    assert result == "engine"
    assert called["url"] == "url"
    assert called["username"] == "u"
    assert called["password"] == "p"


def test_create_engine_with_managed_identity(monkeypatch):
    class DummyCred:
        def get_token(self, scope):
            return type("T", (), {"token": "tok123"})()

    monkeypatch.setattr(pm, "ManagedIdentityCredential", lambda: DummyCred())
    monkeypatch.setattr(pm, "create_engine", lambda url, **kwargs: ("engine", kwargs))

    obj = pm.ProcessMonitoring("url", "db")
    engine, kwargs = obj._create_engine()
    assert kwargs["connect_args"]["access_token"] == "tok123"


def test_get_conn_new(monkeypatch):
    obj = pm.ProcessMonitoring("url", "db", "u", "p")
    dummy_engine = DummyEngine()
    monkeypatch.setattr(pm, "create_engine", lambda *a, **k: dummy_engine)

    obj._engine = None
    obj._conn = None
    conn = obj.get_conn()
    assert isinstance(conn, DummyConn)
    assert obj._engine == dummy_engine


def test_execute_query_success(pm_obj):
    df = pm_obj.execute_query_and_get_results("SELECT 1")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "val"]
    assert pm_obj._conn.trans.committed


def test_execute_query_no_rows(pm_obj):
    pm_obj._conn.returns_rows = False
    df = pm_obj.execute_query_and_get_results("SELECT 1")
    assert df.empty
    assert pm_obj._conn.trans.committed


def test_execute_query_operationalerror_retry(monkeypatch):
    obj = pm.ProcessMonitoring("url", "db")
    dummy_engine = DummyEngine()
    obj._engine = dummy_engine
    conn = dummy_engine.conn
    conn.fail_once = True  # fail first, succeed next

    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    df = obj.execute_query_and_get_results("SELECT 1")
    assert not df.empty
    assert conn.trans.committed


def test_execute_query_operationalerror_max_retries(monkeypatch):
    obj = pm.ProcessMonitoring("url", "db")
    dummy_engine = DummyEngine()
    obj._engine = dummy_engine
    conn = dummy_engine.conn
    conn.should_fail = True

    monkeypatch.setattr(pm.time, "sleep", lambda s: None)
    with pytest.raises(OperationalError):
        obj.execute_query_and_get_results("SELECT 1")


def test_execute_query_generic_exception(pm_obj):
    pm_obj._conn.should_fail = True
    with pytest.raises(Exception):
        pm_obj.execute_query_and_get_results("bad sql")
    assert pm_obj._conn.trans.rolled_back


def test_close_conn(pm_obj):
    engine = pm_obj._engine
    conn = pm_obj._conn
    pm_obj.close_conn()
    assert conn.closed
    assert engine.disposed
    assert pm_obj._conn is None
    assert pm_obj._engine is None


def test_del_triggers_close(monkeypatch):
    called = {}

    def fake_close():
        called["closed"] = True

    obj = pm.ProcessMonitoring("url", "db")
    obj.close_conn = fake_close
    del obj
    assert "closed" in called



import pytest
import pandas as pd
from sqlalchemy.exc import OperationalError
import process_monitoring as pm


class DummyTrans:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class DummyConn:
    def __init__(self, returns_rows=True, fail_once=False, should_fail=False):
        self.closed = False
        self.returns_rows = returns_rows
        self.fail_once = fail_once
        self.should_fail = should_fail
        self.trans = DummyTrans()

    def begin(self):
        return self.trans

    def execute(self, query, params):
        if self.fail_once:
            self.fail_once = False
            raise OperationalError("stmt", "params", "orig")
        if self.should_fail:
            raise Exception("generic error")
        if self.returns_rows:
            return DummyResult()
        return DummyNoRowsResult()

    def close(self):
        self.closed = True


class DummyResult:
    @property
    def returns_rows(self):
        return True

    def fetchall(self):
        return [(1, "a"), (2, "b")]

    def keys(self):
        return ["id", "val"]


class DummyNoRowsResult:
    @property
    def returns_rows(self):
        return False

    def fetchall(self):
        return []

    def keys(self):
        return []


class DummyEngine:
    def __init__(self, conn):
        self.disposed = False
        self.conn = conn

    def connect(self):
        return self.conn

    def dispose(self):
        self.disposed = True


@pytest.mark.parametrize(
    "returns_rows,fail_once,should_fail,expected_exception",
    [
        (True, False, False, None),      # normal rows → commit
        (False, False, False, None),     # no rows → commit
        (True, True, False, None),       # fail once → retry → commit
        (True, True, True, OperationalError),  # fail repeatedly → max retry → raise
        (True, False, True, Exception),  # generic exception → rollback
    ]
)
def test_execute_query_parametrized(monkeypatch, returns_rows, fail_once, should_fail, expected_exception):
    conn = DummyConn(returns_rows=returns_rows, fail_once=fail_once, should_fail=should_fail)
    engine = DummyEngine(conn)
    obj = pm.ProcessMonitoring("url", "db")
    obj._engine = engine
    obj._conn = conn

    # skip actual sleep during retry
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)

    if expected_exception:
        with pytest.raises(expected_exception):
            obj.execute_query_and_get_results("SELECT 1")
        assert conn.trans.rolled_back
        assert not conn.trans.committed
    else:
        df = obj.execute_query_and_get_results("SELECT 1")
        assert isinstance(df, pd.DataFrame)
        # Check commit vs rollback
        assert conn.trans.committed
        assert not conn.trans.rolled_back
        if returns_rows:
            assert df.shape[0] == 2
        else:
            assert df.empty


import pytest
from sqlalchemy.exc import OperationalError
import process_monitoring as pm


class DummyTrans:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class DummyConn:
    def __init__(self, closed=False):
        self.closed = closed
        self.trans = DummyTrans()
        self.closed_flag = False

    def begin(self):
        return self.trans

    def execute(self, query, params):
        return DummyResult()

    def close(self):
        self.closed_flag = True
        self.closed = True


class DummyResult:
    @property
    def returns_rows(self):
        return True

    def fetchall(self):
        return [(1, "a")]

    def keys(self):
        return ["id", "val"]


class DummyEngine:
    def __init__(self, conn):
        self.conn = conn
        self.disposed = False

    def connect(self):
        return self.conn

    def dispose(self):
        self.disposed = True


@pytest.mark.parametrize(
    "initial_conn_closed,expected_recreate",
    [
        (False, False),  # connection open → no recreate
        (True, True),    # connection closed → recreate
        (None, True),    # no connection → create
    ]
)
def test_get_conn_reuse_and_recreate(monkeypatch, initial_conn_closed, expected_recreate):
    conn = DummyConn(closed=initial_conn_closed if initial_conn_closed is not None else False)
    engine = DummyEngine(conn)

    obj = pm.ProcessMonitoring("url", "db")
    obj._engine = engine
    obj._conn = conn if initial_conn_closed is not None else None

    # monkeypatch create_engine to return dummy engine
    monkeypatch.setattr(pm, "create_engine", lambda *a, **k: engine)

    returned_conn = obj.get_conn()
    assert returned_conn == engine.conn
    if expected_recreate:
        # When recreated, connection should be same dummy conn (since we mocked)
        assert obj._conn == engine.conn


@pytest.mark.parametrize(
    "close_conn_initial",
    [True, False, None]
)
def test_close_conn(monkeypatch, close_conn_initial):
    conn = DummyConn()
    engine = DummyEngine(conn)
    obj = pm.ProcessMonitoring("url", "db")
    obj._conn = conn if close_conn_initial is not None else None
    obj._engine = engine if close_conn_initial is not None else None

    obj.close_conn()
    # If there was a connection, it should be closed
    if obj._conn is None:
        assert conn.closed_flag
        assert engine.disposed
    # After closing, attributes should be None
    assert obj._conn is None
    assert obj._engine is None
