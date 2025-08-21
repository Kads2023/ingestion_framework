import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from azure.identity import ManagedIdentityCredential


MAX_ATTEMPTS = 3
DELAY_SECONDS = 2
BACKOFF_FACTOR = 1.0


class ProcessMonitoring:
    def __init__(self, jdbc_url: str, db_name: str, user: str = None, password: str = None):
        """
        Initialize with connection details. Supports Managed Identity if no user/password provided.
        """
        self.jdbc_url = jdbc_url
        self.db_name = db_name
        self.user = user
        self.password = password
        self._engine = None
        self._conn = None

    def _create_engine(self):
        """Create SQLAlchemy engine, using Managed Identity if creds not provided."""
        if self.user and self.password:
            return create_engine(self.jdbc_url, username=self.user, password=self.password)
        else:
            # Managed Identity Token (example for Azure SQL)
            credential = ManagedIdentityCredential()
            token = credential.get_token("https://database.windows.net/.default")
            return create_engine(
                self.jdbc_url,
                connect_args={"access_token": token.token}
            )

    def get_conn(self):
        """
        Get a reusable connection.
        - If first time: create engine + connection
        - If connection dropped: recreate it
        """
        if self._conn is None or self._conn.closed:
            if self._engine is None:
                self._engine = self._create_engine()
            self._conn = self._engine.connect()
        return self._conn

    def execute_query_and_get_results(self, query: str, params: dict = None) -> pd.DataFrame:
        """
        Execute SQL with automatic commit/rollback.
        Retries on transient failures with reconnect.
        """
        attempts = 0
        while attempts < MAX_ATTEMPTS:
            conn = self.get_conn()
            trans = conn.begin()
            try:
                result = conn.execute(text(query), params or {})
                df = pd.DataFrame(result.fetchall(), columns=result.keys()) if result.returns_rows else pd.DataFrame()
                trans.commit()
                return df
            except OperationalError as e:
                # Likely connection dropped
                trans.rollback()
                self.close_conn()  # force reconnect on next attempt
                attempts += 1
                delay = DELAY_SECONDS * (BACKOFF_FACTOR ** attempts)
                time.sleep(delay)
                if attempts >= MAX_ATTEMPTS:
                    raise
            except Exception:
                trans.rollback()
                raise

    def close_conn(self):
        """Close current connection and engine cleanly."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
        if self._engine is not None:
            try:
                self._engine.dispose()
            except Exception:
                pass
            self._engine = None

    def __del__(self):
        """Ensure cleanup at object destruction."""
        self.close_conn()
