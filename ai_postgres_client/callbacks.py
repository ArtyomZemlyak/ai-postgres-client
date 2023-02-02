"""
File contains description of class PostgresCallback.
"""

from typing import Any
from functools import wraps

from ai_postgres_client import PostgresClient


class PostgresCallback:
    """
    A class for callbacks to PostgeSQL DB. Using PostgresClient.update_value func.
    """

    def __init__(
        self, table_name: str, idx_column: str, idx_val: Any, val_column: str
    ) -> None:
        self.postgres = PostgresClient()
        self.postgres.connect()
        self.table_name = table_name
        self.idx_column = idx_column
        self.idx_val = idx_val
        self.val_column = val_column

    def __call__(self, state: Any, *args: Any, **kwds: Any) -> Any:
        self.postgres.update_value(
            table_name=self.table_name,
            idx_column=self.idx_column,
            idx_val=self.idx_val,
            val_column=self.val_column,
            val=state,
        )
