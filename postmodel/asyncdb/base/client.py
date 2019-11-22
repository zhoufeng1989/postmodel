import logging
import sys
from typing import Any, Sequence

from pypika import Query

from postmodel.asyncdb.base.executor import BaseExecutor
from postmodel.asyncdb.base.schema_generator import BaseSchemaGenerator
from postmodel.exceptions import TransactionManagementError

from contextvars import ContextVar


class Capabilities:
    """
    DB Client Capabilities indicates the supported feature-set,
    and is also used to note common workarounds to defeciences.

    Defaults are set with the following standard:

    * Defeciences: assume it is working right.
    * Features: assume it doesn't have it.

    Fields:

    ``dialect``:
        Dialect name of the DB Client driver.
    ``safe_indexes``:
        Indicates that this DB supports optional index creation using ``IF NOT EXISTS``.
    ``requires_limit``:
        Indicates that this DB requires a ``LIMIT`` statement for an ``OFFSET`` statement to work.
    """

    def __init__(
        self,
        dialect: str,
        *,
        # Is the connection a Daemon?
        daemon: bool = True,
        # Deficiencies to work around:
        safe_indexes: bool = True,
        requires_limit: bool = False,
        inline_comment: bool = False,
        pooling: bool = False
    ) -> None:
        super().__setattr__("_mutable", True)

        self.dialect = dialect
        self.daemon = daemon
        self.requires_limit = requires_limit
        self.safe_indexes = safe_indexes
        self.inline_comment = inline_comment
        self.pooling = pooling
        super().__setattr__("_mutable", False)

    def __setattr__(self, attr, value):
        if not getattr(self, "_mutable", False):
            raise AttributeError(attr)
        return super().__setattr__(attr, value)

    def __str__(self) -> str:
        return str(self.__dict__)


class BaseDBAsyncClient:
    query_class = Query
    executor_class = BaseExecutor
    schema_generator = BaseSchemaGenerator
    capabilities = Capabilities("")

    def __init__(self, connection_name: str, fetch_inserted: bool = True, **kwargs) -> None:
        log_config = kwargs['enable_log']
        if log_config:
            self.log = logging.getLogger("db_client")
        else:
            self.log = None

        self.connection_name = connection_name
        self.fetch_inserted = fetch_inserted
        self._current_transaction = ContextVar(self.connection_name, default=self)  # Type: dict

    async def create_connection(self, with_db: bool) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def close(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_create(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_delete(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def acquire_connection(self):
        raise NotImplementedError()  # pragma: nocoverage

    def get_current_transaction(self) -> "BaseDBAsyncClient":
        return self._current_transaction.get()

    def _in_transaction(self) -> "BaseTransactionWrapper":
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_insert(self, query: str, values: list) -> Any:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_query(self, query: str) -> Sequence[dict]:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_script(self, query: str) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    # async def execute_explain(self, query: str) -> Sequence[dict]:
    #     raise NotImplementedError()  # pragma: nocoverage


class ConnectionWrapper:
    __slots__ = ("connection", "lock")

    def __init__(self, connection, lock) -> None:
        self.connection = connection
        self.lock = lock

    async def __aenter__(self):
        await self.lock.acquire()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.lock.release()


class BaseTransactionWrapper:
    async def start(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def release(self, connection) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def finalize(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            if issubclass(exc_type, TransactionManagementError):
                await self.finalize()
            else:
                await self.rollback()
        else:
            await self.commit()
