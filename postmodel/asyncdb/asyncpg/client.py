import asyncio
import logging
from functools import wraps
from typing import List, Optional, Union, SupportsInt, TYPE_CHECKING  # noqa

import asyncpg
from pypika import PostgreSQLQuery

from postmodel.asyncdb.asyncpg.executor import AsyncpgExecutor
from postmodel.asyncdb.asyncpg.schema_generator import AsyncpgSchemaGenerator
from postmodel.asyncdb.base.client import (
    BaseDBAsyncClient,
    BaseTransactionWrapper,
    Capabilities,
    ConnectionWrapper,
)
from postmodel.exceptions import (
    DBConnectionError,
    IntegrityError,
    OperationalError,
    TransactionManagementError,
)
from postmodel.transactions import current_transaction_map
from postmodel.utils import get_current_task

if TYPE_CHECKING:
    from typing import AsyncContextManager


def translate_exceptions(func):
    @wraps(func)
    async def translate_exceptions_(self, *args):
        try:
            return await func(self, *args)
        except asyncpg.SyntaxOrAccessError as exc:
            raise OperationalError(exc)
        except asyncpg.IntegrityConstraintViolationError as exc:
            raise IntegrityError(exc)

    return translate_exceptions_


class PoolConnectionDispatcher:
    __slots__ = ("pool", "connection", "log")

    def __init__(self, pool: asyncpg.pool.Pool, enable_log: bool=False) -> None:
        self.pool = pool
        self.connection = None
        if enable_log:
            self.log = logging.getLogger("db_client")
        else:
            self.log = None

    async def __aenter__(self):
        self.connection = await self.pool.acquire()
        current_task = get_current_task()
        if self.log:
            self.log.debug("Task {task} acquired connection {connection} in PoolConnectionDispatcher.".format(
                task=id(current_task), connection=self.connection))
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        current_task = get_current_task()
        await self.pool.release(self.connection)
        if self.log:
            self.log.debug("Task {task} released connection {connection} in PoolConnectionDispatcher.".format(
                task=id(current_task), connection=self.connection))


class AsyncpgDBClient(BaseDBAsyncClient):
    DSN_TEMPLATE = "postgres://{user}:{password}@{host}:{port}/{database}"
    query_class = PostgreSQLQuery
    executor_class = AsyncpgExecutor
    schema_generator = AsyncpgSchemaGenerator
    capabilities = Capabilities("postgres", pooling=True)

    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        port: SupportsInt,
        min_size: SupportsInt = 10,
        max_size: SupportsInt = 10,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = int(port)  # make sure port is int type
        self.extra = kwargs.copy()
        self.schema = self.extra.pop("schema", None)
        self.extra.pop("connection_name", None)
        self.extra.pop("fetch_inserted", None)
        self.extra.pop("loop", None)
        self.extra.pop("connection_class", None)
        self.extra.pop("enable_log", None)

        self._template = {}  # type: dict
        self._pool = None  # Type: Optional[asyncpg.pool.Pool]
        self.min_size = int(min_size)
        self.max_size = int(max_size)
        self._transaction_class = TransactionWrapper

    async def create_connection(self, with_db: bool) -> None:
        self._template = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database if with_db else None,
            "min_size": self.min_size,
            "max_size": self.max_size,
            **self.extra,
        }
        if not self._pool:
            try:
                self._pool = await asyncpg.create_pool(None, password=self.password, **self._template)
                if self.log:
                    self.log.debug(
                        "Created pool {pool} with params: {params}".format(
                            pool=self._pool, params=self._template
                        ))
            except asyncpg.InvalidCatalogNameError:
                raise DBConnectionError(
                    "Can't establish connection to database {}".format(self.database))
        # Set post-connection variables
        if self.schema:
            await self.execute_script("SET search_path TO {}".format(self.schema))

    async def _close(self) -> None:
        if self._pool:  # pragma: nobranch
            try:
                await asyncio.wait_for(self._pool.close(), timeout=10)
            except asyncio.TimeoutError:
                self._pool.terminate()
            if self.log:
                self.log.debug("Closed poll %s with params: %s", self._pool, self._template)
            self._template.clear()

    async def close(self) -> None:
        await self._close()
        self._pool = None

    async def db_create(self) -> None:
        await self.create_connection(with_db=False)
        await self.execute_script(
            'CREATE DATABASE "{}" OWNER "{}"'.format(self.database, self.user)
        )
        await self.close()

    async def db_delete(self) -> None:
        await self.create_connection(with_db=False)
        try:
            await self.execute_script('DROP DATABASE "{}"'.format(self.database))
        except asyncpg.InvalidCatalogNameError:  # pragma: nocoverage
            pass
        await self.close()

    def acquire_connection(self) -> "AsyncContextManager":
        return PoolConnectionDispatcher(self._pool, self.log)

    def _in_transaction(self) -> "TransactionWrapper":
        return self._transaction_class(None, self)

    @translate_exceptions
    async def execute_insert(self, query: str, values: list) -> Optional[asyncpg.Record]:
        async with self.acquire_connection() as connection:
            current_task = get_current_task()
            if self.log:
                self.log.debug("Task {task} executes query {query} with values {values} via {connection}".format(
                    task=id(current_task), query=query, values=values, connection=connection))
            # TODO: Cache prepared statement
            stmt = await connection.prepare(query)
            return await stmt.fetchrow(*values)

    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            current_task = get_current_task()
            if self.log:
                self.log.debug("Task {task} executes query {query} with values {values} via {connection}".format(
                    task=id(current_task), query=query, values=values, connection=connection))
            # TODO: Consider using copy_records_to_table instead
            await connection.executemany(query, values)

    @translate_exceptions
    async def execute_query(self, query: str) -> List[dict]:
        async with self.acquire_connection() as connection:
            current_task = get_current_task()
            if self.log:
                self.log.debug("Task {task} executes query {query} via {connection}".format(
                    task=id(current_task), query=query, connection=connection))
            return await connection.fetch(query)

    @translate_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            current_task = get_current_task()
            if self.log:
                self.log.debug("Task {task} executes query {query} via {connection}".format(
                    task=id(current_task), query=query, connection=connection))
            await connection.execute(query)


class TransactionWrapper(BaseTransactionWrapper, AsyncpgDBClient):
    def __init__(
        self, connection: asyncpg.Connection,
        parent: Union[AsyncpgDBClient, "TransactionWrapper"]
    ) -> None:
        self._pool = parent._pool  # type: asyncpg.pool.Pool
        self._current_transaction = parent._current_transaction
        if parent.log:
            self.log = logging.getLogger("db_client")
        else:
            self.log = None
        self._transaction_class = self.__class__
        self._old_context_value = None  # type: Optional[BaseDBAsyncClient]
        self.connection_name = parent.connection_name
        self.transaction = None
        self._finalized = False
        self._parent = parent
        self._connection = connection
        if isinstance(parent, TransactionWrapper):
            self._lock = parent._lock
        else:
            self._lock = asyncio.Lock()

        current_task = get_current_task()
        if self.log:
            self.log.debug('Task {task} initialized TransactionWrapper with connection {connection}, parent {parent}, pool {pool}'.format(
                task=id(current_task), connection=self._connection, parent=self._parent, pool=self._pool))

    async def create_connection(self, with_db: bool) -> None:
        await self._parent.create_connection(with_db)
        self._pool = self._parent._pool

    async def _close(self) -> None:
        await self._parent.close()
        self._pool = self._parent._pool

    def acquire_connection(self) -> "AsyncContextManager":
        return ConnectionWrapper(self._connection, self._lock)

    def _in_transaction(self) -> "TransactionWrapper":
        return self._transaction_class(self._connection, self)

    async def start(self):
        if not self._connection:
            self._connection = await self._pool.acquire()
        self.transaction = self._connection.transaction()
        await self.transaction.start()
        self._old_context_value = self._current_transaction.get()
        self._current_transaction.set(self)

        current_task = get_current_task()
        if self.log:
            self.log.debug('Task {task} started transaction with TransactionWrapper {tw}, connection {connection}'.format(
                task=id(current_task), tw=self, connection=self._connection))

    async def finalize(self) -> None:
        if not self._old_context_value:
            raise OperationalError("Finalize was called before transaction start")
        self._finalized = True
        self._current_transaction.set(self._old_context_value)
        await self._pool.release(self._connection)
        self._connection = None

        current_task = get_current_task()
        if self.log:
            self.log.debug('Task {task} finalized transaction with TransactionWrapper {tw}, connection {connection}'.format(
                task=id(current_task), tw=self, connection=self._connection))

    async def commit(self):
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await self.transaction.commit()

        current_task = get_current_task()
        if self.log:
            self.log.debug('Task {task} committed transaction with TransactionWrapper {tw}, connection {connection}'.format(
                task=id(current_task), tw=self, connection=self._connection))

        await self.finalize()

    async def rollback(self):
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await self.transaction.rollback()

        current_task = get_current_task()
        if self.log:
            self.log.debug('Task {task} rollbacked transaction with TransactionWrapper {tw}, connection {connection}'.format(
                task=id(current_task), tw=self, connection=self._connection))

        await self.finalize()
