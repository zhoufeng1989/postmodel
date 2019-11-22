from functools import wraps
from typing import Callable, Dict, Optional  # noqa

from postmodel.asyncdb.base.client import BaseDBAsyncClient, BaseTransactionWrapper
from postmodel.exceptions import ParamsError

current_transaction_map = {}  # type: Dict


def _get_connection(connection_name: Optional[str]) -> BaseDBAsyncClient:
    from postmodel import Postmodel

    if connection_name:
        connection = Postmodel.get_connection(connection_name).get_current_transaction()
    elif len(Postmodel._connections) == 1:
        connection = list(Postmodel._connections.values())[0].get_current_transaction()
    else:
        raise ParamsError(
            "You are running with multiple databases, so you "
            "should specify connection_name: {}".format(list(Postmodel._connections.keys()))
        )
    return connection


def in_transaction(connection_name: Optional[str] = None) -> BaseTransactionWrapper:
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """
    connection = _get_connection(connection_name)
    return connection._in_transaction()


def atomic(connection_name: Optional[str] = None) -> Callable:
    """
    Transaction decorator.

    You can wrap your function with this decorator to run it into one transaction.
    If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """

    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            connection = _get_connection(connection_name)
            async with connection._in_transaction():
                return await func(*args, **kwargs)

        return wrapped

    return wrapper


async def start_transaction(connection_name: Optional[str] = None) -> BaseTransactionWrapper:
    """
    Function to manually control your transaction.

    Returns transaction object with ``.rollback()`` and ``.commit()`` methods.
    All db calls in same coroutine context will run into transaction
    before ending transaction with above methods.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """
    connection = _get_connection(connection_name)
    transaction = connection._in_transaction()
    await transaction.start()
    return transaction
