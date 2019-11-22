from postmodel.utils import set_logger
set_logger('db_client')

from .client import AsyncpgDBClient
client_class = AsyncpgDBClient
