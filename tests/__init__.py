import uuid
import asyncpg
from functools import wraps
from postmodel import Postmodel, fields
from postmodel.models import Model
import random
import asyncio

base_url = 'postgres://postgres:@localhost/'
url = 'postgres://postgres:@localhost/'

async def init_db_postmodel(url):
    await Postmodel.init(
        db_url=url,
        modules={'models': ['tests.models']}
    )
    await Postmodel.generate_schemas()

async def create_database(url, db_name):
    connection = await asyncpg.connect(base_url)
    try:
        await connection.execute('create database {db_name}'.format(db_name=db_name))
    except:
        pass
    finally:
        await connection.close()


def db_context(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        db_name = 'test_db_' + hex(random.getrandbits(64))
        await create_database(base_url, db_name)
        db_url = base_url + db_name
        await init_db_postmodel(db_url)
        res = await fn(*args, **kwargs)
        await Postmodel._drop_databases()
        return res

    return wrapper
