from tests import db_context
from tests.models import MockUser, AssetModel
from postmodel.transactions import in_transaction

import pytest
import asyncio


@pytest.mark.asyncio
@db_context
async def test_concurrency_read():
    concurrency = 1000
    await MockUser.create()
    user1 = await MockUser.first()
    all_read = await asyncio.gather(*[MockUser.first() for _ in range(concurrency)])
    assert all_read == [user1 for _ in range(concurrency)]


@pytest.mark.asyncio
@db_context
async def test_concurrency_create():
    concurrency = 1000
    all_write = await asyncio.gather(*[MockUser.create() for _ in range(1000)])
    all_read = await MockUser.all()
    assert set(all_write) == set(all_read)


async def create_trans():
    async with in_transaction():
        user = await MockUser.create()
        await AssetModel.create(user_id=user.user_id)


@pytest.mark.asyncio
@db_context
async def test_concurrency_transaction_create():
    concurrency = 1000
    await asyncio.gather(*[create_trans() for _ in range(concurrency)])
    users = await MockUser.all()
    assets = await AssetModel.all()

    assert len(list(users)) == concurrency
    assert len(list(assets)) == concurrency


async def create_trans_concurrent(concurrency):
    async with in_transaction():
        await asyncio.gather(*[MockUser.create() for _ in range(concurrency)])


@pytest.mark.asyncio
@db_context
async def test_concurrency_transactions_concurrent():
    concurrency = 10
    concurrency_in_transaction = 100
    await asyncio.gather(*[create_trans_concurrent(concurrency=concurrency_in_transaction) for _ in range(concurrency)])
    users = await MockUser.all()
    assert len(list(users)) == concurrency * concurrency_in_transaction
