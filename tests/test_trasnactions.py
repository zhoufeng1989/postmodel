from tests import db_context
from tests.models import MockUser, AssetModel
from postmodel.transactions import in_transaction
from postmodel.query_utils import Q

import pytest

import asyncio


@pytest.mark.asyncio
@db_context
async def test_transaction_success():
    test_ints = [201, 202, 203]
    async with in_transaction() as connection:
        for test_int in test_ints:
            user = await MockUser.create(test_int=test_int)
            await AssetModel.create(user_id=user.user_id)
    mock_users = await MockUser.filter(Q(test_int__in=test_ints))
    assets = await AssetModel.all()
    assert len(list(mock_users)) == 3
    assert len(list(assets)) == 3


@pytest.mark.asyncio
@db_context
async def test_transaction_fail():
    try:
        async with in_transaction() as conn:
            user = await MockUser.create(test_int=301)
            await AssetModel.create(user_id=user.user_id)
            user = await MockUser.create(test_int=333)
            await AssetModel.create(user_id=user.user_id)
            raise Exception("Opps.")
            user = await MockUser.create(test_int=355)
            await AssetModel.create(user_id=user.user_id)

    except:
        pass

    mock_users = await MockUser.filter(Q(test_int__in=[301, 333, 355]))
    assets = await AssetModel.all()
    assert len(list(mock_users)) == 0
    assert len(list(assets)) == 0
