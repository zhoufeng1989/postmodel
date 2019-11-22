from tests import db_context
from tests.models import MockUser, AssetModel
import asyncio
import pytest

async def mock_create_user(**kwargs):
    return await MockUser.create(**kwargs)

@pytest.mark.asyncio
@db_context
async def test_dmls():
    mock_user_1 = await mock_create_user(
        test_int=32,
        test_txt_field='test text field.<>{:L*&^$#%@!',
    )

    mock_user_2 = await mock_create_user(
        test_int=33,
        test_txt_field='test text field.<>{:L*&^$#%@!',
    )

    mock_user_3 = await mock_create_user(
        test_int=34,
        test_txt_field='test text field.<>{:L*&^$#%@!',
    )

    await mock_user_1.save()
    await mock_user_2.save()
    mock_user_3.test_int = 77
    await mock_user_3.save()

    mock_user_1_copy = await MockUser.get(user_id=mock_user_1.user_id)
    assert mock_user_1_copy is not None

    mock_user_2_copy = await MockUser.get(user_id=mock_user_2.user_id)
    assert mock_user_2_copy is not None

    mock_user_3_copy = await MockUser.get(user_id=mock_user_3.user_id)
    assert mock_user_3_copy is not None
    assert mock_user_3_copy.test_int == 77


@pytest.mark.asyncio
@db_context
async def test_bulk_dmls():
    mock_user_1 = await mock_create_user(
        test_int=32,
        test_txt_field='dml test',
    )

    mock_user_2 = await mock_create_user(
        test_int=33,
        test_txt_field='dml test',
    )

    mock_user_3 = await mock_create_user(
        test_int=34,
        test_txt_field='dml test',
    )

    await MockUser.filter(test_txt_field='dml test').update(test_int=55)

    mock_users = await MockUser.filter(test_txt_field='dml test')

    for mock_user in mock_users:
        assert mock_user.test_int == 55
