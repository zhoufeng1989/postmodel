from tests import db_context
from tests.models import MockUser, AssetModel
import asyncio
import pytest

async def mock_create_user():
    return await MockUser.create()

@pytest.mark.asyncio
@db_context
async def test_basic_types():
    mock_user_1 = await mock_create_user()

    mock_user_2 = await MockUser.get(user_id=mock_user_1.user_id)
    assert mock_user_2 is not None

    mock_user_2.test_int = 2
    await mock_user_2.save()
    print('mock_user_2', mock_user_2.test_datetime_auto_now)
    await asyncio.sleep(1)

    mock_user_3 = await MockUser.get(user_id=mock_user_2.user_id)
    mock_user_3.test_id = 3
    await mock_user_3.save()
    print('mock_user_3', mock_user_3.test_datetime_auto_now)

    assert mock_user_2.test_datetime_auto_now_add == mock_user_3.test_datetime_auto_now_add
    assert mock_user_2.test_datetime_auto_now != mock_user_3.test_datetime_auto_now

    mock_user_3.test_txt_field = 'test text field.<>{:L*&^$#%@!'
    await mock_user_3.save()

    mock_user_4 = await MockUser.get(user_id=mock_user_3.user_id)
    assert mock_user_4.test_txt_field == mock_user_3.test_txt_field

