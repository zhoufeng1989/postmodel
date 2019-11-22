from tests import db_context
from tests.models import MockUser, AssetModel
import asyncio
import pytest

async def mock_create_user(**kwargs):
    return await MockUser.create(**kwargs)


@pytest.mark.asyncio
@db_context
async def test_simple_querys():
    mock_user_1 = await mock_create_user(
        test_int=32,
        test_txt_field='test text field.<>{:L*&^$#%@!',
    )

    mock_user_2 = await MockUser.get(user_id=mock_user_1.user_id)

    assert mock_user_2 is not None

    assert mock_user_2.user_id == mock_user_1.user_id
    assert mock_user_2.test_int == mock_user_1.test_int
    assert mock_user_2.test_txt_field == mock_user_1.test_txt_field

    mock_user_3_qset = await MockUser.filter(user_id=mock_user_2.user_id)
    mock_user_3_lst = list(mock_user_3_qset)

    assert len(mock_user_3_lst) == 1

    mock_user_3 = mock_user_3_lst[0]

    assert mock_user_3.user_id == mock_user_2.user_id
    assert mock_user_3.test_int == mock_user_2.test_int
    assert mock_user_3.test_txt_field == mock_user_2.test_txt_field

    mock_user_4 = await MockUser.filter(user_id=mock_user_3.user_id).first()

    assert mock_user_4 is not None

    assert mock_user_4.user_id == mock_user_3.user_id
    assert mock_user_4.test_int == mock_user_3.test_int
    assert mock_user_4.test_txt_field == mock_user_3.test_txt_field

    mock_user_5_qset = await MockUser.all()
    mock_user_5_lst = list(mock_user_5_qset)

    assert len(mock_user_5_lst) == 1

    mock_user_5 = mock_user_5_lst[0]

    assert mock_user_5.user_id == mock_user_4.user_id
    assert mock_user_5.test_int == mock_user_4.test_int
    assert mock_user_5.test_txt_field == mock_user_4.test_txt_field


@pytest.mark.asyncio
@db_context
async def test_complecated_querys():
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

    mock_users_1 = await MockUser.all().order_by('user_id')

    mock_user_ints = [mock_user.test_int for mock_user in mock_users_1]

    assert mock_user_ints == sorted(mock_user_ints)

    mock_users_2 = await MockUser.all().distinct().order_by('test_int')

    mock_user_ints = [mock_user.test_int for mock_user in mock_users_2]

    assert mock_user_ints == list(set(mock_user_ints))

    mock_user_4 = await mock_create_user(
        test_int=35,
        test_txt_field='test text field.<>{:L*&^$#%@!',
    )

    mock_users_3 = await MockUser.all().distinct().order_by('test_int')

    assert [mock_user.test_int for mock_user in mock_users_3] == [32, 33, 34, 35]
