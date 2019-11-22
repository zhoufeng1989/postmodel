from postmodel import fields
from postmodel.models import Model
import uuid


class MockUser(Model):
    user_id = fields.TextField(pk=True)
    test_int = fields.IntField(null=True)
    test_txt_field = fields.TextField(null=True)
    test_datetime_auto_now = fields.DatetimeField(auto_now=True, null=True)
    test_datetime_auto_now_add = fields.DatetimeField(auto_now_add=True, null=True)

    @classmethod
    async def create(cls, **kwargs):
        kwargs.update({'user_id': str(uuid.uuid1())})
        return await super(MockUser, cls).create(**kwargs)

    class Meta:
        table = 'mock_user'


class AssetModel(Model):
    asset_id = fields.TextField(pk=True)
    user_id = fields.TextField(null=True)
    asset_name = fields.TextField(null=True)
    quantity = fields.IntField(null=True)

    class Meta:
        table = 'asset'

    @classmethod
    async def create(cls, **kwargs):
        kwargs.update({'asset_id': str(uuid.uuid1())})
        return await super(AssetModel, cls).create(**kwargs)
