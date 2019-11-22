"""
This example showcases postgres features
"""
from postmodel import Postmodel, fields, run_async
from postmodel.models import Model


class Report(Model):
    id = fields.IntField(pk=True)
    content = fields.JSONField()

    def __str__(self):
        return str(self.id)


async def run():
    await Postmodel.init(
        {
            "connections": {
                "default": {
                    "engine": "postmodel.asyncdb.asyncpg",
                    "credentials": {
                        "host": "localhost",
                        "port": "5432",
                        "user": "postmodel",
                        "password": "qwerty123",
                        "database": "test",
                    },
                }
            },
            "apps": {"models": {"models": ["__main__"], "default_connection": "default"}},
        },
        _create_db=True,
    )
    await Postmodel.generate_schemas()

    report_data = {"foo": "bar"}
    print(await Report.create(content=report_data))
    print(await Report.filter(content=report_data).first())
    await Postmodel._drop_databases()


if __name__ == "__main__":
    run_async(run())
