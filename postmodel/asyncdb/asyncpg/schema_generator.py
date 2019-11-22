from typing import List

from postmodel import fields
from postmodel.asyncdb.base.schema_generator import BaseSchemaGenerator
from postmodel.utils import get_escape_translation_table


class AsyncpgSchemaGenerator(BaseSchemaGenerator):
    TABLE_COMMENT_TEMPLATE = "COMMENT ON TABLE {table} IS '{comment}';"
    COLUMN_COMMNET_TEMPLATE = "COMMENT ON COLUMN {table}.{column} IS '{comment}';"

    FIELD_TYPE_MAP = {
        **BaseSchemaGenerator.FIELD_TYPE_MAP,
        fields.JSONField: "JSONB",
        fields.UUIDField: "UUID",
    }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.comments_array = []  # type: List[str]

    def _get_primary_key_create_string(self, field_name: str, comment: str) -> str:
        return '"{}" SERIAL NOT NULL PRIMARY KEY'.format(field_name)

    def _escape_comment(self, comment: str) -> str:
        table = get_escape_translation_table()
        table[ord("'")] = "''"
        return comment.translate(table)

    def _table_comment_generator(self, table: str, comment: str) -> str:
        comment = self.TABLE_COMMENT_TEMPLATE.format(
            table=table, comment=self._escape_comment(comment)
        )
        self.comments_array.append(comment)
        return ""

    def _column_comment_generator(self, table: str, column: str, comment: str) -> str:
        comment = self.COLUMN_COMMNET_TEMPLATE.format(
            table=table, column=column, comment=self._escape_comment(comment)
        )
        if comment not in self.comments_array:
            self.comments_array.append(comment)
        return ""

    def _post_table_hook(self) -> str:
        val = "\n".join(self.comments_array)
        self.comments_array = []
        if val:
            return "\n" + val
        return ""
