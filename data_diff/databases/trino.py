from typing import Any

import attrs

from data_diff.abcs.mixins import AbstractMixin_MD5, AbstractMixin_NormalizeValue
from data_diff.abcs.database_types import TemporalType, ColType_UUID
from data_diff.databases import presto
from data_diff.databases.base import import_helper
from data_diff.databases.base import TIMESTAMP_PRECISION_POS


@import_helper("trino")
def import_trino():
    import trino

    return trino


Mixin_MD5 = presto.Mixin_MD5


@attrs.define(frozen=False)
class Mixin_NormalizeValue(presto.Mixin_NormalizeValue):
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        if coltype.rounds:
            s = f"date_format(cast({value} as timestamp({coltype.precision})), '%Y-%m-%d %H:%i:%S.%f')"
        else:
            s = f"date_format(cast({value} as timestamp(6)), '%Y-%m-%d %H:%i:%S.%f')"

        return (
            f"RPAD(RPAD({s}, {TIMESTAMP_PRECISION_POS + coltype.precision}, '.'), {TIMESTAMP_PRECISION_POS + 6}, '0')"
        )

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        return f"TRIM({value})"


class Dialect(presto.Dialect, Mixin_MD5, Mixin_NormalizeValue, AbstractMixin_MD5, AbstractMixin_NormalizeValue):
    name = "Trino"


@attrs.define(frozen=False, init=False, kw_only=True)
class Trino(presto.Presto):
    dialect = Dialect()
    CONNECT_URI_HELP = "trino://<user>@<host>/<catalog>/<schema>"
    CONNECT_URI_PARAMS = ["catalog", "schema"]

    _conn: Any

    def __init__(self, **kw):
        super().__init__()
        trino = import_trino()

        if kw.get("schema"):
            self.default_schema = kw.get("schema")

        self._conn = trino.dbapi.connect(**kw)
