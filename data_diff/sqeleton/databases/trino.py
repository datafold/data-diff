from .database_types import TemporalType, ColType_UUID
from .presto import Presto, Dialect
from .base import import_helper
from .base import TIMESTAMP_PRECISION_POS


@import_helper("trino")
def import_trino():
    import trino

    return trino


class Dialect(Dialect):
    name = "Trino"

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


class Trino(Presto):
    dialect = Dialect()
    CONNECT_URI_HELP = "trino://<user>@<host>/<catalog>/<schema>"
    CONNECT_URI_PARAMS = ["catalog", "schema"]

    def __init__(self, **kw):
        trino = import_trino()

        self._conn = trino.dbapi.connect(**kw)
