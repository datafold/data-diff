from abc import ABC, abstractmethod

import attrs

from data_diff.abcs.database_types import (
    Array,
    TemporalType,
    FractionalType,
    ColType_UUID,
    Boolean,
    ColType,
    String_UUID,
    JSON,
    Struct,
)
from data_diff.abcs.compiler import Compilable


@attrs.define(frozen=False)
class AbstractMixin(ABC):
    "A mixin for a database dialect"


@attrs.define(frozen=False)
class AbstractMixin_NormalizeValue(AbstractMixin):
    @abstractmethod
    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""

    @abstractmethod
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized timestamp.

        The returned expression must accept any SQL datetime/timestamp, and return a string.

        Date format: ``YYYY-MM-DD HH:mm:SS.FFFFFF``

        Precision of dates should be rounded up/down according to coltype.rounds
        """

    @abstractmethod
    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized number.

        The returned expression must accept any SQL int/numeric/float, and return a string.

        Floats/Decimals are expected in the format
        "I.P"

        Where I is the integer part of the number (as many digits as necessary),
        and must be at least one digit (0).
        P is the fractional digits, the amount of which is specified with
        coltype.precision. Trailing zeroes may be necessary.
        If P is 0, the dot is omitted.

        Note: We use 'precision' differently than most databases. For decimals,
        it's the same as ``numeric_scale``, and for floats, who use binary precision,
        it can be calculated as ``log10(2**numeric_precision)``.
        """

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        """Creates an SQL expression, that converts 'value' to either '0' or '1'."""
        return self.to_string(value)

    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        """Creates an SQL expression, that strips uuids of artifacts like whitespace."""
        if isinstance(coltype, String_UUID):
            return f"TRIM({value})"
        return self.to_string(value)

    def normalize_json(self, value: str, _coltype: JSON) -> str:
        """Creates an SQL expression, that converts 'value' to its minified json string representation."""
        return self.to_string(value)

    def normalize_array(self, value: str, _coltype: Array) -> str:
        """Creates an SQL expression, that serialized an array into a JSON string."""
        return self.to_string(value)

    def normalize_struct(self, value: str, _coltype: Struct) -> str:
        """Creates an SQL expression, that serialized a typed struct into a JSON string."""
        return self.to_string(value)

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized representation.

        The returned expression must accept any SQL value, and return a string.

        The default implementation dispatches to a method according to `coltype`:

        ::

            TemporalType    -> normalize_timestamp()
            FractionalType  -> normalize_number()
            *else*          -> to_string()

            (`Integer` falls in the *else* category)

        """
        if isinstance(coltype, TemporalType):
            return self.normalize_timestamp(value, coltype)
        elif isinstance(coltype, FractionalType):
            return self.normalize_number(value, coltype)
        elif isinstance(coltype, ColType_UUID):
            return self.normalize_uuid(value, coltype)
        elif isinstance(coltype, Boolean):
            return self.normalize_boolean(value, coltype)
        elif isinstance(coltype, JSON):
            return self.normalize_json(value, coltype)
        elif isinstance(coltype, Array):
            return self.normalize_array(value, coltype)
        elif isinstance(coltype, Struct):
            return self.normalize_struct(value, coltype)
        return self.to_string(value)


@attrs.define(frozen=False)
class AbstractMixin_MD5(AbstractMixin):
    """Methods for calculating an MD6 hash as an integer."""

    @abstractmethod
    def md5_as_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"


@attrs.define(frozen=False)
class AbstractMixin_Schema(AbstractMixin):
    """Methods for querying the database schema

    TODO: Move AbstractDatabase.query_table_schema() and friends over here
    """

    def table_information(self) -> Compilable:
        "Query to return a table of schema information about existing tables"
        raise NotImplementedError()

    @abstractmethod
    def list_tables(self, table_schema: str, like: Compilable = None) -> Compilable:
        """Query to select the list of tables in the schema. (query return type: table[str])

        If 'like' is specified, the value is applied to the table name, using the 'like' operator.
        """


@attrs.define(frozen=False)
class AbstractMixin_RandomSample(AbstractMixin):
    @abstractmethod
    def random_sample_n(self, tbl: str, size: int) -> str:
        """Take a random sample of the given size, i.e. return 'size' amount of rows"""

    @abstractmethod
    def random_sample_ratio_approx(self, tbl: str, ratio: float) -> str:
        """Take a random sample of the approximate size determined by the ratio (0..1), where 0 means no rows, and 1 means all rows

        i.e. the actual mount of rows returned may vary by standard deviation.
        """

    # def random_sample_ratio(self, table: ITable, ratio: float):
    #     """Take a random sample of the size determined by the ratio (0..1), where 0 means no rows, and 1 means all rows
    #     """


@attrs.define(frozen=False)
class AbstractMixin_TimeTravel(AbstractMixin):
    @abstractmethod
    def time_travel(
        self,
        table: Compilable,
        before: bool = False,
        timestamp: Compilable = None,
        offset: Compilable = None,
        statement: Compilable = None,
    ) -> Compilable:
        """Selects historical data from a table

        Parameters:
            table - The name of the table whose history we're querying
            timestamp - A constant timestamp
            offset - the time 'offset' seconds before now
            statement - identifier for statement, e.g. query ID

        Must specify exactly one of `timestamp`, `offset` or `statement`.
        """


@attrs.define(frozen=False)
class AbstractMixin_OptimizerHints(AbstractMixin):
    @abstractmethod
    def optimizer_hints(self, optimizer_hints: str) -> str:
        """Creates a compatible optimizer_hints string

        Parameters:
            optimizer_hints - string of optimizer hints
        """
