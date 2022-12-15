from abc import ABC, abstractmethod
from .database_types import TemporalType, FractionalType, ColType_UUID, Boolean, ColType, String_UUID
from .compiler import Compilable


class AbstractMixin_NormalizeValue(ABC):
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
        return self.to_string(value)


class AbstractMixin_MD5(ABC):
    """Methods for calculating an MD6 hash as an integer."""

    @abstractmethod
    def md5_as_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"


class AbstractMixin_Schema(ABC):
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
