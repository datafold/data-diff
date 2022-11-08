import logging
import decimal
from abc import ABC, abstractmethod
from typing import Sequence, Optional, Tuple, Union, Dict, List
from datetime import datetime

from runtype import dataclass

from data_diff.utils import ArithAlphanumeric, ArithUUID, CaseAwareMapping, CaseInsensitiveDict, CaseSensitiveDict


DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes, ArithUUID, ArithAlphanumeric]
DbTime = datetime

logger = logging.getLogger("databases")


class ColType:
    supported = True


@dataclass
class PrecisionType(ColType):
    precision: int
    rounds: bool


class Boolean(ColType):
    supported = True

class TemporalType(PrecisionType):
    pass


class Timestamp(TemporalType):
    pass


class TimestampTZ(TemporalType):
    pass


class Datetime(TemporalType):
    pass


@dataclass
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


class FractionalType(NumericType):
    pass


class Float(FractionalType):
    pass


class IKey(ABC):
    "Interface for ColType, for using a column as a key in data-diff"
    python_type: type

    def make_value(self, value):
        return self.python_type(value)


class Decimal(FractionalType, IKey):  # Snowflake may use Decimal as a key
    @property
    def python_type(self) -> type:
        if self.precision == 0:
            return int
        return decimal.Decimal


class StringType(ColType):
    pass


class ColType_UUID(ColType, IKey):
    python_type = ArithUUID


class ColType_Alphanum(ColType, IKey):
    python_type = ArithAlphanumeric


class Native_UUID(ColType_UUID):
    pass


class String_UUID(StringType, ColType_UUID):
    pass


class String_Alphanum(StringType, ColType_Alphanum):
    @staticmethod
    def test_value(value: str) -> bool:
        try:
            ArithAlphanumeric(value)
            return True
        except ValueError:
            return False

    def make_value(self, value):
        return self.python_type(value)


class String_VaryingAlphanum(String_Alphanum):
    pass


@dataclass
class String_FixedAlphanum(String_Alphanum):
    length: int

    def make_value(self, value):
        if len(value) != self.length:
            raise ValueError(f"Expected alphanumeric value of length {self.length}, but got '{value}'.")
        return self.python_type(value, max_len=self.length)


@dataclass
class Text(StringType):
    supported = False


@dataclass
class Integer(NumericType, IKey):
    precision: int = 0
    python_type: type = int

    def __post_init__(self):
        assert self.precision == 0


@dataclass
class UnknownColType(ColType):
    text: str

    supported = False


class AbstractDialect(ABC):
    """Dialect-dependent query expressions"""

    name: str

    @property
    @abstractmethod
    def name(self) -> str:
        "Name of the dialect"

    @property
    @abstractmethod
    def ROUNDS_ON_PREC_LOSS(self) -> bool:
        "True if db rounds real values when losing precision, False if it truncates."

    @abstractmethod
    def quote(self, s: str):
        "Quote SQL name"
        ...

    @abstractmethod
    def concat(self, items: List[str]) -> str:
        "Provide SQL for concatenating a bunch of columns into a string"
        ...

    @abstractmethod
    def is_distinct_from(self, a: str, b: str) -> str:
        "Provide SQL for a comparison where NULL = NULL is true"
        ...

    @abstractmethod
    def to_string(self, s: str) -> str:
        # TODO rewrite using cast_to(x, str)
        "Provide SQL for casting a column to string"
        ...

    @abstractmethod
    def random(self) -> str:
        "Provide SQL for generating a random number betweein 0..1"

    @abstractmethod
    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        "Provide SQL fragment for limit and offset inside a select"
        ...

    @abstractmethod
    def explain_as_text(self, query: str) -> str:
        "Provide SQL for explaining a query, returned as table(varchar)"
        ...

    @abstractmethod
    def timestamp_value(self, t: datetime) -> str:
        "Provide SQL for the given timestamp value"
        ...

    @abstractmethod
    def parse_type(
        self,
        table_path: DbPath,
        col_name: str,
        type_repr: str,
        datetime_precision: int = None,
        numeric_precision: int = None,
        numeric_scale: int = None,
    ) -> ColType:
        "Parse type info as returned by the database"


class AbstractMixin_NormalizeValue(ABC):
    @abstractmethod
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized timestamp.

        The returned expression must accept any SQL datetime/timestamp, and return a string.

        Date format: ``YYYY-MM-DD HH:mm:SS.FFFFFF``

        Precision of dates should be rounded up/down according to coltype.rounds
        """
        ...

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
        ...

    @abstractmethod
    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized uuid.

        i.e. just makes sure there is no trailing whitespace.
        """
        ...

    def normalize_boolean(self, value: str, coltype: Boolean) -> str:
        """Creates an SQL expression, that converts 'value' to either '0' or '1'.
        """
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
    """Dialect-dependent query expressions, that are specific to data-diff"""

    @abstractmethod
    def md5_as_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"
        ...


class AbstractDatabase:
    @abstractmethod
    def _query(self, sql_code: str) -> list:
        "Send query to database and return result"
        ...

    @abstractmethod
    def select_table_schema(self, path: DbPath) -> str:
        "Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"
        ...

    @abstractmethod
    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        """Query the table for its schema for table in 'path', and return {column: tuple}
        where the tuple is (table_name, col_name, type_repr, datetime_precision?, numeric_precision?, numeric_scale?)
        """
        ...

    @abstractmethod
    def select_table_unique_columns(self, path: DbPath) -> str:
        "Provide SQL for selecting the names of unique columns in the table"
        ...

    @abstractmethod
    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        """Query the table for its unique columns for table in 'path', and return {column}"""
        ...

    @abstractmethod
    def _process_table_schema(
        self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str], where: str = None
    ):
        """Process the result of query_table_schema().

        Done in a separate step, to minimize the amount of processed columns.
        Needed because processing each column may:
        * throw errors and warnings
        * query the database to sample values

        """

    @abstractmethod
    def parse_table_name(self, name: str) -> DbPath:
        "Parse the given table name into a DbPath"
        ...

    @abstractmethod
    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."
        ...

    @abstractmethod
    def _normalize_table_path(self, path: DbPath) -> DbPath:
        ...

    @property
    @abstractmethod
    def is_autocommit(self) -> bool:
        ...


Schema = CaseAwareMapping


def create_schema(db: AbstractDatabase, table_path: DbPath, schema: dict, case_sensitive: bool) -> CaseAwareMapping:
    logger.debug(f"[{db.name}] Schema = {schema}")

    if case_sensitive:
        return CaseSensitiveDict(schema)

    if len({k.lower() for k in schema}) < len(schema):
        logger.warning(f'Ambiguous schema for {db}:{".".join(table_path)} | Columns = {", ".join(list(schema))}')
        logger.warning("We recommend to disable case-insensitivity (set --case-sensitive).")
    return CaseInsensitiveDict(schema)
