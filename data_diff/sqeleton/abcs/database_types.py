import decimal
from abc import ABC, abstractmethod
from typing import Sequence, Optional, Tuple, Union, Dict, List
from datetime import datetime

from runtype import dataclass

from ..utils import ArithAlphanumeric, ArithUUID, Self, Unknown


DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes, ArithUUID, ArithAlphanumeric]
DbTime = datetime


@dataclass
class ColType:
    supported = True


@dataclass
class PrecisionType(ColType):
    precision: int
    rounds: Union[bool, Unknown] = Unknown


class Boolean(ColType):
    precision = 0


class TemporalType(PrecisionType):
    pass


class Timestamp(TemporalType):
    pass


class TimestampTZ(TemporalType):
    pass


class Datetime(TemporalType):
    pass


class Date(TemporalType):
    pass


@dataclass
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


class FractionalType(NumericType):
    pass


class Float(FractionalType):
    python_type = float


class IKey(ABC):
    "Interface for ColType, for using a column as a key in table."

    @property
    @abstractmethod
    def python_type(self) -> type:
        "Return the equivalent Python type of the key"

    def make_value(self, value):
        return self.python_type(value)


class Decimal(FractionalType, IKey):  # Snowflake may use Decimal as a key
    @property
    def python_type(self) -> type:
        if self.precision == 0:
            return int
        return decimal.Decimal


@dataclass
class StringType(ColType):
    python_type = str


class ColType_UUID(ColType, IKey):
    python_type = ArithUUID


class ColType_Alphanum(ColType, IKey):
    python_type = ArithAlphanumeric


class Native_UUID(ColType_UUID):
    pass


class String_UUID(ColType_UUID, StringType):
    pass


class String_Alphanum(ColType_Alphanum, StringType):
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


# In majority of DBMSes, it is called JSON/JSONB. Only in Snowflake, it is OBJECT.
@dataclass
class JSON(ColType):
    pass


@dataclass
class Array(ColType):
    item_type: ColType


# Unlike JSON, structs are not free-form and have a very specific set of fields and their types.
# We do not parse & use those fields now, but we can do this later.
# For example, in BigQuery:
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#struct_literals
@dataclass
class Struct(ColType):
    pass


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

    @property
    @abstractmethod
    def name(self) -> str:
        "Name of the dialect"

    @classmethod
    @abstractmethod
    def load_mixins(cls, *abstract_mixins) -> Self:
        "Load a list of mixins that implement the given abstract mixins"

    @property
    @abstractmethod
    def ROUNDS_ON_PREC_LOSS(self) -> bool:
        "True if db rounds real values when losing precision, False if it truncates."

    @abstractmethod
    def quote(self, s: str):
        "Quote SQL name"

    @abstractmethod
    def concat(self, items: List[str]) -> str:
        "Provide SQL for concatenating a bunch of columns into a string"

    @abstractmethod
    def is_distinct_from(self, a: str, b: str) -> str:
        "Provide SQL for a comparison where NULL = NULL is true"

    @abstractmethod
    def to_string(self, s: str) -> str:
        # TODO rewrite using cast_to(x, str)
        "Provide SQL for casting a column to string"

    @abstractmethod
    def random(self) -> str:
        "Provide SQL for generating a random number betweein 0..1"

    @abstractmethod
    def current_timestamp(self) -> str:
        "Provide SQL for returning the current timestamp, aka now"

    @abstractmethod
    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        "Provide SQL fragment for limit and offset inside a select"

    @abstractmethod
    def explain_as_text(self, query: str) -> str:
        "Provide SQL for explaining a query, returned as table(varchar)"

    @abstractmethod
    def timestamp_value(self, t: datetime) -> str:
        "Provide SQL for the given timestamp value"

    @abstractmethod
    def set_timezone_to_utc(self) -> str:
        "Provide SQL for setting the session timezone to UTC"

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

    @abstractmethod
    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""


from typing import TypeVar, Generic

T_Dialect = TypeVar("T_Dialect", bound=AbstractDialect)


class AbstractDatabase(Generic[T_Dialect]):
    @property
    @abstractmethod
    def dialect(self) -> T_Dialect:
        "The dialect of the database. Used internally by Database, and also available publicly."

    @classmethod
    @abstractmethod
    def load_mixins(cls, *abstract_mixins) -> type:
        "Extend the dialect with a list of mixins that implement the given abstract mixins."

    @property
    @abstractmethod
    def CONNECT_URI_HELP(self) -> str:
        "Example URI to show the user in help and error messages"

    @property
    @abstractmethod
    def CONNECT_URI_PARAMS(self) -> List[str]:
        "List of parameters given in the path of the URI"

    @abstractmethod
    def _query(self, sql_code: str) -> list:
        "Send query to database and return result"

    @abstractmethod
    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:
        """Query the table for its schema for table in 'path', and return {column: tuple}
        where the tuple is (table_name, col_name, type_repr, datetime_precision?, numeric_precision?, numeric_scale?)

        Note: This method exists instead of select_table_schema(), just because not all databases support
              accessing the schema using a SQL query.
        """

    @abstractmethod
    def select_table_unique_columns(self, path: DbPath) -> str:
        "Provide SQL for selecting the names of unique columns in the table"

    @abstractmethod
    def query_table_unique_columns(self, path: DbPath) -> List[str]:
        """Query the table for its unique columns for table in 'path', and return {column}"""

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

    @abstractmethod
    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."

    @property
    @abstractmethod
    def is_autocommit(self) -> bool:
        "Return whether the database autocommits changes. When false, COMMIT statements are skipped."


class AbstractTable(ABC):
    @abstractmethod
    def select(self, *exprs, distinct=False, **named_exprs) -> "AbstractTable":
        """Choose new columns, based on the old ones. (aka Projection)

        Parameters:
            exprs: List of expressions to constitute the columns of the new table.
                    If not provided, returns all columns in source table (i.e. ``select *``)
            distinct: 'select' or 'select distinct'
            named_exprs: More expressions to constitute the columns of the new table, aliased to keyword name.

        """
        # XXX distinct=SKIP

    @abstractmethod
    def where(self, *exprs) -> "AbstractTable":
        """Filter the rows, based on the given predicates. (aka Selection)"""

    @abstractmethod
    def order_by(self, *exprs) -> "AbstractTable":
        """Order the rows lexicographically, according to the given expressions."""

    @abstractmethod
    def limit(self, limit: int) -> "AbstractTable":
        """Stop yielding rows after the given limit. i.e. take the first 'n=limit' rows"""

    @abstractmethod
    def join(self, target) -> "AbstractTable":
        """Join the current table with the target table, returning a new table containing both side-by-side.

        When joining, it's recommended to use explicit tables names, instead of `this`, in order to avoid potential name collisions.

        Example:
            ::

                person = table('person')
                city = table('city')

                name_and_city = (
                    person
                    .join(city)
                    .on(person['city_id'] == city['id'])
                    .select(person['id'], city['name'])
                )
        """

    @abstractmethod
    def group_by(self, *keys):
        """Behaves like in SQL, except for a small change in syntax:

        A call to `.agg()` must follow every call to `.group_by()`.

        Example:
            ::

                # SELECT a, sum(b) FROM tmp GROUP BY 1
                table('tmp').group_by(this.a).agg(this.b.sum())

                # SELECT a, sum(b) FROM a GROUP BY 1 HAVING (b > 10)
                (table('tmp')
                    .group_by(this.a)
                    .agg(this.b.sum())
                    .having(this.b > 10)
                )

        """

    @abstractmethod
    def count(self) -> int:
        """SELECT count() FROM self"""

    @abstractmethod
    def union(self, other: "ITable"):
        """SELECT * FROM self UNION other"""

    @abstractmethod
    def union_all(self, other: "ITable"):
        """SELECT * FROM self UNION ALL other"""

    @abstractmethod
    def minus(self, other: "ITable"):
        """SELECT * FROM self EXCEPT other"""

    @abstractmethod
    def intersect(self, other: "ITable"):
        """SELECT * FROM self INTERSECT other"""
