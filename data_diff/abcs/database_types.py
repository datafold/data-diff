import decimal
from abc import ABC, abstractmethod
from typing import Sequence, Optional, Tuple, Union, Dict, List
from datetime import datetime

from runtype import dataclass
from typing_extensions import Self

from data_diff.abcs.compiler import AbstractCompiler
from data_diff.utils import ArithAlphanumeric, ArithUUID, Unknown


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
