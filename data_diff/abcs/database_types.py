import decimal
from abc import ABC, abstractmethod
from typing import Tuple, Union
from datetime import datetime

from runtype import dataclass

from data_diff.utils import ArithAlphanumeric, ArithUUID, Unknown


DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes, ArithUUID, ArithAlphanumeric]
DbTime = datetime


@dataclass
class ColType:
    @property
    def supported(self) -> bool:
        return True


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
    @property
    def supported(self) -> bool:
        return False


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

    @property
    def supported(self) -> bool:
        return False
