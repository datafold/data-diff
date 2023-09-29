import decimal
from abc import ABC, abstractmethod
from typing import Tuple, Union
from datetime import datetime

import attrs

from data_diff.utils import ArithAlphanumeric, ArithUUID, Unknown


DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes, ArithUUID, ArithAlphanumeric]
DbTime = datetime


@attrs.define(frozen=True)
class ColType:
    @property
    def supported(self) -> bool:
        return True


@attrs.define(frozen=True)
class PrecisionType(ColType):
    precision: int
    rounds: Union[bool, Unknown] = Unknown


@attrs.define(frozen=True)
class Boolean(ColType):
    precision = 0


@attrs.define(frozen=True)
class TemporalType(PrecisionType):
    pass


@attrs.define(frozen=True)
class Timestamp(TemporalType):
    pass


@attrs.define(frozen=True)
class TimestampTZ(TemporalType):
    pass


@attrs.define(frozen=True)
class Datetime(TemporalType):
    pass


@attrs.define(frozen=True)
class Date(TemporalType):
    pass


@attrs.define(frozen=True)
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


@attrs.define(frozen=True)
class FractionalType(NumericType):
    pass


@attrs.define(frozen=True)
class Float(FractionalType):
    python_type = float


@attrs.define(frozen=True)
class IKey(ABC):
    "Interface for ColType, for using a column as a key in table."

    @property
    @abstractmethod
    def python_type(self) -> type:
        "Return the equivalent Python type of the key"

    def make_value(self, value):
        return self.python_type(value)


@attrs.define(frozen=True)
class Decimal(FractionalType, IKey):  # Snowflake may use Decimal as a key
    @property
    def python_type(self) -> type:
        if self.precision == 0:
            return int
        return decimal.Decimal


@attrs.define(frozen=True)
class StringType(ColType):
    python_type = str


@attrs.define(frozen=True)
class ColType_UUID(ColType, IKey):
    python_type = ArithUUID


@attrs.define(frozen=True)
class ColType_Alphanum(ColType, IKey):
    python_type = ArithAlphanumeric


@attrs.define(frozen=True)
class Native_UUID(ColType_UUID):
    pass


@attrs.define(frozen=True)
class String_UUID(ColType_UUID, StringType):
    pass


@attrs.define(frozen=True)
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


@attrs.define(frozen=True)
class String_VaryingAlphanum(String_Alphanum):
    pass


@attrs.define(frozen=True)
class String_FixedAlphanum(String_Alphanum):
    length: int

    def make_value(self, value):
        if len(value) != self.length:
            raise ValueError(f"Expected alphanumeric value of length {self.length}, but got '{value}'.")
        return self.python_type(value, max_len=self.length)


@attrs.define(frozen=True)
class Text(StringType):
    @property
    def supported(self) -> bool:
        return False


# In majority of DBMSes, it is called JSON/JSONB. Only in Snowflake, it is OBJECT.
@attrs.define(frozen=True)
class JSON(ColType):
    pass


@attrs.define(frozen=True)
class Array(ColType):
    item_type: ColType


# Unlike JSON, structs are not free-form and have a very specific set of fields and their types.
# We do not parse & use those fields now, but we can do this later.
# For example, in BigQuery:
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
# - https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#struct_literals
@attrs.define(frozen=True)
class Struct(ColType):
    pass


@attrs.define(frozen=True)
class Integer(NumericType, IKey):
    precision: int = 0
    python_type: type = int

    def __attrs_post_init__(self):
        assert self.precision == 0


@attrs.define(frozen=True)
class UnknownColType(ColType):
    text: str

    @property
    def supported(self) -> bool:
        return False
