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
