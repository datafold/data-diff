from abc import ABC

import attrs


@attrs.define
class AbstractCompiler(ABC):
    pass


@attrs.define(eq=False)
class Compilable(ABC):
    pass
