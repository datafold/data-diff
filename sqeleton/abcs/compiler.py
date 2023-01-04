from typing import Any, Dict
from abc import ABC, abstractmethod


class AbstractCompiler(ABC):
    @abstractmethod
    def compile(self, elem: Any, params: Dict[str, Any] = None) -> str:
        ...


class Compilable(ABC):
    # TODO generic syntax, so we can write Compilable[T] for expressions returning a value of type T
    @abstractmethod
    def compile(self, c: AbstractCompiler) -> str:
        ...
