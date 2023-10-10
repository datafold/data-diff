"Useful AST classes that don't quite fall within the scope of regular SQL"
from typing import Callable, Optional, Sequence

import attrs

from data_diff.abcs.database_types import ColType
from data_diff.queries.ast_classes import Expr, ExprNode


@attrs.define(frozen=True)
class NormalizeAsString(ExprNode):
    expr: ExprNode
    expr_type: Optional[ColType] = None

    @property
    def type(self) -> Optional[type]:
        return str


@attrs.define(frozen=True)
class ApplyFuncAndNormalizeAsString(ExprNode):
    expr: ExprNode
    apply_func: Optional[Callable] = None


@attrs.define(frozen=True)
class Checksum(ExprNode):
    exprs: Sequence[Expr]
