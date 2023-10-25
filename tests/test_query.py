from datetime import datetime
from typing import List, Optional
import unittest

from data_diff.abcs.database_types import FractionalType, TemporalType
from data_diff.databases.base import Database, BaseDialect
from data_diff.utils import CaseInsensitiveDict, CaseSensitiveDict

from data_diff.databases.base import Compiler, CompileError
from data_diff.queries.api import outerjoin, cte, when, coalesce
from data_diff.queries.ast_classes import Random
from data_diff.queries.api import code, this, table


def normalize_spaces(s: str):
    return " ".join(s.split())


class MockDialect(BaseDialect):
    name = "MockDialect"

    PLACEHOLDER_TABLE = None
    ROUNDS_ON_PREC_LOSS = False

    def quote(self, s: str) -> str:
        return s

    def concat(self, l: List[str]) -> str:
        s = ", ".join(l)
        return f"concat({s})"

    def to_comparable(self, s: str) -> str:
        return s

    def to_string(self, s: str) -> str:
        return f"cast({s} as varchar)"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"{a} is distinct from {b}"

    def random(self) -> str:
        return "random()"

    def current_timestamp(self) -> str:
        return "now()"

    def current_database(self) -> str:
        return "current_database()"

    def current_schema(self) -> str:
        return "current_schema()"

    def offset_limit(
        self, offset: Optional[int] = None, limit: Optional[int] = None, has_order_by: Optional[bool] = None
    ) -> str:
        x = offset and f"OFFSET {offset}", limit and f"LIMIT {limit}"
        return " ".join(filter(None, x))

    def explain_as_text(self, query: str) -> str:
        return f"explain {query}"

    def timestamp_value(self, t: datetime) -> str:
        return f"timestamp '{t}'"

    def set_timezone_to_utc(self) -> str:
        return "set timezone 'UTC'"

    def optimizer_hints(self, s: str):
        return f"/*+ {s} */ "

    def md5_as_int(self, s: str) -> str:
        raise NotImplementedError

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        raise NotImplementedError

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        raise NotImplementedError

    parse_type = NotImplemented


class MockDatabase(Database):
    CONNECT_URI_HELP = "mock://"
    CONNECT_URI_PARAMS = []
    dialect = MockDialect()

    _query = NotImplemented
    query_table_schema = NotImplemented
    select_table_schema = NotImplemented
    _process_table_schema = NotImplemented
    parse_table_name = NotImplemented
    close = NotImplemented
    _normalize_table_path = NotImplemented
    is_autocommit = NotImplemented


class TestQuery(unittest.TestCase):
    def setUp(self):
        pass

    def test_basic(self):
        c = Compiler(MockDatabase())

        t = table("point")
        t2 = t.select(x=this.x + 1, y=t["y"] + this.x)
        assert c.compile(t2) == "SELECT (x + 1) AS x, (y + x) AS y FROM point"

        t = table("point").where(this.x == 1, this.y == 2)
        assert c.compile(t) == "SELECT * FROM point WHERE (x = 1) AND (y = 2)"

        t = table("person").where(this.name == "Albert")
        self.assertEqual(c.compile(t), "SELECT * FROM person WHERE (name = 'Albert')")

    def test_outerjoin(self):
        c = Compiler(MockDatabase())

        a = table("a")
        b = table("b")
        keys = ["x", "y"]
        cols = ["u", "v"]

        j = outerjoin(a, b).on(a[k] == b[k] for k in keys)

        self.assertEqual(
            c.compile(j), "SELECT * FROM a tmp1 FULL OUTER JOIN b tmp2 ON (tmp1.x = tmp2.x) AND (tmp1.y = tmp2.y)"
        )

    def test_schema(self):
        c = Compiler(MockDatabase())
        schema = dict(id="int", comment="varchar")

        # test table
        t = table("a", schema=CaseInsensitiveDict(schema))
        q = t.select(this.Id, t["COMMENT"])
        assert c.compile(q) == "SELECT id, comment FROM a"

        t = table("a", schema=CaseSensitiveDict(schema))
        self.assertRaises(KeyError, t.__getitem__, "Id")
        self.assertRaises(KeyError, t.select, this.Id)

        # test select
        q = t.select(this.id)
        self.assertRaises(KeyError, q.__getitem__, "comment")

        # test join
        s = CaseInsensitiveDict({"x": int, "y": int})
        a = table("a", schema=s)
        b = table("b", schema=s)
        keys = ["x", "y"]
        j = outerjoin(a, b).on(a[k] == b[k] for k in keys).select(a["x"], b["y"], xsum=a["x"] + b["x"])
        j["x"], j["y"], j["xsum"]
        self.assertRaises(KeyError, j.__getitem__, "ysum")

    def test_commutable_select(self):
        # c = Compiler(MockDatabase())

        t = table("a")
        q1 = t.select("a").where("b")
        q2 = t.where("b").select("a")
        assert q1 == q2, (q1, q2)

    def test_cte(self):
        c = Compiler(MockDatabase())

        t = table("a")

        # single cte
        t2 = cte(t.select(this.x))
        t3 = t2.select(this.x)

        expected = "WITH tmp1 AS (SELECT x FROM a) SELECT x FROM tmp1"
        assert normalize_spaces(c.compile(t3)) == expected

        # nested cte
        c = Compiler(MockDatabase())
        t4 = cte(t3).select(this.x)

        expected = "WITH tmp1 AS (SELECT x FROM a), tmp2 AS (SELECT x FROM tmp1) SELECT x FROM tmp2"
        assert normalize_spaces(c.compile(t4)) == expected

        # parameterized cte
        c = Compiler(MockDatabase())
        t2 = cte(t.select(this.x), params=["y"])
        t3 = t2.select(this.y)

        expected = "WITH tmp1(y) AS (SELECT x FROM a) SELECT y FROM tmp1"
        assert normalize_spaces(c.compile(t3)) == expected

    def test_funcs(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.order_by(Random()).limit(10))
        self.assertEqual(q, "SELECT * FROM a ORDER BY random() LIMIT 10")

        q = c.compile(t.select(coalesce(this.a, this.b)))
        self.assertEqual(q, "SELECT COALESCE(a, b) FROM a")

    def test_select_distinct(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.select(this.b, distinct=True))
        assert q == "SELECT DISTINCT b FROM a"

        # selects merge
        q = c.compile(t.where(this.b > 10).select(this.b, distinct=True))
        self.assertEqual(q, "SELECT DISTINCT b FROM a WHERE (b > 10)")

        # selects stay apart
        q = c.compile(t.limit(10).select(this.b, distinct=True))
        self.assertEqual(q, "SELECT DISTINCT b FROM (SELECT * FROM a LIMIT 10) tmp1")

        q = c.compile(t.select(this.b, distinct=True).select(distinct=False))
        self.assertEqual(q, "SELECT * FROM (SELECT DISTINCT b FROM a) tmp2")

    def test_select_with_optimizer_hints(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.select(this.b, optimizer_hints="PARALLEL(a 16)"))
        assert q == "SELECT /*+ PARALLEL(a 16) */ b FROM a"

        q = c.compile(t.where(this.b > 10).select(this.b, optimizer_hints="PARALLEL(a 16)"))
        self.assertEqual(q, "SELECT /*+ PARALLEL(a 16) */ b FROM a WHERE (b > 10)")

        q = c.compile(t.limit(10).select(this.b, optimizer_hints="PARALLEL(a 16)"))
        self.assertEqual(q, "SELECT /*+ PARALLEL(a 16) */ b FROM (SELECT * FROM a LIMIT 10) tmp1")

        q = c.compile(t.select(this.a).group_by(this.b).agg(this.c).select(optimizer_hints="PARALLEL(a 16)"))
        self.assertEqual(
            q, "SELECT /*+ PARALLEL(a 16) */ * FROM (SELECT b, c FROM (SELECT a FROM a) tmp2 GROUP BY 1) tmp3"
        )

    def test_table_ops(self):
        c = Compiler(MockDatabase())
        a = table("a").select(this.x)
        b = table("b").select(this.y)

        q = c.compile(a.union(b))
        assert q == "SELECT x FROM a UNION SELECT y FROM b"

        q = c.compile(a.union_all(b))
        assert q == "SELECT x FROM a UNION ALL SELECT y FROM b"

        q = c.compile(a.minus(b))
        assert q == "SELECT x FROM a EXCEPT SELECT y FROM b"

        q = c.compile(a.intersect(b))
        assert q == "SELECT x FROM a INTERSECT SELECT y FROM b"

    def test_ops(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.select(this.b + this.c))
        self.assertEqual(q, "SELECT (b + c) FROM a")

        q = c.compile(t.select(this.b.like(this.c)))
        self.assertEqual(q, "SELECT (b LIKE c) FROM a")

        q = c.compile(t.select(-this.b.sum()))
        self.assertEqual(q, "SELECT (-SUM(b)) FROM a")

    def test_group_by(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.group_by(this.b).agg(this.c))
        self.assertEqual(q, "SELECT b, c FROM a GROUP BY 1")

        q = c.compile(t.where(this.b > 1).group_by(this.b).agg(this.c))
        self.assertEqual(q, "SELECT b, c FROM a WHERE (b > 1) GROUP BY 1")

        self.assertRaises(CompileError, c.compile, t.select(this.b).group_by(this.b))

        q = c.compile(t.select(this.b).group_by(this.b).agg())
        self.assertEqual(q, "SELECT b FROM (SELECT b FROM a) tmp1 GROUP BY 1")

        q = c.compile(t.group_by(this.b, this.c).agg(this.d, this.e))
        self.assertEqual(q, "SELECT b, c, d, e FROM a GROUP BY 1, 2")

        # Having
        q = c.compile(t.group_by(this.b).agg(this.c).having(this.b > 1))
        self.assertEqual(q, "SELECT b, c FROM a GROUP BY 1 HAVING (b > 1)")

        q = c.compile(t.group_by(this.b).having(this.b > 1).agg(this.c))
        self.assertEqual(q, "SELECT b, c FROM a GROUP BY 1 HAVING (b > 1)")

        q = c.compile(t.select(this.b).group_by(this.b).agg().having(this.b > 1))
        self.assertEqual(q, "SELECT b FROM (SELECT b FROM a) tmp2 GROUP BY 1 HAVING (b > 1)")

        # Having sum
        q = c.compile(t.group_by(this.b).agg(this.c, this.d).having(this.b.sum() > 1))
        self.assertEqual(q, "SELECT b, c, d FROM a GROUP BY 1 HAVING (SUM(b) > 1)")

        # Select interaction
        q = c.compile(t.select(this.a).group_by(this.b).agg(this.c).select(this.c + 1))
        self.assertEqual(q, "SELECT (c + 1) FROM (SELECT b, c FROM (SELECT a FROM a) tmp3 GROUP BY 1) tmp4")

    def test_case_when(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.select(when(this.b).then(this.c)))
        self.assertEqual(q, "SELECT CASE WHEN b THEN c END FROM a")

        q = c.compile(t.select(when(this.b).then(this.c).else_(this.d)))
        self.assertEqual(q, "SELECT CASE WHEN b THEN c ELSE d END FROM a")

        q = c.compile(
            t.select(
                when(this.type == "text")
                .then(this.text)
                .when(this.type == "number")
                .then(this.number)
                .else_("unknown type")
            )
        )
        self.assertEqual(
            q,
            "SELECT CASE WHEN (type = 'text') THEN text WHEN (type = 'number') THEN number ELSE 'unknown type' END FROM a",
        )

    def test_code(self):
        c = Compiler(MockDatabase())
        t = table("a")

        q = c.compile(t.select(this.b, code("<x>")).where(code("<y>")))
        self.assertEqual(q, "SELECT b, <x> FROM a WHERE <y>")

        def tablesample(t, size):
            return code("{t} TABLESAMPLE BERNOULLI ({size})", t=t, size=size)

        nonzero = table("points").where(this.x > 0, this.y > 0)

        q = c.compile(tablesample(nonzero, 10))
        self.assertEqual(q, "SELECT * FROM points WHERE (x > 0) AND (y > 0) TABLESAMPLE BERNOULLI (10)")
