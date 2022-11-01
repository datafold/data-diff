from datetime import datetime
from typing import List, Optional
import unittest
from data_diff.databases.database_types import AbstractDatabase, AbstractDialect, CaseInsensitiveDict, CaseSensitiveDict

from data_diff.queries import this, table, Compiler, outerjoin, cte
from data_diff.queries.ast_classes import Random


def normalize_spaces(s: str):
    return " ".join(s.split())


class MockDialect(AbstractDialect):
    name = "MockDialect"

    ROUNDS_ON_PREC_LOSS = False

    def quote(self, s: str) -> str:
        return s

    def concat(self, l: List[str]) -> str:
        s = ", ".join(l)
        return f"concat({s})"

    def to_string(self, s: str) -> str:
        return f"cast({s} as varchar)"

    def is_distinct_from(self, a: str, b: str) -> str:
        return f"{a} is distinct from {b}"

    def random(self) -> str:
        return "random()"

    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        x = offset and f"offset {offset}", limit and f"limit {limit}"
        return " ".join(filter(None, x))

    def explain_as_text(self, query: str) -> str:
        return f"explain {query}"

    def timestamp_value(self, t: datetime) -> str:
        return f"timestamp '{t}'"

    parse_type = NotImplemented


class MockDatabase(AbstractDatabase):
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

        t = table("point").select("x", "y")
        assert c.compile(t) == "SELECT x, y FROM point"

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

        # diffed = j.select("*", **{f"is_diff_col_{c}": a[c].is_distinct_from(b[c]) for c in cols})

        # t = diffed.select(
        #     **{f"total_diff_col_{c}": diffed[f"is_diff_col_{c}"].sum() for c in cols},
        #     total_diff=or_(diffed[f"is_diff_col_{c}"] for c in cols).sum(),
        # )

        # print(c.compile(t))

        # t.group_by(keys=[this.x], values=[this.py])

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
        assert q == "SELECT * FROM a ORDER BY random() limit 10"

    def test_union(self):
        c = Compiler(MockDatabase())
        a = table("a").select("x")
        b = table("b").select("y")

        q = c.compile(a.union(b))
        assert q == "SELECT x FROM a UNION SELECT y FROM b"
