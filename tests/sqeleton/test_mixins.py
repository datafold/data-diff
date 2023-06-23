import unittest

from data_diff.sqeleton import connect

from data_diff.sqeleton.abcs import AbstractDialect, AbstractDatabase
from data_diff.sqeleton.abcs.mixins import (
    AbstractMixin_NormalizeValue,
    AbstractMixin_RandomSample,
    AbstractMixin_TimeTravel,
)


class TestMixins(unittest.TestCase):
    def test_normalize(self):
        # - Test sanity
        ddb1 = connect("duckdb://:memory:")
        assert not hasattr(ddb1.dialect, "normalize_boolean")

        # - Test abstract mixins
        class NewAbstractDialect(AbstractDialect, AbstractMixin_NormalizeValue, AbstractMixin_RandomSample):
            pass

        new_connect = connect.load_mixins(AbstractMixin_NormalizeValue, AbstractMixin_RandomSample)
        ddb2: AbstractDatabase[NewAbstractDialect] = new_connect("duckdb://:memory:")
        # Implementation may change; Just update the test
        assert ddb2.dialect.normalize_boolean("bool", None) == "bool::INTEGER::VARCHAR"
        assert ddb2.dialect.random_sample_n("x", 10)

        # - Test immutability
        ddb3 = connect("duckdb://:memory:")
        assert not hasattr(ddb3.dialect, "normalize_boolean")

        self.assertRaises(TypeError, connect.load_mixins, AbstractMixin_TimeTravel)

        new_connect = connect.for_databases("bigquery", "snowflake").load_mixins(AbstractMixin_TimeTravel)
        self.assertRaises(NotImplementedError, new_connect, "duckdb://:memory:")
