import unittest

from data_diff.utils import remove_passwords_in_dict, match_regexps, match_like, number_to_human
from data_diff.__main__ import _remove_passwords_in_dict


class TestUtils(unittest.TestCase):
    def test_remove_passwords_in_dict(self):
        # Test replacing password value
        d = {"password": "mypassword"}
        remove_passwords_in_dict(d)
        assert d["password"] == "***"

        # Test replacing password in database URL
        d = {"database_url": "mysql://user:mypassword@localhost/db"}
        remove_passwords_in_dict(d, "$$$$")
        assert d["database_url"] == "mysql://user:$$$$@localhost/db"

        # Test replacing motherduck token in database URL
        d = {"database_url": "md:datafold_demo?motherduck_token=jaiwefjoaisdk"}
        remove_passwords_in_dict(d, "$$$$")
        assert d["database_url"] == "md:datafold_demo?motherduck_token=$$$$"

        # Test replacing password in nested dictionary
        d = {"info": {"password": "mypassword"}}
        remove_passwords_in_dict(d, "%%")
        assert d["info"]["password"] == "%%"

        # Test replacing a motherduck token in nested dictionary
        d = {
            "database1": {"driver": "duckdb", "filepath": "md:datafold_demo?motherduck_token=awieojfaowiejacijobhiwaef"}
        }
        remove_passwords_in_dict(d, "%%")
        assert d["database1"]["filepath"] == "md:datafold_demo?motherduck_token=%%"

    # Test __main__ utility version of this function
    def test__main__remove_passwords_in_dict(self):
        # Test replacing password value
        d = {"password": "mypassword"}
        _remove_passwords_in_dict(d)
        assert d["password"] == "**********"

        # Test replacing password in database URL
        d = {"database_url": "mysql://user:mypassword@localhost/db"}
        _remove_passwords_in_dict(d)
        assert d["database_url"] == "mysql://user:***@localhost/db"

        # Test replacing motherduck token in database URL
        d = {"database_url": "md:datafold_demo?motherduck_token=jaiwefjoaisdk"}
        _remove_passwords_in_dict(d)
        assert d["database_url"] == "md:datafold_demo?motherduck_token=***"

        # Test replacing password in nested dictionary
        d = {"info": {"password": "mypassword"}}
        _remove_passwords_in_dict(d)
        assert d["info"]["password"] == "**********"

        # Test replacing a motherduck token in nested dictionary
        d = {
            "database1": {"driver": "duckdb", "filepath": "md:datafold_demo?motherduck_token=awieojfaowiejacijobhiwaef"}
        }
        _remove_passwords_in_dict(d)
        assert d["database1"]["filepath"] == "md:datafold_demo?motherduck_token=**********"

    def test_match_regexps(self):
        def only_results(x):
            return [v for k, v in x]

        # Test with no matches
        regexps = {"a*": 1, "b*": 2}
        s = "c"
        assert only_results(match_regexps(regexps, s)) == []

        # Test with one match
        regexps = {"a*": 1, "b*": 2}
        s = "b"
        assert only_results(match_regexps(regexps, s)) == [2]

        # Test with multiple matches
        regexps = {"abc": 1, "ab*c": 2, "c*": 3}
        s = "abc"
        assert only_results(match_regexps(regexps, s)) == [1, 2]

        # Test with regexp that doesn't match the end of the string
        regexps = {"a*b": 1}
        s = "acb"
        assert only_results(match_regexps(regexps, s)) == []

    def test_match_like(self):
        strs = ["abc", "abcd", "ab", "bcd", "def"]

        # Test exact match
        pattern = "abc"
        result = list(match_like(pattern, strs))
        assert result == ["abc"]

        # Test % match
        pattern = "a%"
        result = list(match_like(pattern, strs))
        self.assertEqual(result, ["abc", "abcd", "ab"])

        # Test ? match
        pattern = "a?c"
        result = list(match_like(pattern, strs))
        self.assertEqual(result, ["abc"])

    def test_number_to_human(self):
        # Test basic conversion
        assert number_to_human(1000) == "1k"
        assert number_to_human(1000000) == "1m"
        assert number_to_human(1000000000) == "1b"

        # Test decimal values
        assert number_to_human(1234) == "1k"
        assert number_to_human(12345) == "12k"
        assert number_to_human(123456) == "123k"
        assert number_to_human(1234567) == "1m"
        assert number_to_human(12345678) == "12m"
        assert number_to_human(123456789) == "123m"
        assert number_to_human(1234567890) == "1b"

        # Test negative values
        assert number_to_human(-1000) == "-1k"
        assert number_to_human(-1000000) == "-1m"
        assert number_to_human(-1000000000) == "-1b"
