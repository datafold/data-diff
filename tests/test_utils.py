import unittest
import re

from data_diff.utils import (
    remove_passwords_in_dict,
    match_regexps,
    match_like,
    number_to_human,
    diff_int_dynamic_color_template,
    dbt_diff_string_template,
    columns_removed_template,
    columns_added_template,
    columns_type_changed_template,
)

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


class TestDiffIntDynamicColorTemplate(unittest.TestCase):
    def test_string_input(self):
        self.assertEqual(diff_int_dynamic_color_template("test_string"), "test_string")

    def test_positive_diff_value(self):
        self.assertEqual(diff_int_dynamic_color_template(10), "[green]+10[/]")

    def test_negative_diff_value(self):
        self.assertEqual(diff_int_dynamic_color_template(-10), "[red]-10[/]")

    def test_zero_diff(self):
        self.assertEqual(diff_int_dynamic_color_template(0), "0")


class TestDbtDiffStringTemplateNoMock(unittest.TestCase):
    def test_dbt_diff_string_template(self):
        self.maxDiff = None

        expected_output = """
rows       PROD    <>            DEV
---------  ------  ------------  ------------------
Total      10                    20 [[green]+10[/]]
Added              [green]+5[/]
Removed            [red]-2[/]
Different          3
Unchanged          5

columns    # diff values
---------  ---------------
info       values

deps    # data assets
------  ---------------
dep     assets"""

        output = dbt_diff_string_template(
            total_rows_table1=10,
            total_rows_table2=20,
            total_rows_diff=10,
            rows_added=5,
            rows_removed=2,
            rows_updated=3,
            rows_unchanged=5,
            extra_info_dict={"info": "values"},
            extra_info_str="extra info",
            is_cloud=False,
            deps_impacts={"dep": "assets"},
        )

        self.assertEqual(output, expected_output)


class TestColumnsTemplateMethods(unittest.TestCase):
    def extract_columns_set(self, output):
        # Extract quoted words by regex
        output_list = re.findall(r"'(\w*)'", output)
        # Convert list to set
        output_set = set(output_list)
        return output_set

    def test_columns_removed_template(self):
        output = columns_removed_template({"column1", "column2"})
        self.assertIn("[red]Columns removed [-2]:[/]", output)
        self.assertEqual(self.extract_columns_set(output), {"column1", "column2"})

    def test_columns_added_template(self):
        output = columns_added_template({"column1", "column2"})
        self.assertIn("[green]Columns added [+2]:", output)
        self.assertEqual(self.extract_columns_set(output), {"column1", "column2"})

    def test_columns_type_changed_template(self):
        output = columns_type_changed_template({"column1", "column2"})
        self.assertIn("Type changed [2]: [green]", output)
        self.assertEqual(self.extract_columns_set(output), {"column1", "column2"})
