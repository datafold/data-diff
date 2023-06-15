import unittest
from data_diff.diff_tables import DiffResultWrapper, InfoTree, SegmentInfo, TableSegment
from data_diff.format import jsonify
from data_diff.sqeleton.databases import Database


class TestFormat(unittest.TestCase):
    maxDiff = None

    def test_jsonify_diff(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(info=SegmentInfo(
                tables=[
                    TableSegment(table_path=('db', 'schema', 'table1'), key_columns=('id',), database=Database()),
                    TableSegment(table_path=('db', 'schema', 'table2'), key_columns=('id',), database=Database())
                ],
                diff_schema=(('is_exclusive_a', bool),
                             ('is_exclusive_b', bool),
                             ('is_diff_id', int),
                             ('is_diff_value', int),
                             ('id_a', str),
                             ('id_b', str),
                             ('value_a', str),
                             ('value_b', str)),
                diff=[(False, False, 0, 1, '1', '1', '3', '201'),
                      (True, False, 1, 1, '2', None, '4', None),
                      (False, True, 1, 1, None, '3', None, '202')])
            ),
            diff=[], stats={}
        )
        json_diff = jsonify(diff, dbt_model='my_model')
        self.assertEqual(json_diff, {
            'version': '1.0.0',
            'status': 'different',
            'model': 'my_model',
            'table1': ['db', 'schema', 'table1'],
            'table2': ['db', 'schema', 'table2'],
            'rows': {
                'exclusive': {
                    'table1': [
                        {
                            'id': {'isPK': True, 'value': '2'},
                            'value': {'isPK': False, 'value': '4'}
                        }
                    ],
                    'table2': [
                        {
                            'id': {'isPK': True, 'value': '3'},
                            'value': {'isPK': False, 'value': '202'}
                        }
                    ]},
                'diff': [
                    {
                        'id': {'isPK': True, 'table1': '1', 'table2': '1', 'isDiff': False},
                        'value': {'isPK': False, 'table1': '3', 'table2': '201', 'isDiff': True},
                    },
                ],
            },
            'summary': None,
            'columns': None,
        })

    def test_jsonify_diff_no_difeference(self):
        diff = DiffResultWrapper(
            info_tree=InfoTree(info=SegmentInfo(
                tables=[
                    TableSegment(table_path=('db', 'schema', 'table1'), key_columns=('id',), database=Database()),
                    TableSegment(table_path=('db', 'schema', 'table2'), key_columns=('id',), database=Database())
                ],
                diff_schema=(('is_exclusive_a', bool),
                             ('is_exclusive_b', bool),
                             ('is_diff_id', int),
                             ('is_diff_value', int),
                             ('id_a', str),
                             ('id_b', str),
                             ('value_a', str),
                             ('value_b', str)),
                diff=[])
            ),
            diff=[], stats={}
        )
        json_diff = jsonify(diff, dbt_model='model')
        self.assertEqual(json_diff, {
            'version': '1.0.0',
            'status': 'identical',
            'model': 'model',
            'table1': ['db', 'schema', 'table1'],
            'table2': ['db', 'schema', 'table2'],
            'rows': {
                'exclusive': {
                    'table1': [],
                    'table2': []},
                'diff': [],
            },
            'summary': None,
            'columns': None,
        })
