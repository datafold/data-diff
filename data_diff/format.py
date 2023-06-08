import collections

from data_diff.diff_tables import DiffResultWrapper
from typing import TypedDict, Any, Optional



class ColumnsDiff(TypedDict):
    removed: list[str]
    added: list[str]
    changed: list[str]


def jsonify(diff: DiffResultWrapper,
            with_summary: bool = False,
            with_columns: ColumnsDiff | None = None) -> 'JsonDiff':
    """
    Converts the diff result into a JSON-serializable format.
    Optionally add stats summary and schema diff.
    """
    diff_info = diff.info_tree.info
    table1 = diff_info.tables[0]
    table2 = diff_info.tables[1]
    key_columns = table1.key_columns

    t1_exclusive_rows = []
    t2_exclusive_rows = []
    diff_rows = []
    schema = [field for field, _ in diff_info.diff_schema]

    t1_exclusive_rows, t2_exclusive_rows, diff_rows = _group_rows(diff_info, schema)


    diff_rows_jsonified = []
    for row in diff_rows:
        diff_rows_jsonified.append(_jsonify_diff(row, key_columns))

    t1_exclusive_rows_jsonified = []
    for row in t1_exclusive_rows:
        t1_exclusive_rows_jsonified.append(_jsonify_exclusive(row, key_columns))

    t2_exclusive_rows_jsonified = []
    for row in t2_exclusive_rows:
        t2_exclusive_rows_jsonified.append(_jsonify_exclusive(row, key_columns))
    
    summary = None
    if with_summary:
        summary = _jsonify_diff_summary(diff.get_stats_dict())
    
    columns = None
    if with_columns:
        added, removed, changed = with_columns['added'], with_columns['removed'], with_columns['changed']
        columns = _jsonify_columns_diff(added, removed, changed)

    is_different = bool(
        t1_exclusive_rows
        or t2_exclusive_rows
        or diff_rows
        or with_columns and (
            with_columns['added']
            or with_columns['removed']
            or with_columns['changed']
        )
    )
    return {
        'isDifferent': is_different,
        'table1': list(table1.table_path),
        'table2': list(table2.table_path),
        'rows': {
            'exclusive': {
                'table1': t1_exclusive_rows_jsonified,
                'table2': t2_exclusive_rows_jsonified,
            },
            'diff': diff_rows_jsonified,
        },
        'summary': summary,
        'columns': columns,
    }

class JsonDiff(TypedDict):
    table1: list[str]
    table2: list[str]
    rows: TypedDict('Rows', {
        'exclusive': TypedDict('Exclusive', {
            'table1': list['JsonExclusiveRow'],
            'table2': list['JsonExclusiveRow'],
        }),
        'diff': list['JsonDiffRow'],
    })
    summary: Optional['JsonDiffSummary' ]
    columns: Optional['JsonColumnsSummary']



class JsonExclusiveRowValue(TypedDict):
    """
    Value of a single column in a row
    """
    isPK: bool
    value: Any

class JsonDiffRowValue(TypedDict):
    """
    Pair of diffed values for 2 rows with equal PKs
    """
    table1: Any
    table2: Any
    isDiff: bool
    isPK: bool


JsonDiffRow = dict[str, JsonDiffRowValue]
JsonExclusiveRow = dict[str, JsonExclusiveRowValue]


class JsonDiffSummary(TypedDict):
    rows: TypedDict('Rows', {
        'total': TypedDict('Total', {
            'table1': int,
            'table2': int,
        }),
        'exclusive': TypedDict('Exclusive', {
            'table1': int,
            'table2': int,
        }),
        'updated': int,
        'unchanged': int,
    })
    stats: TypedDict('Stats', {
        'diffCounts': dict[str, int],
    })

class JsonColumnsSummary(TypedDict):
    exclusive: TypedDict('Exclusive', {
        'table1': list[str],
        'table2': list[str],
    })
    typeChanged: list[str]



def _group_rows(diff_info: DiffResultWrapper, 
                schema: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    t1_exclusive_rows = []
    t2_exclusive_rows = []
    diff_rows = []

    for row in diff_info.diff:
        row_w_schema = dict(zip(schema, row))
        is_t1_exclusive = row_w_schema['is_exclusive_a']
        is_t2_exclusive = row_w_schema['is_exclusive_b']

        if is_t1_exclusive:
            t1_exclusive_rows.append(row_w_schema)

        elif is_t2_exclusive:
            t2_exclusive_rows.append(row_w_schema)

        else:
            diff_rows.append(row_w_schema)
    
    return t1_exclusive_rows, t2_exclusive_rows, diff_rows


def _jsonify_diff(row: dict[str, Any], key_columns: list[str]) -> JsonDiffRowValue:
    columns = collections.defaultdict(dict)
    for field, value in row.items():
        if field in ('is_exclusive_a', 'is_exclusive_b'):
            continue

        if field.startswith('is_diff_'):
            column_name = field.replace('is_diff_', '')
            columns[column_name]['isDiff'] = bool(value)

        elif field.endswith('_a'):
            column_name = field.replace('_a', '')
            columns[column_name]['table1'] = value
            columns[column_name]['isPK'] = column_name in key_columns

        elif field.endswith('_b'):
            column_name = field.replace('_b', '')
            columns[column_name]['table2'] = value
            columns[column_name]['isPK'] = column_name in key_columns
    
    return columns


def _jsonify_exclusive(row: dict[str, Any], key_columns: list[str]) -> JsonExclusiveRow:
    columns = collections.defaultdict(dict)
    for field, value in row.items():
        if field in ('is_exclusive_a', 'is_exclusive_b'):
            continue
        if field.startswith('is_diff_'):
            continue
        if field.endswith('_b') and row['is_exclusive_b']:
            column_name = field.replace('_b', '')
            columns[column_name]['isPK'] = column_name in key_columns
            columns[column_name]['value'] = value
        elif field.endswith('_a') and row['is_exclusive_a']:
            column_name = field.replace('_a', '')
            columns[column_name]['isPK'] = column_name in key_columns
            columns[column_name]['value'] = value
    return columns


def _jsonify_diff_summary(stats_dict: dict) -> JsonDiffSummary:
    return {
        'rows': {
            'total': {
               'table1': stats_dict["rows_A"],
               'table2': stats_dict["rows_B"]
            },
            'exclusive': {
                'table1': stats_dict["exclusive_A"],
                'table2': stats_dict["exclusive_B"],
            },
            'updated': stats_dict["updated"],
            'unchanged': stats_dict["unchanged"]
        },
        'stats': {
            'diffCounts': stats_dict["stats"]['diff_counts']
        }
    }


def _jsonify_columns_diff(added_columns: list[str], 
                          removed_columns: list[str],
                          changed_columns: list[str]) -> JsonColumnsSummary:
    columns = {
        'exclusive': {
            'table2': list(added_columns),
            'table1': list(removed_columns),
        },
        'typeChanged': list(changed_columns),
    }
    return columns