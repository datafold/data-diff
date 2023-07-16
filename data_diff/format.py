import collections
from enum import Enum
from typing import Any, Optional, List, Dict, Tuple, Type

from runtype import dataclass
from data_diff.diff_tables import DiffResultWrapper
from data_diff.sqeleton.abcs.database_types import (
    JSON,
    Boolean,
    ColType,
    Array,
    ColType_UUID,
    Date,
    FractionalType,
    NumericType,
    Struct,
    TemporalType,
    ColType_Alphanum,
    String_Alphanum,
)


def jsonify_error(table1: List[str], table2: List[str], dbt_model: str, error: str) -> "FailedDiff":
    return FailedDiff(
        status="failed",
        model=dbt_model,
        dataset1=table1,
        dataset2=table2,
        error=error,
    ).json()


Columns = List[Tuple[str, str, ColType]]


def jsonify(
    diff: DiffResultWrapper,
    dbt_model: str,
    dataset1_columns: Columns,
    dataset2_columns: Columns,
    columns_diff: Dict[str, List[str]],
    with_summary: bool = False,
) -> "JsonDiff":
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
        summary = _jsonify_diff_summary(diff.get_stats_dict(is_dbt=True))

    columns = _jsonify_columns_diff(dataset1_columns, dataset2_columns, columns_diff, list(key_columns))

    is_different = bool(
        t1_exclusive_rows
        or t2_exclusive_rows
        or diff_rows
        or (columns_diff["added"] or columns_diff["removed"] or columns_diff["changed"])
    )
    return JsonDiff(
        status="success",
        result="different" if is_different else "identical",
        model=dbt_model,
        dataset1=list(table1.table_path),
        dataset2=list(table2.table_path),
        rows=RowsDiff(
            exclusive=ExclusiveDiff(dataset1=t1_exclusive_rows_jsonified, dataset2=t2_exclusive_rows_jsonified),
            diff=diff_rows_jsonified,
        ),
        summary=summary,
        columns=columns,
    ).json()


@dataclass
class JsonExclusiveRowValue:
    """
    Value of a single column in a row
    """

    isPK: bool
    value: Any


@dataclass
class JsonDiffRowValue:
    """
    Pair of diffed values for 2 rows with equal PKs
    """

    dataset1: Any
    dataset2: Any
    isDiff: bool
    isPK: bool


@dataclass
class Total:
    dataset1: int
    dataset2: int


@dataclass
class ExclusiveRows:
    dataset1: int
    dataset2: int


@dataclass
class Rows:
    total: Total
    exclusive: ExclusiveRows
    updated: int
    unchanged: int


@dataclass
class Stats:
    diffCounts: Dict[str, int]


@dataclass
class JsonDiffSummary:
    rows: Rows
    stats: Stats


@dataclass
class ExclusiveColumns:
    dataset1: List[str]
    dataset2: List[str]


class ColumnKind(Enum):
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    DATE = "date"
    TIME = "time"
    DATETIME = "datetime"
    BOOL = "boolean"
    UNSUPPORTED = "unsupported"


KIND_MAPPING: List[Tuple[Type[ColType], ColumnKind]] = [
    (Boolean, ColumnKind.BOOL),
    (Date, ColumnKind.DATE),
    (TemporalType, ColumnKind.DATETIME),
    (FractionalType, ColumnKind.FLOAT),
    (NumericType, ColumnKind.INTEGER),
    (ColType_UUID, ColumnKind.STRING),
    (ColType_Alphanum, ColumnKind.STRING),
    (String_Alphanum, ColumnKind.STRING),
    (JSON, ColumnKind.STRING),
    (Array, ColumnKind.STRING),
    (Struct, ColumnKind.STRING),
    (ColType, ColumnKind.UNSUPPORTED),
]


@dataclass
class Column:
    name: str
    type: str
    kind: str


@dataclass
class JsonColumnsSummary:
    dataset1: List[Column]
    dataset2: List[Column]
    primaryKey: List[str]
    exclusive: ExclusiveColumns
    typeChanged: List[str]


@dataclass
class ExclusiveDiff:
    dataset1: List[Dict[str, JsonExclusiveRowValue]]
    dataset2: List[Dict[str, JsonExclusiveRowValue]]


@dataclass
class RowsDiff:
    exclusive: ExclusiveDiff
    diff: List[Dict[str, JsonDiffRowValue]]


@dataclass
class FailedDiff:
    status: str  # Literal ["failed"]
    model: str
    dataset1: List[str]
    dataset2: List[str]
    error: str

    version: str = "1.0.0"


@dataclass
class JsonDiff:
    status: str  # Literal ["success"]
    result: str  # Literal ["different", "identical"]
    model: str
    dataset1: List[str]
    dataset2: List[str]
    rows: RowsDiff
    summary: Optional[JsonDiffSummary]
    columns: Optional[JsonColumnsSummary]

    version: str = "1.1.0"


def _group_rows(
    diff_info: DiffResultWrapper, schema: List[str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    t1_exclusive_rows = []
    t2_exclusive_rows = []
    diff_rows = []

    for row in diff_info.diff:
        row_w_schema = dict(zip(schema, row))
        is_t1_exclusive = row_w_schema["is_exclusive_a"]
        is_t2_exclusive = row_w_schema["is_exclusive_b"]

        if is_t1_exclusive:
            t1_exclusive_rows.append(row_w_schema)

        elif is_t2_exclusive:
            t2_exclusive_rows.append(row_w_schema)

        else:
            diff_rows.append(row_w_schema)

    return t1_exclusive_rows, t2_exclusive_rows, diff_rows


def _jsonify_diff(row: Dict[str, Any], key_columns: List[str]) -> Dict[str, JsonDiffRowValue]:
    columns = collections.defaultdict(dict)
    for field, value in row.items():
        if field in ("is_exclusive_a", "is_exclusive_b"):
            continue

        if field.startswith("is_diff_"):
            column_name = field[len("is_diff_") :]
            columns[column_name]["isDiff"] = bool(value)

        elif field.endswith("_a"):
            column_name = field[: -len("_a")]
            columns[column_name]["dataset1"] = value
            columns[column_name]["isPK"] = column_name in key_columns

        elif field.endswith("_b"):
            column_name = field[: -len("_b")]
            columns[column_name]["dataset2"] = value
            columns[column_name]["isPK"] = column_name in key_columns

    return {column: JsonDiffRowValue(**data) for column, data in columns.items()}


def _jsonify_exclusive(row: Dict[str, Any], key_columns: List[str]) -> Dict[str, JsonExclusiveRowValue]:
    columns = collections.defaultdict(dict)
    for field, value in row.items():
        if field in ("is_exclusive_a", "is_exclusive_b"):
            continue
        if field.startswith("is_diff_"):
            continue
        if field.endswith("_b") and row["is_exclusive_b"]:
            column_name = field[: -len("_b")]
            columns[column_name]["isPK"] = column_name in key_columns
            columns[column_name]["value"] = value
        elif field.endswith("_a") and row["is_exclusive_a"]:
            column_name = field[: -len("_a")]
            columns[column_name]["isPK"] = column_name in key_columns
            columns[column_name]["value"] = value
    return {column: JsonExclusiveRowValue(**data) for column, data in columns.items()}


def _jsonify_diff_summary(stats_dict: dict) -> JsonDiffSummary:
    return JsonDiffSummary(
        rows=Rows(
            total=Total(dataset1=stats_dict["rows_A"], dataset2=stats_dict["rows_B"]),
            exclusive=ExclusiveRows(
                dataset1=stats_dict["exclusive_A"],
                dataset2=stats_dict["exclusive_B"],
            ),
            updated=stats_dict["updated"],
            unchanged=stats_dict["unchanged"],
        ),
        stats=Stats(diffCounts=stats_dict["values"]),
    )


def _jsonify_columns_diff(
    dataset1_columns: Columns, dataset2_columns: Columns, columns_diff: Dict[str, List[str]], key_columns: List[str]
) -> JsonColumnsSummary:
    return JsonColumnsSummary(
        dataset1=[
            Column(name=name, type=type_, kind=_map_kind(kind).value) for (name, type_, kind) in dataset1_columns
        ],
        dataset2=[
            Column(name=name, type=type_, kind=_map_kind(kind).value) for (name, type_, kind) in dataset2_columns
        ],
        primaryKey=key_columns,
        exclusive=ExclusiveColumns(
            dataset2=list(columns_diff.get("added", [])),
            dataset1=list(columns_diff.get("removed", [])),
        ),
        typeChanged=list(columns_diff.get("changed", [])),
    )


def _map_kind(kind: ColType) -> ColumnKind:
    for raw_kind, json_kind in KIND_MAPPING:
        if isinstance(kind, raw_kind):
            return json_kind
    return ColumnKind.UNSUPPORTED
