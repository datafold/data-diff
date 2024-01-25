from dataclasses import dataclass
from typing import Optional, Literal, Dict, Union


@dataclass
class CliOptions:
    bisection_factor: int
    bisection_threshold: int
    table_write_limit: int
    database1: Optional[str] = None
    table1: Optional[str] = None
    database2: Optional[str] = None
    table2: Optional[str] = None
    key_columns: tuple[str] = ()
    update_column: Optional[str] = None
    columns: tuple[str] = ()
    limit: Optional[int] = None
    materialize_to_table: Optional[str] = None
    min_age: Optional[str] = None
    max_age: Optional[str] = None
    stats: bool = False
    debug: bool = False
    json_output: bool = False
    verbose: bool = False
    version: bool = False
    interactive: bool = False
    no_tracking: bool = False
    case_sensitive: bool = False
    assume_unique_key: bool = False
    sample_exclusive_rows: bool = False
    materialize_all_rows: bool = False
    threads: Union[int, Literal["serial"]] = 1
    threads1: Optional[int] = None
    threads2: Optional[int] = None
    threaded: bool = False
    where: Optional[str] = None
    algorithm: Literal["auto", "joindiff", "hashdiff"] = None
    conf: Optional[str] = None
    run: Optional[str] = None
    dbt: bool = False
    cloud: bool = False
    dbt_profiles_dir: Optional[str] = None
    dbt_project_dir: Optional[str] = None
    select: Optional[str] = None
    state: Optional[str] = None
    prod_database: Optional[str] = None
    prod_schema: Optional[str] = None
    __conf__: Optional[Dict] = None
