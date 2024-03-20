from typing import Optional, Literal, Dict, Union, Tuple

from pydantic import BaseModel, PositiveInt


class CliOptions(BaseModel):
    bisection_factor: PositiveInt
    bisection_threshold: PositiveInt
    table_write_limit: PositiveInt
    database1: Union[str, Dict, None] = None
    table1: Optional[str] = None
    database2: Union[str, Dict, None] = None
    table2: Optional[str] = None
    key_columns: Tuple[str, ...] = ()
    update_column: Optional[str] = None
    columns: Tuple[str, ...] = ()
    limit: Optional[PositiveInt] = None
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
    threads: Union[None, PositiveInt, Literal["serial"]] = None
    threads1: Optional[PositiveInt] = None
    threads2: Optional[PositiveInt] = None
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
    run_args: Optional[Dict] = None
