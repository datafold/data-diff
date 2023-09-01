from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field


class TDatadiffModelConfig(BaseModel):
    where_filter: Optional[str] = None
    include_columns: List[str] = []
    exclude_columns: List[str] = []


class TDatadiffConfig(BaseModel):
    prod_database: Optional[str] = None
    prod_schema: Optional[str] = None
    prod_custom_schema: Optional[str] = None
    datasource_id: Optional[int] = None


class ManifestJsonConfig(BaseModel):
    class Metadata(BaseModel):
        dbt_version: str = Field(..., regex=r"^\d+\.\d+\.\d+([a-zA-Z0-9]+)?$")
        project_id: str
        user_id: str

    class Nodes(BaseModel):
        class Config(BaseModel):
            database: Optional[str]
            schema_: Optional[str] = Field(..., alias="schema")
            tags: Optional[List[str]]

        class Column(BaseModel):
            meta: Dict[str, Any]
            tags: Optional[List[str]]

        class TestMetadata(BaseModel):
            name: str
            kwargs: Dict[str, Any]

        class DependsOn(BaseModel):
            macros: Optional[List[str]] = []
            nodes: Optional[List[str]] = []

        unique_id: str
        resource_type: str
        alias: Optional[str]
        database: Optional[str]
        schema_: Optional[str] = Field(..., alias="schema")
        columns: Optional[Dict[str, Column]]
        meta: Optional[Dict[str, Any]]
        config: Config
        tags: Optional[List[str]]
        test_metadata: Optional[TestMetadata]
        depends_on: Optional[DependsOn]
        name: str

    metadata: Metadata
    nodes: Dict[str, Nodes]


class RunResultsJsonConfig(BaseModel):
    class Metadata(BaseModel):
        dbt_version: str = Field(..., regex=r"^\d+\.\d+\.\d+([a-zA-Z0-9]+)?$")

    class Results(BaseModel):
        class Status(Enum):
            success = "success"
            error = "error"
            skipped = "skipped"
            pass_ = "pass"
            fail = "fail"
            warn = "warn"
            runtime_error = "runtime error"

        status: Status
        unique_id: str = Field("...")

    metadata: Metadata
    results: List[Results]
