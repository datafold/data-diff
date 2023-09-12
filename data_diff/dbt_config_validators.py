from enum import Enum
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field


class ManifestJsonConfig(BaseModel):
    class Metadata(BaseModel):
        dbt_version: str = Field(..., regex=r"^\d+\.\d+\.\d+([a-zA-Z0-9]+)?$")
        project_id: str
        user_id: str

    class Nodes(BaseModel):
        class Config(BaseModel):
            database: Optional[str]
            schema_: Optional[str] = Field(..., alias="schema")
            tags: List[str]

        class Column(BaseModel):
            meta: Dict[str, Any]
            tags: List[str]

        class TestMetadata(BaseModel):
            name: str
            kwargs: Dict[str, Any]

        class DependsOn(BaseModel):
            macros: List[str] = []
            nodes: List[str] = []

        unique_id: str
        resource_type: str
        name: str
        alias: str
        database: str
        schema_: str = Field(..., alias="schema")
        columns: Optional[Dict[str, Column]]
        meta: Dict[str, Any]
        config: Config
        tags: List[str]
        test_metadata: Optional[TestMetadata]
        depends_on: DependsOn

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
