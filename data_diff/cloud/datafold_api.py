import dataclasses
import enum
from typing import Any, Dict, List, Optional

import pydantic
import requests


class TestDataSourceStatus(str, enum.Enum):
    SUCCESS = "ok"
    FAILED = "error"
    SKIP = "skip"
    UNKNOWN = "unknown"


class TCloudApiDataSourceSchema(pydantic.BaseModel):
    title: str
    properties: Dict[str, Dict[str, Any]]
    required: List[str]
    secret: List[str]


class TCloudApiDataSourceConfigSchema(pydantic.BaseModel):
    name: str
    db_type: str
    config_schema: TCloudApiDataSourceSchema


class TCloudApiDataSource(pydantic.BaseModel):
    id: Optional[int] = None
    name: str
    type: str
    is_paused: Optional[bool] = False
    hidden: Optional[bool] = False
    temp_schema: Optional[str] = None
    disable_schema_indexing: Optional[bool] = False
    disable_profiling: Optional[bool] = False
    catalog_include_list: Optional[str] = None
    catalog_exclude_list: Optional[str] = None
    schema_indexing_schedule: Optional[str] = None
    schema_max_age_s: Optional[int] = None
    profile_schedule: Optional[str] = None
    profile_exclude_list: Optional[str] = None
    profile_include_list: Optional[str] = None
    discourage_manual_profiling: Optional[bool] = False
    lineage_schedule: Optional[str] = None
    float_tolerance: Optional[float] = 0.0
    options: Optional[Dict[str, Any]] = None
    queue_name: Optional[str] = None
    scheduled_queue_name: Optional[str] = None
    groups: Optional[Dict[int, bool]] = None
    view_only: Optional[bool] = False
    created_from: Optional[str] = None
    source: Optional[str] = None
    max_allowed_connections: Optional[int] = None
    last_test: Optional[Any] = None
    secret_id: Optional[int] = None


class TDsConfig(pydantic.BaseModel):
    name: str
    type: str
    temp_schema: str
    float_tolerance: float = 0.0
    options: Dict[str, Any]
    disable_schema_indexing: bool = True
    disable_profiling: bool = True


class TCloudApiDataDiff(pydantic.BaseModel):
    data_source1_id: int
    data_source2_id: int
    table1: List[str]
    table2: List[str]
    pk_columns: List[str]


class TCloudDataSourceTestResult(pydantic.BaseModel):
    status: TestDataSourceStatus
    message: str
    outcome: str


class TCloudApiDataSourceTestResult(pydantic.BaseModel):
    name: str
    status: str
    result: TCloudDataSourceTestResult


@dataclasses.dataclass
class DatafoldAPI:
    api_key: str
    host: str = "https://app.datafold.com"
    timeout: int = 30

    def __post_init__(self):
        self.host = self.host.rstrip("/")
        self.headers = {
            "Authorization": f"Key {self.api_key}",
            "Content-Type": "application/json",
        }

    def make_get_request(self, url: str) -> Any:
        rv = requests.get(url=f"{self.host}/{url}", headers=self.headers, timeout=self.timeout)
        rv.raise_for_status()
        return rv

    def make_post_request(self, url: str, payload: Any) -> Any:
        rv = requests.post(url=f"{self.host}/{url}", headers=self.headers, json=payload, timeout=self.timeout)
        rv.raise_for_status()
        return rv

    def get_data_sources(self) -> List[TCloudApiDataSource]:
        rv = self.make_get_request(url="api/data_sources")
        rv.raise_for_status()
        return [TCloudApiDataSource(**item) for item in rv.json()]

    def create_data_source(self, config: TDsConfig) -> TCloudApiDataSource:
        # TODO: replace an internal url by a public one
        rv = self.make_post_request(url="api/internal/data_sources", payload=config.dict())
        return TCloudApiDataSource(**rv.json())

    def get_data_source_schema_config(self) -> List[TCloudApiDataSourceConfigSchema]:
        # TODO: replace an internal url by a public one
        rv = self.make_get_request(url="api/internal/data_sources/types")
        return [
            TCloudApiDataSourceConfigSchema(
                name=item["name"],
                db_type=item["type"],
                config_schema=TCloudApiDataSourceSchema(
                    title=item["configuration_schema"]["title"],
                    properties=item["configuration_schema"]["properties"],
                    required=item["configuration_schema"]["required"],
                    secret=item["configuration_schema"]["secret"],
                ),
            )
            for item in rv.json()
        ]

    def create_data_diff(self, payload: TCloudApiDataDiff) -> int:
        rv = self.make_post_request(url="api/v1/datadiffs", payload=payload.dict())
        return rv.json()["id"]

    def test_data_source(self, data_source_id: int) -> int:
        # TODO: replace an internal url by a public one
        rv = self.make_post_request(f"api/internal/data_sources/{data_source_id}/test", {})
        return rv.json()["job_id"]

    def check_data_source_test_results(self, job_id: int) -> List[TCloudApiDataSourceTestResult]:
        # TODO: replace an internal url by a public one
        rv = self.make_get_request(f"api/internal/data_sources/test/{job_id}")
        return [
            TCloudApiDataSourceTestResult(
                name=item["step"],
                status=item["status"],
                result=TCloudDataSourceTestResult(
                    status=item["result"]["code"].lower(),
                    message=item["result"]["message"],
                    outcome=item["result"]["outcome"],
                ),
            )
            for item in rv.json()["results"]
        ]
