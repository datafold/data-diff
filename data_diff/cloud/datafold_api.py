import base64
import enum
import time
from typing import Any, Dict, List, Optional, Type, Tuple

import attrs
import pydantic
import requests
from typing_extensions import Self

from data_diff.errors import DataDiffCloudDiffFailed, DataDiffCloudDiffTimedOut, DataDiffDatasourceIdNotFoundError
from data_diff.utils import getLogger

logger = getLogger(__name__)


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

    @classmethod
    def from_orm(cls, obj: Any) -> Self:
        data_source_types_required_parameters = {
            "bigquery": ["projectId", "jsonKeyFile", "location"],
            "databricks": ["host", "http_password", "database", "http_path"],
            "mysql": ["host", "user", "passwd", "db"],
            "pg": ["host", "user", "port", "password", "dbname"],
            "postgres_aurora": ["host", "user", "port", "password", "dbname"],
            "postgres_aws_rds": ["host", "user", "port", "password", "dbname"],
            "redshift": ["host", "user", "port", "password", "dbname"],
            "snowflake": ["account", "user", "password", "warehouse", "role", "default_db"],
        }

        return cls(
            title=obj["configuration_schema"]["title"],
            properties=obj["configuration_schema"]["properties"],
            required=data_source_types_required_parameters[obj["type"]],
            secret=obj["configuration_schema"]["secret"],
        )


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
    filter1: Optional[str] = None
    filter2: Optional[str] = None
    include_columns: Optional[List[str]]
    exclude_columns: Optional[List[str]]


class TCloudApiOrgMeta(pydantic.BaseModel):
    org_id: int
    org_name: str
    user_id: int


class TSummaryResultPrimaryKeyStats(pydantic.BaseModel):
    total_rows: Tuple[int, int]
    nulls: Tuple[int, int]
    dupes: Tuple[int, int]
    exclusives: Tuple[int, int]
    distincts: Tuple[int, int]


class TSummaryResultColumnDiffStats(pydantic.BaseModel):
    column_name: str
    match: float


class TSummaryResultValueStats(pydantic.BaseModel):
    total_rows: int
    rows_with_differences: int
    total_values: int
    compared_columns: int
    columns_with_differences: int
    columns_diff_stats: List[TSummaryResultColumnDiffStats]


class TSummaryResultSchemaStats(pydantic.BaseModel):
    columns_mismatched: Tuple[int, int]
    column_type_mismatches: int
    column_reorders: int
    column_counts: Tuple[int, int]
    column_type_differs: List[str]
    exclusive_columns: Tuple[List[str], List[str]]


class TCloudApiDataDiffSummaryResult(pydantic.BaseModel):
    status: str
    pks: Optional[TSummaryResultPrimaryKeyStats]
    values: Optional[TSummaryResultValueStats]
    schema_: Optional[TSummaryResultSchemaStats]
    dependencies: Optional[Dict[str, Any]]

    @classmethod
    def from_orm(cls, obj: Any) -> Self:
        pks = TSummaryResultPrimaryKeyStats(**obj["pks"]) if "pks" in obj else None
        values = TSummaryResultValueStats(**obj["values"]) if "values" in obj else None
        deps = obj["deps"] if "deps" in obj else None
        schema = TSummaryResultSchemaStats(**obj["schema"]) if "schema" in obj else None
        return cls(
            status=obj["status"],
            pks=pks,
            values=values,
            schema_=schema,
            deps=deps,
        )


class TCloudDataSourceTestResult(pydantic.BaseModel):
    status: TestDataSourceStatus
    message: str
    outcome: str


class TCloudApiDataSourceTestResult(pydantic.BaseModel):
    name: str
    status: str
    result: Optional[TCloudDataSourceTestResult]


@attrs.define(frozen=False)
class DatafoldAPI:
    api_key: str
    headers: str = ""
    host: str = "https://app.datafold.com"
    timeout: int = 30

    def __attrs_post_init__(self):
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
        rv = self.make_get_request(url="api/v1/data_sources")
        rv.raise_for_status()
        return [TCloudApiDataSource(**item) for item in rv.json()]

    def get_data_source(self, data_source_id: int) -> TCloudApiDataSource:
        rv = self.make_get_request(url=f"api/v1/data_sources")
        rv.raise_for_status()
        response_json = rv.json()
        datasource = next((datasource for datasource in response_json if datasource["id"] == data_source_id), None)
        if not datasource:
            raise DataDiffDatasourceIdNotFoundError(
                f"Datasource ID: {data_source_id} was not found in your Datafold account!"
            )
        return TCloudApiDataSource(**datasource)

    def create_data_source(self, config: TDsConfig) -> TCloudApiDataSource:
        payload = config.dict()
        if config.type == "bigquery":
            json_string = payload["options"]["jsonKeyFile"].encode("utf-8")
            payload["options"]["jsonKeyFile"] = base64.b64encode(json_string).decode("utf-8")
        rv = self.make_post_request(url="api/v1/data_sources", payload=payload)
        return TCloudApiDataSource(**rv.json())

    def get_data_source_schema_config(
        self,
        only_important_properties: bool = False,
    ) -> List[TCloudApiDataSourceConfigSchema]:
        rv = self.make_get_request(url="api/v1/data_sources/types")
        return [
            TCloudApiDataSourceConfigSchema(
                name=item["name"],
                db_type=item["type"],
                config_schema=TCloudApiDataSourceSchema.from_orm(obj=item),
            )
            for item in rv.json()
        ]

    def create_data_diff(self, payload: TCloudApiDataDiff) -> int:
        rv = self.make_post_request(url="api/v1/datadiffs", payload=payload.dict())
        return rv.json()["id"]

    def poll_data_diff_results(self, diff_id: int) -> TCloudApiDataDiffSummaryResult:
        summary_results = None
        start_time = time.monotonic()
        sleep_interval = 3
        max_sleep_interval = 20
        max_wait_time = 300

        diff_url = f"{self.host}/datadiffs/{diff_id}/overview"
        while not summary_results:
            logger.debug("Polling Datafold for results...")
            response = self.make_get_request(url=f"api/v1/datadiffs/{diff_id}/summary_results")
            response_json = response.json()
            if response_json["status"] == "success":
                summary_results = response_json
            elif response_json["status"] == "failed":
                raise DataDiffCloudDiffFailed(f"Diff failed: {str(response_json)}")

            if time.monotonic() - start_time > max_wait_time:
                raise DataDiffCloudDiffTimedOut(
                    f"Timed out waiting for diff results. Please, go to the UI for details: {diff_url}"
                )

            time.sleep(sleep_interval)
            sleep_interval = min(sleep_interval + 1, max_sleep_interval)

        return TCloudApiDataDiffSummaryResult.from_orm(summary_results)

    def test_data_source(self, data_source_id: int) -> int:
        rv = self.make_post_request(f"api/v1/data_sources/{data_source_id}/test", {})
        return rv.json()["job_id"]

    def check_data_source_test_results(self, job_id: int) -> List[TCloudApiDataSourceTestResult]:
        rv = self.make_get_request(f"api/v1/data_sources/test/{job_id}")
        return [
            TCloudApiDataSourceTestResult(
                name=item["step"],
                status=item["status"],
                result=TCloudDataSourceTestResult(
                    status=item["result"]["code"].lower(),
                    message=item["result"]["message"],
                    outcome=item["result"]["outcome"],
                )
                if item["result"] is not None
                else None,
            )
            for item in rv.json()["results"]
        ]

    def get_org_meta(self) -> TCloudApiOrgMeta:
        response = self.make_get_request(f"api/v1/organization/meta")
        response_json = response.json()
        return TCloudApiOrgMeta(
            org_id=response_json["org_id"], org_name=response_json["org_name"], user_id=response_json["user_id"]
        )
