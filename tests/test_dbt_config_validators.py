from pathlib import Path
import unittest
import json

from data_diff.dbt_config_validators import (
    ManifestJsonConfig,
    RunResultsJsonConfig,
)


RUN_RESULTS_PATH = "tests/dbt_artifacts/run_results/"
MANIFEST_PATH = "tests/dbt_artifacts/manifests/"


class TestRunResultsJsonConfig(unittest.TestCase):
    def test_run_results(self):
        # https://docs.getdbt.com/reference/artifacts/run-results-json
        versions = ["v4", "v5"]

        for version in versions:
            with self.subTest(version=version):
                with open(Path(RUN_RESULTS_PATH, f"run_results_{version}.json"), "r", encoding="utf-8") as run_results:
                    RunResultsJsonConfig.parse_obj(json.load(run_results))


class TestManifestJsonConfig(unittest.TestCase):
    def test_manifest(self):
        # https://docs.getdbt.com/reference/artifacts/manifest-json
        versions = ["v4", "v5", "v6", "v7", "v8", "v9", "v10", "v11", "v11_no_tracking"]

        for version in versions:
            with self.subTest(version=version):
                with open(Path(MANIFEST_PATH, f"manifest_{version}.json"), "r", encoding="utf-8") as manifest:
                    ManifestJsonConfig.parse_obj(json.load(manifest))
