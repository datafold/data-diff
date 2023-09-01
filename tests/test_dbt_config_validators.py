from pathlib import Path
import unittest
import json

from data_diff.dbt_config_validators import (
    TDatadiffModelConfig,
    TDatadiffConfig,
    ManifestJsonConfig,
    RunResultsJsonConfig,
)


RUN_RESULTS_PATH = "tests/dbt_artifacts/run_results/"
MANIFEST_PATH = "tests/dbt_artifacts/manifests/"


class TestRunResultsJsonConfig(unittest.TestCase):

    def test_run_results(self):
        versions = ["v1", "v2", "v3", "v4"]
        for version in versions:
            with self.subTest(version=version):
                with open(Path(RUN_RESULTS_PATH, f"run_results_{version}.json"), encoding='utf-8') as run_results:
                    run_results_dict = json.load(run_results)
                RunResultsJsonConfig.parse_obj(run_results_dict)


class TestManifestJsonConfig(unittest.TestCase):

    def test_manifest(self):
        versions = ["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"]
        for version in versions:
            with self.subTest(version=version):
                with open(Path(MANIFEST_PATH, f"manifest_{version}.json"), encoding='utf-8') as manifest:
                    manifest_dict = json.load(manifest)
                ManifestJsonConfig.parse_obj(manifest_dict)
