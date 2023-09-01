from pathlib import Path
import unittest

from data_diff.dbt_config_validators import (
    TDatadiffModelConfig,
    TDatadiffConfig,
    ManifestJsonConfig,
    RunResultsJsonConfig,
)


# TODO: add fixtures to dynamically test multiple manifest.json and run_results.json schema versions