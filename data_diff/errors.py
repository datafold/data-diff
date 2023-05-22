
class DbtProjectVarsNotFoundError(Exception):
    "Raised when an expected dbt_project.yml section is missing."

class DbtProfileNotFoundError(Exception):
    "Raised when an expected profiles.yml section is missing."

class DbtNoSuccessfulModelsInRunError(Exception):
    "Raised when there are no successful model runs in the run_results.json"

class DbtRunResultsVersionError(Exception):
    "Raised when the dbt version in run_results.json is lower than the minimum version."

class DbtSelectNoMatchingModelsError(Exception):
    "Raised when the `--select` flag returns no models."

class DbtSelectUnexpectedError(Exception):
    "Catch all for unexpected dbt list --select results."

class DbtSnowflakeSetConnectionError(Exception):
    "Raised when a dbt snowflake profile has unexpected values."

class DbtBigQueryOauthOnlyError(Exception):
    "Raised when trying to use a method other than oauth with BigQuery."

class DbtRedshiftPasswordOnlyError(Exception):
    "Raised when using a non-password connection method with Redshift."

class DbtConnectionNotImplementedError(Exception):
    "Raised when trying to use an unsupported dbt connection method."

class DbtCoreNoRunnerError(Exception):
    "Raised when the manifest version >= 1.5, but the dbt-core package is < 1.5. This is an edge case most likely to occur in development."

class DbtSelectVersionTooLowError(Exception):
    "Raised when attempting to use `--select` with a dbt-core version < 1.5."