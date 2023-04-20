# Install / Get started

Sqeleton can be installed using pip:

```
pip install sqeleton
```

## Database drivers

To ensure that the database drivers are compatible with sqeleton, we recommend installing them along with sqeleton, using pip's `[]` syntax:

- `pip install 'sqeleton[mysql]'`

- `pip install 'sqeleton[postgresql]'`

- `pip install 'sqeleton[snowflake]'`

- `pip install 'sqeleton[presto]'`

- `pip install 'sqeleton[oracle]'`

- `pip install 'sqeleton[trino]'`

- `pip install 'sqeleton[clickhouse]'`

- `pip install 'sqeleton[vertica]'`

- For BigQuery, see: https://pypi.org/project/google-cloud-bigquery/

_Some drivers have dependencies that cannot be installed using `pip` and still need to be installed manually._


It is also possible to install several databases at once. For example:

```bash
pip install 'sqeleton[mysql, postgresql]'
```

Note: Some shells use `"` for escaping instead, like:

```bash
pip install "sqeleton[mysql, postgresql]"
```

## What's next?

Read the [introduction](intro.md) and start coding!
