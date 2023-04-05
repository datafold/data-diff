# List of supported databases

| Database      | Status | Connection string |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------|--------|
| PostgreSQL >=10 |  💚    | `postgresql://<user>:<password>@<host>:5432/<database>`                                                                        |
| MySQL         |  💚    | `mysql://<user>:<password>@<hostname>:5432/<database>`                                                                              |
| Snowflake     |  💚    | `"snowflake://<user>[:<password>]@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<role>[&authenticator=externalbrowser]"` |
| BigQuery      |  💚    | `bigquery://<project>/<dataset>`                                                                                                    |
| Redshift      |  💚    | `redshift://<username>:<password>@<hostname>:5439/<database>`                                                                       |
| Oracle        |  💛    | `oracle://<username>:<password>@<hostname>/database`                                                                                |
| Presto        |  💛    | `presto://<username>:<password>@<hostname>:8080/<database>`                                                                         |
| Databricks    |  💛    | `databricks://<http_path>:<access_token>@<server_hostname>/<catalog>/<schema>`                                                      |
| Trino         |  💛    | `trino://<username>:<password>@<hostname>:8080/<database>`                                                                          |
| Clickhouse    |  💛    | `clickhouse://<username>:<password>@<hostname>:9000/<database>`                                                                     |
| Vertica       |  💛    | `vertica://<username>:<password>@<hostname>:5433/<database>`                                                                        |
| DuckDB        |  💛    |                                                                                                                                     |
| ElasticSearch |  📝    |                                                                                                                                     |
| Planetscale   |  📝    |                                                                                                                                     |
| Pinot         |  📝    |                                                                                                                                     |
| Druid         |  📝    |                                                                                                                                     |
| Kafka         |  📝    |                                                                                                                                     |
| SQLite        |  📝    |                                                                                                                                     |

* 💚: Implemented and thoroughly tested.
* 💛: Implemented, but not thoroughly tested yet.
* ⏳: Implementation in progress.
* 📝: Implementation planned. Contributions welcome.

Is your database not listed here? We accept pull-requests!
