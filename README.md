<p align="center">
    <a href="https://datafold.com/"><img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="30%" /></a>
</p>

<h2 align="center">
data-diff: Compare datasets fast, within or across SQL databases

![data-diff-logo](docs/data-diff-logo.png)
</h2>
<br>

# Use Cases

## Data Migration & Replication Testing
Compare source to target and check for discrepancies when moving data between systems:
- Migrating to a new data warehouse (e.g., Oracle > Snowflake)
- Converting SQL to a new transformation framework (e.g., stored procedures > dbt)
- Continuously replicating data from an OLTP DB to OLAP DWH (e.g., MySQL > Redshift)


## Data Development Testing
Test SQL code and preview changes by comparing development/staging environment data to production:
1. Make a change to some SQL code
2. Run the SQL code to create a new dataset
3. Compare the dataset with its production version or another iteration

  <p align="left">
  <img alt="dbt" src="https://seeklogo.com/images/D/dbt-logo-E4B0ED72A2-seeklogo.com.png" width="10%" />
  </p>

<details>
<summary> data-diff integrates with dbt Core to seamlessly compare local development to production datasets

 </summary>

![data-development-testing](docs/development_testing.png)

</details>

> [dbt Cloud users should check out Datafold's out-of-the-box deployment testing integration](https://www.datafold.com/data-deployment-testing)

:eyes: **Watch [4-min demo video](https://www.loom.com/share/ad3df969ba6b4298939efb2fbcc14cde)**

**[Get started with data-diff & dbt](https://docs.datafold.com/development_testing/open_source)**

Also available in a [VS Code Extension](https://marketplace.visualstudio.com/items?itemName=Datafold.datafold-vscode)

Reach out on the dbt Slack in [#tools-datafold](https://getdbt.slack.com/archives/C03D25A92UU) for advice and support


# How it works

When comparing the data, `data-diff` utilizes the resources of the underlying databases as much as possible. It has two primary modes of comparison:

## `joindiff`
- Recommended for comparing data within the same database
- Uses the outer join operation to diff the rows as efficiently as possible within the same database
- Fully relies on the underlying database engine for computation
- Requires both datasets to be queryable with a single SQL query
- Time complexity approximates JOIN operation and is largely independent of the number of differences in the dataset

## `hashdiff`
- Recommended for comparing datasets across different databases
- Can also be helpful in diffing very large tables with few expected differences within the same database
- Employs a divide-and-conquer algorithm based on hashing and binary search
- Can diff data across distinct database engines, e.g., PostgreSQL <> Snowflake
- Time complexity approximates COUNT(*) operation when there are few differences
- Performance degrades when datasets have a large number of differences

More information about the algorithm and performance considerations can be found [here](https://github.com/datafold/data-diff/blob/master/docs/technical-explanation.md)

# Get started

## Validating dbt model changes between dev and prod
⚡ Looking to use `data-diff` in dbt development? Head over to [our `data-diff` + `dbt` documentation](https://docs.datafold.com/development_testing/open_source/) to get started!

## Compare data tables between databases
🔀 To compare data between databases, install `data-diff` with specific database adapters, e.g.:

```
pip install data-diff 'data-diff[postgresql,snowflake]' -U
```

Run `data-diff` with connection URIs. In the following example, we compare tables between PostgreSQL and Snowflake using hashdiff algorithm:
```
data-diff \
  postgresql://<username>:'<password>'@localhost:5432/<database> \
  <table> \
  "snowflake://<username>:<password>@<password>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>" \
  <TABLE> \
  -k <primary key column> \
  -c <columns to compare> \
  -w <filter condition>
```

Check out [documentation](https://docs.datafold.com/reference/open_source/cli) for the full command reference.


# Supported databases


| Database      | Status | Connection string                                                                                                                   |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| PostgreSQL >=10 |  🟢   | `postgresql://<user>:<password>@<host>:5432/<database>`                                                                             |
| MySQL         |  🟢     | `mysql://<user>:<password>@<hostname>:5432/<database>`                                                                              |
| Snowflake     |  🟢     | `"snowflake://<user>[:<password>]@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<role>[&authenticator=externalbrowser]"` |
| BigQuery      |  🟢     | `bigquery://<project>/<dataset>`                                                                                                    |
| Redshift      |  🟢     | `redshift://<username>:<password>@<hostname>:5439/<database>`                                                                       |
| Oracle        |  🟡   | `oracle://<username>:<password>@<hostname>/servive_or_sid`                                                                          |
| Presto        |  🟡   | `presto://<username>:<password>@<hostname>:8080/<database>`                                                                         |
| Databricks    |  🟡   | `databricks://<http_path>:<access_token>@<server_hostname>/<catalog>/<schema>`                                                      |
| Trino         |  🟡   | `trino://<username>:<password>@<hostname>:8080/<database>`                                                                          |
| Clickhouse    |  🟡   | `clickhouse://<username>:<password>@<hostname>:9000/<database>`                                                                     |
| Vertica       |  🟡   | `vertica://<username>:<password>@<hostname>:5433/<database>`                                                                        |
| DuckDB        |  🟡   |                                                                                                                                     |
| ElasticSearch |  📝    |                                                                                                                                     |
| Planetscale   |  📝    |                                                                                                                                     |
| Pinot         |  📝    |                                                                                                                                     |
| Druid         |  📝    |                                                                                                                                     |
| Kafka         |  📝    |                                                                                                                                     |
| SQLite        |  📝    |                                                                                                                                     |

* 🟢: Implemented and thoroughly tested.
* 🟡: Implemented, but not thoroughly tested yet.
* ⏳: Implementation in progress.
* 📝: Implementation planned. Contributions welcome.

Your database not listed here?

- Contribute a [new database adapter](https://github.com/datafold/data-diff/blob/master/docs/new-database-driver-guide.rst) – we accept pull requests!
- [Get in touch](https://www.datafold.com/demo) about enterprise support and adding new adapters and features


<br>

## Contributors

We thank everyone who contributed so far!

<a href="https://github.com/datafold/data-diff/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=datafold/data-diff" />
</a>

<br>

## Analytics

* [Usage Analytics & Data Privacy](https://github.com/datafold/data-diff/blob/master/docs/usage_analytics.md)

<br>

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
