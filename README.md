<p align="left">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="30%" />
</p>

<h1 align="left">
data-diff: compare datasets fast, within or across SQL databases
</h1>

<br>


# Use cases

## Data Migration & Replication Testing
Compare source to target and check for discrepancies when moving data between systems:
- Migrating to a new data warehouse (e.g., Oracle > Snowflake)
- Converting SQL to a new transformation framework (e.g., stored procedures > dbt)
- Continuously replicating data from an OLTP DB to OLAP DWH (e.g., MySQL > Redshift)


Install `data-diff` with specific database adapters, e.g.:

```
pip install data-diff 'data-diff[postgresql,snowflake	]' -U
```
Run `data-diff` with connection URIs to compare tables:
```
data-diff \
  postgresql://<username>:'<password>'@localhost:5432/<database> \
  <table> \
  "snowflake://<username>:<password>@<password>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>" \
  <TABLE> \
  -k activity_id \
  -c activity \
  -w "event_timestamp < '2022-10-10'"
```
Check out [documentation](https://docs.datafold.com/reference/open_source/cli) for full command reference.

## Data Development Testing
Test SQL code and preview changes by comparing development/staging environment data to production:
1. Make a change to some SQL code
2. Run the SQL code to create a new dataset
3. Compare the dataset with its production version or another iteration

  <p align="left">
  <img alt="dbt" src="https://seeklogo.com/images/D/dbt-logo-E4B0ED72A2-seeklogo.com.png" width="10%" />
  </p>
  
`data-diff` integrates with dbt Core and dbt Cloud to seamlessly compare local development to production datasets. 

:eyes: **Watch [4-min demo video](https://www.loom.com/share/ad3df969ba6b4298939efb2fbcc14cde)**

**[Get started with data-diff & dbt](https://docs.datafold.com/development_testing/open_source)**

Reach out on the dbt Slack in [#tools-datafold](https://getdbt.slack.com/archives/C03D25A92UU) for advice and support

## Supported databases

- PostgreSQL >=10
- MySQL
- Snowflake
- BigQuery
- Redshift
- Oracle
- Presto
- Databricks
- Trino
- Clickhouse
- Vertica
- DuckDB >=0.6
- SQLite (coming soon)


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
