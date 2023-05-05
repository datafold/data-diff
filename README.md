<p align="center">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="50%" />
</p>

<h1 align="center">
data-diff
</h1>

<h2 align="center">
Develop dbt models faster by testing as you code.
</h2>
<h4 align="center">
See how every change to dbt code affects the data produced in the modified model and downstream.
</h4>
<br>

## What is `data-diff`?

data-diff is an open source package that you can use to see the impact of your dbt code changes on your dbt models as you code.

<div align="center">

![development_testing_gif](https://user-images.githubusercontent.com/1799931/236354286-d1d044cf-2168-4128-8a21-8c8ca7fd494c.gif)

</div>

<br>

## Getting Started

**Install `data-diff`**
```
pip install data-diff
```

**Update a few lines in your `dbt_project.yml`**
```
#dbt_project.yml
vars:
  data_diff:
    prod_database: my_database
    prod_schema: my_default_schema
```

**Run your first data diff!**

```
dbt run && data-diff --dbt
```

We recommend you get started by walking through [our simple setup instructions](https://docs.datafold.com/development_testing/open_source) which contain examples and details.

Please reach out on the dbt Slack in [#tools-datafold](https://getdbt.slack.com/archives/C03D25A92UU) if you have any trouble whatsoever getting started!

<br><br>

### Diffing between databases

Check out our [documentation](https://github.com/datafold/data-diff/blob/master/docs/supported-databases.md) if you're looking to compare data across databases (for example, between Postgres and Snowflake).

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
