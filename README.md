<p align="center">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="50%" />
</p>

# **data-diff**

## What is `data-diff`?
data-diff is a **free, open-source tool** that enables data professionals to detect differences in values between any two tables. It's fast, easy to use, and reliable. Even at massive scale.

## Use cases

### Between Databases: Quickly identify issues when moving data between databases

<p align="center">
  <img alt="diff1" src="https://user-images.githubusercontent.com/1799931/196479137-2b4744ea-464f-489e-8d01-6e8e54d62fba.png" />
  <img alt="diff2" src="https://user-images.githubusercontent.com/1799931/196565574-2a7f0efa-4820-4b1b-b010-62ed35fb105a.png" width="30%" />
</p>



### Within a Database: Improve code reviews by identifying data problems you don't have tests for
(video is rough draft, screenshot will be replaced)
<p align="center">
  <a href="https://www.loom.com/share/4ddda4625ae14abfae5d6f264412e50a" target="_blank">
    <img alt="Intro to Diff" src="https://user-images.githubusercontent.com/1799931/196011700-5ad867bb-2236-42f4-8462-34169164ce35.png" width="50%" height="50%" />
  </a>
</p>

&nbsp;
&nbsp;

## Get started

### Installation

#### First, install `data-diff` using `pip`.

```
pip install data-diff
```

**Note:** Once you've installed Python 3.7+, it's most likely that `pip` and `pip3` can be used interchangeably.

#### Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- We support 10+ other databases. Check out [our detailed documentation](https://www.datafold.com/docs/os_diff/how_to_use) for specifics.

### Run your first diff

Once you've installed `data-diff`, you can run it from the command line:

```
data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]
```

You can find all the correct syntax for your database setup in [our documentation](https://www.datafold.com/docs/os_diff/how_to_use).

Here's an example command for your copy/pasting, taken from the screenshot above:

```
data-diff \
  postgresql://leoebfolsom:'$PW_POSTGRES'@localhost:5432/diff_test \
  org_activity_stream \
  "snowflake://leo:$PW_SNOWFLAKE@BYA42734/analytics/ANALYTICS?warehouse=ANALYTICS&role=DATAFOLDROLE" \
  ORG_ACTIVITY_STREAM \
  -k activity_id \
  -c activity \
  -w "event_timestamp < '2022-10-10'"
```

That's just an example, but sure to check out the documentation for more details about [the options](https://www.datafold.com/docs/os_diff/how_to_use) you can use to create a command that's useful to you.


### We're here to help

We know, that `data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]` command can become long! And maybe you're new to the command line. We're here to help [on slack](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S) if you have ANY questions as you use `data-diff` in your workflow.

## Reporting bugs and contributing

- [Open an issue](https://github.com/datafold/data-diff/issues/new/choose) or chat with us [on slack](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S).
- Interested in contributing to this open source project? Please see our [Contributing Guideline](https://github.com/datafold/data-diff/blob/master/CONTRIBUTING.md)!
- Did we mention [we're hiring](https://www.datafold.com/careers)?

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
