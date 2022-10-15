<img width="207" alt="datafold" src="https://user-images.githubusercontent.com/1799931/195919667-b2b037a1-34d5-4bf3-a32f-0ac357b5a7da.png">

# **data-diff**

[Link to documentation!]

#### 💸💸 **Looking for paid contributors!** 💸💸
We're looking for developers with a deep understanding of databases and solid Python knowledge. [**Apply here!**]

----

## What is `data-diff`?
**data-diff** enables data professionals to detect differences in values between any two tables. It's fast, easy to use, and reliable. Even at massive scale.

## How to use

### Quickly identify issues when migrating data between databases
<img width="1044" alt="Screen Shot 2022-10-14 at 3 15 31 PM" src="https://user-images.githubusercontent.com/1799931/195951813-85e55304-b857-40e1-bd3c-2a12d4ae87c5.png">

### Improve code reviews by identifying data probelems you don't have tests for (i'm not sure this text is quite right)
(video is rough draft, screenshot should be replaced with something better)
<p align="center">
  <a href="https://www.loom.com/share/4ddda4625ae14abfae5d6f264412e50a" target="_blank">
    <img alt="Why Cypress Video" src="https://user-images.githubusercontent.com/1799931/196011700-5ad867bb-2236-42f4-8462-34169164ce35.png" width="50%" height="50%" />
  </a>
</p>

&nbsp;
&nbsp;

## Get started

### Installation

#### First, install `data-diff` using `pip`.

```pip install data-diff```

**Note:** Once you've installed Python 3.7+, it's most likely that `pip` and `pip3` can be used interchangeably.

#### Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- TODO We support 10+ other databases. Check out [TODO link to documentation] for specifics.

### Run your first diff

Once you've installed `data-diff`, you can run it from the command line:

`data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

Check out the [Documentation TODO add link](#) for all the options and database-specific configurations.

## Reporting bugs and contributing

- [Open an issue](https://github.com/datafold/data-diff/issues/new/choose) or chat with us [on slack](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S).
- Interested in contributing to this open source project? Please see our [Contributing Guideline](https://github.com/datafold/data-diff/blob/master/CONTRIBUTING.md)!

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
