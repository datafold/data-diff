<p align="center">
    <img alt="Datafold" src="https://user-images.githubusercontent.com/1799931/196497110-d3de1113-a97f-4322-b531-026d859b867a.png" width="50%" />
</p>

# **data-diff**

## What is `data-diff`?
data-diff is a **free, open-source tool** that enables data professionals to detect differences in values between any two tables. It's fast, easy to use, and reliable. Even at massive scale.

#### 💸 Join our team 
We're looking for developers with a deep understanding of databases and solid Python knowledge. [**Apply here!**](https://www.datafold.com/careers)

#### 📖 Documentation
Check out [our detailed documentation](https://www.datafold.com/docs) for instructions on how to use, common use cases, features, and technical details.

## How to use

### Quickly identify issues when migrating data between databases

<p align="center">
  <img alt="diff1" src="https://user-images.githubusercontent.com/1799931/196479137-2b4744ea-464f-489e-8d01-6e8e54d62fba.png" />
  <img alt="diff2" src="https://user-images.githubusercontent.com/1799931/196264192-4e3e79ae-4906-44ae-98a9-e4e51544bb85.png" />
</p>



### Improve code reviews by identifying data problems you don't have tests for
(video is rough draft, screenshot will be replaced with something better)
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

```pip install data-diff```

**Note:** Once you've installed Python 3.7+, it's most likely that `pip` and `pip3` can be used interchangeably.

#### Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'data-diff[postgresql]'`

- `pip install 'data-diff[snowflake]'`

- TODO We support 10+ other databases. Check out [TODO link to documentation] for specifics.

### Run your first diff

Once you've installed `data-diff`, you can run it from the command line:

`data-diff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]`

[TODO here's one example of code that you can copy and paste, just like from the screenshot]

Check out the [Documentation TODO add link](#) for all the options and database-specific configurations.

## Reporting bugs and contributing

- [Open an issue](https://github.com/datafold/data-diff/issues/new/choose) or chat with us [on slack](https://locallyoptimistic.slack.com/archives/C03HUNGQV0S).
- Interested in contributing to this open source project? Please see our [Contributing Guideline](https://github.com/datafold/data-diff/blob/master/CONTRIBUTING.md)!
- Did we mention [we're hiring](https://www.datafold.com/careers)?

## License

This project is licensed under the terms of the [MIT License](https://github.com/datafold/data-diff/blob/master/LICENSE).
