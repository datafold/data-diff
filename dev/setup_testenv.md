From inside the `dev` directory, run the following:

1. Install XDiff

either `pip install xdiff` or 

2. Install Preql (0.2.9 or up)

`pip install preql -U`

3. Download CSV

```
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
```

4. Setup databases

```
preql -m prepare_db  postgres://<uri>

preql -m prepare_db  mysql://<uri>

preql -m prepare_db  snowflake://<uri>

etc.
```

And it's ready to use!

Example:

```bash
xdiff postgres:/// Rating postgres:/// Rating_del1p -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0

```