# Test XDiff with Postgres and MySQL
From inside the `dev` directory, run the following:

```
chmod +x ./example.sh
./example.sh
```

NB for Mac. If the process takes very long (e.g.  importing CSV file takes >30m), make sure that you have the latest version of Docker installed and have enabled the experimental features `Use the new Virtualization framework` and `Enable VirtioFS accelerated directory sharing`. Because the interaction with Docker and the MacOS FS is a bottleneck.

## Manual setup

1. Install XDiff

`pip install xdiff -e ../`

2. Install Preql (0.2.9 or up)

`pip install preql -U`

3. Download CSV

```
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip
```

4. Setup databases

(note: bigquery and mssql have their own setup scripts)

```
preql -m prepare_db postgres://<uri>

preql -m prepare_db mysql://<uri>

preql -m prepare_db snowflake://<uri>

preql -m prepare_db_bigquery bigquery:///<project>

preql -m prepare_db_mssql mssql://<uri>


etc.
```

And it's ready to use!

Example:

```bash
xdiff postgres://<uri> Rating postgres://<uri> Rating_del1 -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0

```
