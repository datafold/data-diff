# Technical explanation

data-diff can diff tables within the same database, or across different databases.

**Same-DB Diff:**
- Uses an outer-join to diff the rows as efficiently and accurately as possible.
- Supports materializing the diff results to a database table.
- Can also collect various extra statistics about the tables.

**Cross-DB Diff:** Employs a divide and conquer algorithm based on hashing, optimized for few changes.

The following is a technical explanation of the cross-db diff.

### Overview

data-diff splits the table into smaller segments, then checksums each segment in both databases. When the checksums for a segment aren't equal, it will further divide that segment into yet smaller segments, checksumming those until it gets to the differing row(s).

This approach has performance within an order of magnitude of count(*) when there are few/no changes, but is able to output each differing row! By pushing the compute into the databases, it's much faster than querying for and comparing every row.

![Performance for 100M rows](https://user-images.githubusercontent.com/97400/175182987-a3900d4e-c097-4732-a4e9-19a40fac8cdc.png)

**†:** The implementation for downloading all rows that `data-diff` and
`count(*)` is compared to is not optimal. It is a single Python multi-threaded
process. The performance is fairly driver-specific, e.g. PostgreSQL's performs 10x
better than MySQL.

### Deep Dive

In this section we'll be doing a walk-through of exactly how **data-diff**
works, and how to tune `--bisection-factor` and `--bisection-threshold`.

Let's consider a scenario with an `orders` table with 1M rows. Fivetran is
replicating it contionously from PostgreSQL to Snowflake:

```
┌─────────────┐                        ┌─────────────┐
│ PostgreSQL  │                        │  Snowflake  │
├─────────────┤                        ├─────────────┤
│             │                        │             │
│             │                        │             │
│             │  ┌─────────────┐       │ table with  │
│ table with  ├──┤ replication ├──────▶│ ?maybe? all │
│lots of rows!│  └─────────────┘       │  the same   │
│             │                        │    rows.    │
│             │                        │             │
│             │                        │             │
│             │                        │             │
└─────────────┘                        └─────────────┘
```

In order to check whether the two tables are the same, **data-diff** splits
the table into `--bisection-factor=10` segments.

We also have to choose which columns we want to checksum. In our case, we care
about the primary key, `--key-column=id` and the update column
`--update-column=updated_at`. `updated_at` is updated every time the row is, and
we have an index on it.

**data-diff** starts by querying both databases for the `min(id)` and `max(id)`
of the table. Then it splits the table into `--bisection-factor=10` segments of
`1M/10 = 100K` keys each:

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│      id=1..100k      │              │      id=1..100k      │
├──────────────────────┤              ├──────────────────────┤
│    id=100k..200k     │              │    id=100k..200k     │
├──────────────────────┤              ├──────────────────────┤
│    id=200k..300k     ├─────────────▶│    id=200k..300k     │
├──────────────────────┤              ├──────────────────────┤
│    id=300k..400k     │              │    id=300k..400k     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│      900k..100k      │              │      900k..100k      │
└───────────────────▲──┘              └▲─────────────────────┘
                    ┃                  ┃
                    ┃                  ┃
                    ┃ checksum queries ┃
                    ┃                  ┃
                  ┌─┻──────────────────┻────┐
                  │        data-diff        │
                  └─────────────────────────┘
```

Now **data-diff** will start running `--threads=1` queries in parallel that
checksum each segment. The queries for checksumming each segment will look
something like this, depending on the database:

```sql
SELECT count(*),
    sum(cast(conv(substring(md5(concat(cast(id as char), cast(timestamp as char))), 18), 16, 10) as unsigned))
FROM `rating_del1`
WHERE (id >= 1) AND (id < 100000)
```

This keeps the amount of data that has to be transferred between the databases
to a minimum, making it very performant! Additionally, if you have an index on
`updated_at` (highly recommended), then the query will be fast, as the database
only has to do a partial index scan between `id=1..100k`.

If you are not sure whether the queries are using an index, you can run it with
`--interactive`. This puts **data-diff** in interactive mode, where it shows an
`EXPLAIN` before executing each query, requiring confirmation to proceed.

After running the checksum queries on both sides, we see that all segments
are the same except `id=100k..200k`:

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│    checksum=0102     │              │    checksum=0102     │
├──────────────────────┤   mismatch!  ├──────────────────────┤
│    checksum=ffff     ◀──────────────▶    checksum=aaab    │
├──────────────────────┤              ├──────────────────────┤
│    checksum=abab     │              │    checksum=abab     │
├──────────────────────┤              ├──────────────────────┤
│    checksum=f0f0     │              │    checksum=f0f0     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│    checksum=9494     │              │    checksum=9494     │
└──────────────────────┘              └──────────────────────┘
```

Now **data-diff** will do exactly as it just did for the _whole table_ for only
this segment: Split it into `--bisection-factor` segments.

However, this time, because each segment has `100k/10=10k` entries, which is
less than the `--bisection-threshold`, it will pull down every row in the segment
and compare them in memory in **data-diff**.

```
┌──────────────────────┐              ┌──────────────────────┐
│     PostgreSQL       │              │      Snowflake       │
├──────────────────────┤              ├──────────────────────┤
│    id=100k..110k     │              │    id=100k..110k     │
├──────────────────────┤              ├──────────────────────┤
│    id=110k..120k     │              │    id=110k..120k     │
├──────────────────────┤              ├──────────────────────┤
│    id=120k..130k     │              │    id=120k..130k     │
├──────────────────────┤              ├──────────────────────┤
│    id=130k..140k     │              │    id=130k..140k     │
├──────────────────────┤              ├──────────────────────┤
│         ...          │              │         ...          │
├──────────────────────┤              ├──────────────────────┤
│      190k..200k      │              │      190k..200k      │
└──────────────────────┘              └──────────────────────┘
```

Finally **data-diff** will output the `(id, updated_at)` for each row that was different:

```
(122001, 1653672821)
```

If you pass `--stats` you'll see stats such as the % of rows were different.

### Performance Considerations

* Ensure that you have indexes on the columns you are comparing. Preferably a
  compound index. You can run with `--interactive` to see an `EXPLAIN` for the
  queries.
* Consider increasing the number of simultaneous threads executing
  queries per database with `--threads`. For databases that limit concurrency
  per query, such as PostgreSQL/MySQL, this can improve performance dramatically.
* If you are only interested in _whether_ something changed, pass `--limit 1`.
  This can be useful if changes are very rare. This is often faster than doing a
  `count(*)`, for the reason mentioned above.
* If the table is _very_ large, consider a larger `--bisection-factor`. Otherwise, you may run into timeouts.
* If there are a lot of changes, consider a larger `--bisection-threshold`.
* If there are very large gaps in your key column (e.g., 10s of millions of
  continuous rows missing), then **data-diff** may perform poorly, doing lots of
  queries for ranges of rows that do not exist. We have ideas on how to tackle this issue, which we have yet to implement. If you're experiencing this effect, please open an issue, and we
  will prioritize it.
* The fewer columns you verify (passed with `--columns`), the faster
  **data-diff** will be. On one extreme, you can verify every column; on the
  other, you can verify _only_ `updated_at`, if you trust it enough. You can also
  _only_ verify `id` if you're interested in only presence, such as to detect
  missing hard deletes. You can do also do a hybrid where you verify
  `updated_at` and the most critical value, such as a money value in `amount`, but
  not verify a large serialized column like `json_settings`.
* We have ideas for making **data-diff** even faster that
  we haven't implemented yet: faster checksums by reducing type-casts
  and using a faster hash than MD5, dynamic adaptation of
  `bisection_factor`/`threads`/`bisection_threshold` (especially with large key
  gaps), and improvements to bypass Python/driver performance limitations when
  comparing huge amounts of rows locally (i.e. for very high `bisection_threshold` values).
