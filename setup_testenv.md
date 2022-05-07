# Test Data Diff with Postgres and MySQL


```
chmod +x ./dev/example.sh
./dev/example.sh
```

NB for Mac. If the process takes very long (e.g.  importing CSV file takes >30m), make sure that you have the latest version of Docker installed and have enabled the experimental features `Use the new Virtualization framework` and `Enable VirtioFS accelerated directory sharing`. Because the interaction with Docker and the MacOS FS is a bottleneck.

## Manual setup

1. Install Data Diff

```
poetry build --format wheel
pip install pip install "dist/data_diff-0.0.2-py3-none-any.whl[preql,mysql,pgsql]"
```

2. Download CSV

```
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
unzip ml-25m.zip -d dev/
```

4. Setup databases

(note: bigquery has its own setup script)

```
preql -f dev/prepare_db postgres://<uri>

preql -f dev/prepare_db mysql://<uri>

preql -f dev/prepare_db snowflake://<uri>

preql -f dev/prepare_db mssql://<uri>

preql -f dev/prepare_db_bigquery bigquery:///<project>


etc.
```

And it's ready to use!

Example:

```bash
data_diff postgres://user:password@host:db Rating mysql://user:password@host:db Rating_del1 -c timestamp --stats

Diff-Total: 250156 changed rows out of 25000095
Diff-Percent: 1.0006%
Diff-Split: +250156  -0

```

## Database settings with explanation
*Inline comments in docker-compose.yml will break the databases.* 

**PostgreSQL:**

```
-c work_mem=1GB                   # Reduce writing temporary disk files.
-c maintenance_work_mem=1GB       # Improve VACUUM, CREATE INDEX, ALTER TABLE ADD FOREIGN KEY operations.
-c max_wal_size=8GB               # Filling of the table with movie lens data creates an higher write  
                                  # load than the default assumption of 1GB/hour. 
```
**MySQL:**
```
--default-authentication-plugin=mysql_native_password  # Required for setting password via env vars.
--innodb-buffer-pool-size=8G                           # Recommendation is to set to 50-75% of available 
                                                       # memmory. However, this is no dedicated instance. 
--innodb_io_capacity=2000                              # Default setting is for hard drives. SSD benefits 
                                                       # from higher values.
--innodb_log_file_size=1G                              # Tuning recommendation based on the 
                                                       # innodb-buffer-pool-size setting.
--binlog-cache-size=16M                                # Tuning recommendation
--key_buffer_size=0                                    # No MyISAM tables, InnoDB engine is used.
--max_connections=10                                   # Test setup, not a lot connection needed.
--innodb_flush_log_at_trx_commit=2                     # Reduce creation of logs for performance.
--innodb_flush_log_at_timeout=10                       # Idem
--innodb_flush_method=O_DSYNC                          # Suffers less from race conditions than fsync.
--innodb_log_compressed_pages=OFF                      # To write less data to the redo_log.
--sync_binlog=0                                        # Disables synchronization of the binary log to disk 
                                                       # by the MySQL server. Instead, the MySQL server relies 
                                                       # on the operating system to flush the binary log to 
                                                       # disk from time to time as it does for any other file. 
                                                       # This setting provides the best performance.
```