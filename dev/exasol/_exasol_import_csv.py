# This is a Python script, used for setting up a exasol database for developement and testing
#
# In loads a "rating" dataset and generates a set of tables from it, with various modifications.

import pyexasol

C = pyexasol.connect(dsn="127.0.0.1:8563", user="sys", password="exasol")

schema = "DATADIFF"

print("Cleanup...")
C.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
C.commit()

print("Create schema...")
C.execute(f"CREATE SCHEMA {schema}")

print("Create tmp_rating table...")
C.execute(
    f'CREATE TABLE {schema}.tmp_rating ("userid" DECIMAL(18,0), "movieid" DECIMAL(18,0), "rating" DOUBLE PRECISION, "timestamp" DECIMAL(18,0))'
)

print("Import csv from s3 into tmp_rating table...")
C.import_from_file(
    table=(schema, "tmp_rating"),
    src="dev/ratings.csv",
    import_params={"skip": 1, "column_separator": ",", "row_separator": "CRLF"},
)

print("Create rating table from tmp_rating...")
C.execute(
    f'SELECT ROW_NUMBER() OVER (order by "userid", "movieid", "timestamp") AS "id", "userid", "movieid", "rating", "timestamp"  INTO TABLE {schema}."rating" FROM {schema}.tmp_rating'
)

print("Drop tmp_rating tabel...")
C.execute(f"DROP TABLE {schema}.tmp_rating")
C.commit()

print("Create modification tables...")
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_del1" FROM {schema}."rating"'
)
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_update1" FROM {schema}."rating"'
)
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_update001p" FROM {schema}."rating"'
)
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_update1p" FROM {schema}."rating"'
)
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_del1p" FROM {schema}."rating"'
)
C.execute(
    f'SELECT "id", "userid", "movieid", "rating", "timestamp" INTO TABLE {schema}."rating_update50p" FROM {schema}."rating"'
)

count = C.execute(f'SELECT COUNT(*) FROM {schema}."rating"').fetchval()
middle = count // 2

print("Alter tables...")
C.execute(f'DELETE FROM {schema}."rating_del1" WHERE "id"={ middle +1 };')
assert (
    C.execute(f'SELECT COUNT(*) FROM {schema}."rating"').fetchval()
    == C.execute(f'SELECT COUNT(*) FROM {schema}."rating_del1"').fetchval() + 1
)

C.execute(f'UPDATE {schema}."rating_update1" SET "timestamp"="timestamp"+1 WHERE "id"={ middle + 1 };')
assert (
    C.execute(f'SELECT "timestamp" FROM {schema}."rating" WHERE "id" = { middle + 1 }').fetchval()
    == C.execute(f'SELECT "timestamp" FROM {schema}."rating_update1" WHERE "id" = { middle + 1 }').fetchval() - 1
)

C.execute(f'UPDATE {schema}."rating_update50p" SET "timestamp"="timestamp"+1 WHERE MOD("id",2)=0;')
C.execute(f'UPDATE {schema}."rating_update1p" SET "timestamp"="timestamp"+1 WHERE MOD("id",100)=0;')
C.execute(f'UPDATE {schema}."rating_update001p" SET "timestamp"="timestamp"+1 WHERE MOD("id",10000)=0;')
C.execute(f'DELETE FROM {schema}."rating_del1p" WHERE MOD("id",100)=0;')
assert (
    C.execute(f'SELECT COUNT(*) FROM {schema}."rating"').fetchval() * 0.99
    == C.execute(f'SELECT COUNT(*) FROM {schema}."rating_del1p"').fetchval()
)

C.commit()
C.close()
print("...finished setup.")
