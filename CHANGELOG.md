CHANGELOG
=========

1.2.0, 2022-11-30
-----------------
- support data classes and objects in query arguments and results
- return SQLite rows as OrderedDict in Py2
- log generated SQL at half DEBUG level

1.1.0, 2022-11-25
-----------------
- fix using Postgres server-side cursors
- allow specifying batch size for Postgres server-side cursors
- load database schema only on demand in Postgres
- provide Database.cursor and Transaction.cursor
- support any stringable column types in query parameters
- more tests

1.0.0, 2022-11-22
-----------------
- first release
