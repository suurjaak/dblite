CHANGELOG
=========

1.3.0, 2023-0-23
-----------------
- add attributes Database.ENGINE and Transaction.ENGINE

1.2.0, 2022-12-07
-----------------
- support data classes and objects in query arguments and results
- support dictionaries for column/group/order arguments
- support custom row factories
- make ORDER BY boolean value stand for ascending order instead of descending
- fix concurrent SQLite transactions not restoring connection isolation level
- return rows as OrderedDict in Py2 and dict in Py3 in all engines
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
