CHANGELOG
=========

1.1.0, 2022-11-24
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
