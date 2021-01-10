# ImmuSQL

Small demo using immuDB as storage for a relational database.

## Quickstart

- `docker network create immudbnet`
- `docker run -d --net immudbnet -it --rm --name immudb -p 3322:3322 codenotary/immudb:latest`
- `docker run -it --rm --net immudbnet --name immuclient codenotary/immuclient:latest -a immudb`
- `go run ./...`

Now you can execute some queries. Some examples:

- `CREATE TABLE test_table (id INTEGER PRIMARY KEY, description TEXT, d TIMESTAMP)`

Primary key is mandatory for this demo. Only INTEGER TEXT and TIMESTAMP types supported.

- `SHOW TABLES`
- `DESCRIBE test_table`
- Insert some data
```
INSERT INTO test_table VALUES (0,'text example 0',NOW())
INSERT INTO test_table VALUES (1,'text example 1',NOW())
INSERT INTO test_table VALUES (2,'text example 2',NULL)
```
- `SELECT * FROM test_table`

## Storage structure

We need some specific keys to save some metadata, like tables in a database and the schema for a specific table.

On the other hand, data is saved per row, to be able to work well on OLTP scenarios. 
We could store data per column with some caveats (we need to partition columns in different keys, and data update will be more costly).