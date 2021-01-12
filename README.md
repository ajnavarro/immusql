# ImmuSQL

Small demo using immuDB as storage for a relational database.

## Quickstart


- Download an execute immudb binary: `./immudb-v0.9.0-linux-amd64-static`
- Run `go run ./...`

Now you can execute some queries. Some examples:

- 
```
CREATE TABLE authors (id INTEGER PRIMARY KEY,first_name TEXT,last_name TEXT,email TEXT, birthdate TIMESTAMP, added TIMESTAMP DEFAULT now());
```

Primary key is mandatory for this demo. Only INTEGER, TEXT, and TIMESTAMP types are supported.

- `SHOW TABLES;`
- `DESCRIBE authors;`
- Insert some data
```
INSERT INTO `authors` VALUES (0,'Frederique','Hermann','ycruickshank@example.org','1970-12-15 11:50:22','2008-10-17 21:40:31'),(2,'Jayde','Smith','hayes.clara@example.com','1993-01-03 00:44:39','1992-08-30 08:35:42'),(3,'Cassie','Bruen','mnikolaus@example.org','1990-01-08 18:49:13','2007-10-28 03:19:13'),(5,'Daisha','Goyette','emmerich.mathilde@example.org','1996-10-05 17:10:43','1977-07-04 00:32:01'),(7,'Chaz','Goyette','sking@example.net','1987-01-20 08:06:48','1981-07-12 11:43:55'),(12,'Velma','Eichmann','manley93@example.org','1985-05-04 03:32:00','1983-07-17 19:42:57'),(42,'Marilyne','Gottlieb','schiller.winona@example.com','2017-12-18 19:02:29','2007-03-24 14:54:14'),(55,'Daryl','Greenfelder','jessyca.hoppe@example.com','1999-03-05 22:47:11','2014-06-10 01:39:54'),(621,'Julianne','Huels','cruz39@example.net','2014-12-15 02:59:20','1986-08-01 02:29:12'),(674,'Geo','Turner','melisa.ryan@example.net','2018-06-14 10:00:04','1988-12-21 11:24:32'),(1629,'Helga','Doyle','gkuhlman@example.net','1989-07-19 11:04:29','1996-03-23 15:36:09'),(55008,'Timmothy','Murphy','christopher.ruecker@example.net','1983-05-31 18:37:37','1995-07-31 00:04:01'),(68299,'Anne','Langworth','janick.green@example.com','2006-12-23 23:05:31','2003-06-16 16:38:35'),(91382,'Rowena','Quigley','uchamplin@example.com','2005-02-13 10:58:18','2006-01-26 05:51:08'),(315153,'Sage','Rau','zdach@example.com','2000-11-02 01:17:49','1995-08-25 19:10:34'),(736648,'Kiera','Kessler','anika18@example.com','2016-01-26 07:14:52','1986-03-18 14:32:00'),(876226,'Glenda','Walker','aurore62@example.org','1973-04-15 05:47:05','2013-03-14 06:10:12'),(881126,'Riley','Von','balistreri.kaylin@example.org','2005-09-16 04:33:55','1973-08-21 10:58:40'),(7690090,'Maddison','Schinner','langworth.jannie@example.com','2002-12-29 06:29:59','1990-05-08 22:18:00'),(36911734,'Baylee','Rodriguez','marjory.greenholt@example.net','1993-11-20 00:06:18','1978-07-06 15:43:23'),(56026529,'Raleigh','Kshlerin','lydia.terry@example.com','2009-06-13 13:15:45','1970-12-12 15:57:34'),(63037433,'Icie','Wilderman','christa.cronin@example.com','2007-12-30 10:27:45','1979-12-25 15:01:25'),(95841523,'Michelle','Collier','heller.burley@example.com','2010-06-29 17:58:50','2000-12-29 05:17:24'),(123088367,'Cicero','Keeling','qhyatt@example.com','1979-03-13 15:18:25','1981-02-17 13:52:23'),(500268653,'Clark','Corwin','greynolds@example.net','1978-09-10 14:08:08','1973-03-16 13:54:56');

```
- `SELECT * FROM authors`
- `SELECT count(*) from authors`
- Get all people born in January: `SELECT * FROM authors where MONTH(birthdate) = 1`
- `SELECT * FROM authors where first_name REGEXP '^A.*'`
## Storage structure
How to store data is the most difficult part in my opinion. You need to think in a versionable, fast, simple format for data and metadata.

This implementation is really simple, and it is storing the needed data using the following keys:

### Keys

#### tm/[database_name]/[table_name]

Here we are saving the schema for a specific table.

####  dbm/[database_name]

Here we have a list of tables that belong to `[database_name]` database.

#### td/[database_name]/[table_name]/[partition_key]

Here we are saving each row for the specific table for the specific database. The partition key in that demo is the primary key value, but it can be any list of columns defined by `partition by (column_names)`. If no partition key is defined, and there is no primary key, the partition key will be the row content hashed.

### Values

Values in that demo are just stored as JSON for simplicity. 

As a better approach, the format used to store data and metadata should be:
- Versionable: All serialized data should contain a version number to make immusql be able to read old formats.
- Space: Avoid serializing unnecessary data or identifiers.
- Fast: It should be fast to read and write. It should be able to be partially read.

## Other formats 

Data saved per row is really good for OLTP scenarios. It makes possible fast and parallelizable reads, writes, and modifications.

For analytics use cases we could save data per column instead. This makes inserts and modifications more complicated and slow, but it can speed up full scans for a specific column.
Because will be really inefficient just to save all the columns for a table in just one immudb value, we should distribute keys among several shards, distributing keys between several shards. We can use any hash-ring algorithm to do that. This has a lot of potential, making it possible to improve column filtering lookouts (you know exactly which shard you should retrieve to check if a specific column value exists, per example).

We could implement both table formats to cover most of the use cases. The user could define the format to use when creating the table (`CREATE TABLE test_table (id INTEGER PRIMARY KEY, description TEXT) ENGINE=col_engine`).