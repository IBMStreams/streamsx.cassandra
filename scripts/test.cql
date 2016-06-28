CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

drop table IF EXISTS testkeyspace.testtable;

create table IF NOT EXISTS testkeyspace.testtable (
  greeting varchar,
  count bigint,
  testList list<int>,
  PRIMARY KEY (greeting)
) with caching = 'none';