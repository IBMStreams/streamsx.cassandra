#!/usr/bin/env bash
echo "Setting up Cassandra"
/opt/cassandra-cassandra-2.1.11/bin/cqlsh 127.0.0.1 9042 -f test.cql
echo "Done setting up Cassandra"