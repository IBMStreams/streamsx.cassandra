#! /bin/bash

#localdc, authUsername, authPassword, sslKeystore, sslPassword omitted
streamtool mkappconfig \
--property consistencyLevel=local_quorum \
--property dateFormat=yy-MM-dd\ HH:mm:ss \
--property port=9042 \
--property remapClusterMinutes=15 \
--property seeds=10.0.2.2 \
--property writeOperationTimeout=10000 \
--property authEnabled=false \
--property sslEnabled=false \
--property keyspace=testkeyspace \
--property table=testtable \
--property ttl=2592000 \
--property cacheSize=1000 \
testCassandraConfiguration

streamtool mkappconfig \
--property count=0 \
--property nInt=-2147483647 \
testCassandraNullValues