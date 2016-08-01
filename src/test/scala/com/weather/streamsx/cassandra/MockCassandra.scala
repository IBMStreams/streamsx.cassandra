package com.weather.streamsx.cassandra

import java.net.InetAddress

import com.datastax.driver.core.{ConsistencyLevel, SSLOptions, AuthProvider}
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object MockCassandra {

//  EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

  val cassandra: CassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("abc.cql", "test"),
    EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);

  val session = cassandra.session


  val clientConfig = CassSinkClientConfig(
    localdc = "",
    port = 9042,
    remapClusterMinutes = 1000,
    writeOperationTimeout = 1000,
    authEnabled = false,
    authUsername = "",
    authPassword = "",
    sslEnabled = false,
    sslKeystore = "",
    sslPassword = "",
    dateFormat = DateTimeFormat.forPattern("yy-MM-dd HH:mm:ss"),
    authProvider = AuthProvider.NONE,
    sslOptions = None,
    consistencylevel = ConsistencyLevel.LOCAL_QUORUM,
    seeds = InetAddress.getAllByName("localhost").toList,
    keyspace = "test",
    table = "test",
    ttl = 10000,
    cacheSize = 1000
  )




}
