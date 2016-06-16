package com.weather.streamsx.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.weather.streamsx.cassandra.config.CassandraConfig

object cassanalytics extends CassandraConfig("cassandra-analytics")


abstract class CassandraConnector(val ccfg: CassandraConfig) {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  protected val cluster = {
    val c = Cluster.builder()
      .addContactPoints(ccfg.seeds: _*)
      .withPort(ccfg.port)
      .withAuthProvider(ccfg.authProvider)
      .withSSL(ccfg.sslOptions.orNull)
    if (ccfg.localdc.isEmpty) c
    else c.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(ccfg.localdc).build()))
  }.build()

  val session = cluster.connect

  def shutdown() {
    try {
      session.close()
      cluster.close()
    } catch { case e: Throwable => log.error("Failed to shutdown cassandra{}", e) }
  }
}

object CasAnalyticsConnector extends CassandraConnector(cassanalytics) {
  def apply(): Unit = {}
}
