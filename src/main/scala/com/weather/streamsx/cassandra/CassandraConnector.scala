package com.weather.streamsx.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}

class CassandraConnector(ccfg: CassSinkClientConfig) {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val writeOperationTimeout = ccfg.writeOperationTimeout


  protected val cluster = {
    val c = Cluster.builder()
      .addContactPoints(ccfg.seeds: _*)
      .withPort(ccfg.port)
      .withAuthProvider(ccfg.authProvider)
      .withSSL(ccfg.sslOptions.orNull)
    if (ccfg.localDC.isEmpty) c
    else c.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(ccfg.localDC).build()))
  }.build()

  val session = cluster.connect

  def shutdown() {
    try {
      session.close()
      cluster.close()
    } catch { case e: Throwable => log.error("Failed to shutdown cassandra{}", e) }
  }
}

//object CasAnalyticsConnector extends CassandraConnector(cassanalytics) {
//  def apply(): Unit = {}
//}
