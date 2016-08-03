package com.weather.streamsx.cassandra.connection

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.weather.streamsx.cassandra.config.CassSinkClientConfig

//TODO MODIFY SCALASTYLE TO NOT COMPLAIN ABOUT IF/ELSE BRACES

class CassandraConnector(ccfg: CassSinkClientConfig) {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  val writeOperationTimeout = ccfg.writeOperationTimeout

  protected val cluster = {
    val c = Cluster.builder()
      .addContactPoints(ccfg.seeds.split(","): _*)
      .withPort(ccfg.port)
      .withAuthProvider(ccfg.authProvider)
      .withSSL(ccfg.sslOptions.orNull)
    if (ccfg.localdc.isEmpty){ c }
    else {c.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc(ccfg.localdc).build()))}
  }.build()

  val session = cluster.connect

  def shutdown() {
    try {
      session.close()
      cluster.close()
    } catch { case e: Throwable => log.error("Failed to shutdown cassandra{}", e) }
  }
}
