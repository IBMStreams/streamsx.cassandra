package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.ibm.streams.operator.Tuple
import com.weather.analytics.zooklient.ZooKlient
import com.weather.streamsx.cassandra.config.{NullValueConfig, CassSinkClientConfig, PrimitiveTypeConfig}
import com.weather.streamsx.cassandra.connection.{ZKClient, CassandraConnector, CassandraAwaiter}
import com.weather.streamsx.util.{StringifyStackTrace => SST}

object CassandraSinkImpl {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  def mkWriter(connectionConfigZNode: String, nullMapZNode: String): CassandraSinkImpl = {
    try {

      val zkCli: ZooKlient = ZKClient()
      val clientConfig = PrimitiveTypeConfig.read(zkCli, connectionConfigZNode) match {
        case Some(cc) => CassSinkClientConfig(cc)
        case _ => log.error(s"Failed to getData from $connectionConfigZNode"); null
      }
      val nullMapValues = NullValueConfig(zkCli, nullMapZNode)

      val cassConnector = new CassandraConnector(clientConfig)
      new CassandraSinkImpl(clientConfig, cassConnector, nullMapValues)
    } catch { case e: Exception => log.error("Failed to create SQS client", e); null }
  }
}



class CassandraSinkImpl(cfg: CassSinkClientConfig, connector: CassandraConnector, nullMapValues: Map[String, Any]) extends CassandraAwaiter{
  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = connector.writeOperationTimeout

  def insertTuple(tuple: Tuple): Unit = {
    try{
      val bs: BoundStatement = TupleToStatement(tuple, connector.session, cfg, nullMapValues)
        logFailure(awaitOne()(connector.session.executeAsync(bs)))
    } catch { case e: Throwable => log.error(s"Failed to write agg to Cassandra.\n${SST(e)}", e) }
  }
}
