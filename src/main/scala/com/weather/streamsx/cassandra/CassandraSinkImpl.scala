package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.ibm.streams.operator.Tuple
import com.weather.analytics.zooklient.ZooKlient
import com.weather.streamsx.cassandra.config.{NullValueConfig, CassSinkClientConfig, PrimitiveTypeConfig}
import com.weather.streamsx.cassandra.connection.{ZKClient, CassandraConnector, CassandraAwaiter}
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.weather.streamsx.util.{StringifyStackTrace => SST}

object CassandraSinkImpl {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def mkWriter(connectionConfigZNode: String, nullMapZnode: String, zkConnectionString: String = ""): CassandraSinkImpl = {
    log.trace("Making the Cassandra writer operator")
    try {
      val connectStr: Option[String] = zkConnectionString match {
        case "" => None
        case s: String => Some(s)
        case _ => None
      }
      val zkCli: ZooKlient = ZKClient(connectStr = connectStr)
      val clientConfig = PrimitiveTypeConfig.read(zkCli, connectionConfigZNode) match {
        case Some(cc) => CassSinkClientConfig(cc)
        case _ => throw new CassandraWriterException(s"Failed to getData from $connectionConfigZNode", new Exception); null
      }
      val nullMapValues = NullValueConfig(zkCli, nullMapZnode) match {
        case Some(map) => map
        case _ => Map[String, Any]()
      }
      val cassConnector = new CassandraConnector(clientConfig)
      new CassandraSinkImpl(clientConfig, cassConnector, nullMapValues)
    } catch { case e: Exception => throw new CassandraWriterException(s"Failed to create Cassandra client\n${SST(e)})", e); null }
  }
}

//TODO CHECK AND SEE IF EMPTY COLLECTIONS ARE TRUE NULLS OR TOMBSTONES

class CassandraSinkImpl(cfg: CassSinkClientConfig, connector: CassandraConnector, nullMapValues: Map[String, Any]) extends CassandraAwaiter{
  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = connector.writeOperationTimeout

  private var tbs: Option[TupleBasedStructures] = None

  def insertTuple(tuple: Tuple): Unit = {
    log.trace("Inserting tuple...")

    tbs match {
      case None => tbs = Some(new TupleBasedStructures(tuple, connector.session, cfg))
      case _ => ()
    }
    
    try{
      val bs: BoundStatement = TupleToStatement(tuple, tbs.get, cfg, nullMapValues)
      logFailure(awaitOne()(connector.session.executeAsync(bs)))
      log.trace("Tuple inserted sucessfully!")
    } catch { case e: Throwable => throw new CassandraWriterException(s"Failed to write tuple to Cassandra. \n ${SST(e)}", e) }
  }

  def shutdown(): Unit = {
    connector.shutdown()
  }
}
