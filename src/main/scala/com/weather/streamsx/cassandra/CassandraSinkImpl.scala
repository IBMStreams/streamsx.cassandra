package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.exceptions.UnauthorizedException
import com.ibm.streams.operator.Tuple
import com.weather.analytics.zooklient.ZooKlient
import com.weather.streamsx.cassandra.config.{NullValueConfig, CassSinkClientConfig, PrimitiveTypeConfig}
import com.weather.streamsx.cassandra.connection.{ZKClient, CassandraConnector, CassandraAwaiter}
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.weather.streamsx.util.{StringifyStackTrace => SST}

object CassandraSinkImpl {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  // refactor to split up the zk stuff and cassandra stuff for 6-lining
  def mkWriter(connectionConfigZNode: String, nullMapZnode: String, zkConnectionString: String = ""): CassandraSinkImpl = {
    log.trace("Making the Cassandra writer operator")
    try {
      val connectStr: Option[String] = zkConnectionString match {
        case s: String if s.nonEmpty => Some(s)
        case _ => None
      }
      val zkCli: ZooKlient = ZKClient(connectStr = connectStr)
      val clientConfig = PrimitiveTypeConfig.read(zkCli, connectionConfigZNode) match {
        case Some(cc) => CassSinkClientConfig(cc)
        case _ => throw CassandraWriterException(s"Failed to getData from $connectionConfigZNode"); null
      }
      val cassConnector = new CassandraConnector(clientConfig)
      new CassandraSinkImpl(clientConfig, cassConnector, NullValueConfig(zkCli, nullMapZnode))
    } catch { case e: Exception => throw CassandraWriterException(s"Failed to create Cassandra client\n${SST(e)})", e); null }
  }
}

//TODO CHECK AND SEE IF EMPTY COLLECTIONS ARE TRUE NULLS OR TOMBSTONES

class CassandraSinkImpl(cfg: CassSinkClientConfig, connector: CassandraConnector, nullMapValues: Map[String, Any]) extends CassandraAwaiter{
  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = connector.writeOperationTimeout

  private var tbs: TupleBasedStructures = _
  private def init(tuple: Tuple): Unit = if (tbs == null) tbs = new TupleBasedStructures(tuple, connector.session, cfg)

  def insertTuple(tuple: Tuple): Unit = {
    log.trace("Inserting tuple...")
    init(tuple)

    //todo catch auth exceptions in logFailure
    try{
      val bs: BoundStatement = TupleToStatement(tuple, tbs, cfg, nullMapValues)
      logFailure(awaitOne()(connector.session.executeAsync(bs)))
      log.trace("Tuple inserted sucessfully!")
    } catch {
      case u: UnauthorizedException => throw u
      case e: Throwable => throw CassandraWriterException(s"Failed to write tuple to Cassandra. \n ${SST(e)}", e) }
  }

  def shutdown(): Unit = connector.shutdown()
}