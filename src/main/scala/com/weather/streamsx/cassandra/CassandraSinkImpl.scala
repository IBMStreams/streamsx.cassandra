package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.datastax.driver.core.exceptions.UnauthorizedException
import com.ibm.streams.operator.Tuple
import com.weather.streamsx.cassandra.config.{NullValueConfig, CassSinkClientConfig, PrimitiveTypeConfig}
import com.weather.streamsx.cassandra.connection.{CassandraConnector, CassandraAwaiter}
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.weather.streamsx.cassandra.util.{StringifyStackTrace => SST}
import scala.collection.JavaConverters._

object CassandraSinkImpl {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  // refactor to split up the zk stuff and cassandra stuff for 6-lining
  def mkWriter(connectionConfigMap: java.util.Map[String, String], nullMap: java.util.Map[String, String]): CassandraSinkImpl = {
    log.trace("Making the Cassandra writer operator")
    try {
      val ptc = PrimitiveTypeConfig(connectionConfigMap.asScala.toMap)
      val clientConfig = CassSinkClientConfig(ptc)
      val cassConnector = new CassandraConnector(clientConfig)

      if(nullMap == null) new CassandraSinkImpl(clientConfig, cassConnector, Map())
      else new CassandraSinkImpl(clientConfig, cassConnector, nullMap.asScala.toMap)

    } catch { case e: Exception => throw CassandraWriterException(s"Failed to create Cassandra client\n${SST(e)})", e); null }
  }
}

//TODO CHECK AND SEE IF EMPTY COLLECTIONS ARE TRUE NULLS OR TOMBSTONES

class CassandraSinkImpl(cfg: CassSinkClientConfig, connector: CassandraConnector, nullMapStrValues: Map[String, String]) extends CassandraAwaiter{
  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = connector.writeOperationTimeout

  private var tbs: TupleBasedStructures = _
  private def init(tuple: Tuple): Unit = if (tbs == null) tbs = new TupleBasedStructures(tuple, connector.session, cfg, nullMapStrValues)

  def insertTuple(tuple: Tuple): Unit = {
    log.trace("Inserting tuple...")
    init(tuple)

    //todo catch auth exceptions in logFailure
    try{
      val bs: BoundStatement = TupleToStatement(tuple, tbs, cfg)
      logFailure(awaitOne()(connector.session.executeAsync(bs)))
      log.trace("Tuple inserted sucessfully!")
    } catch {
      case u: UnauthorizedException => throw u
      case e: Throwable => throw CassandraWriterException(s"Failed to write tuple to Cassandra. \n ${SST(e)}", e) }
  }

  def shutdown(): Unit = connector.shutdown()
}