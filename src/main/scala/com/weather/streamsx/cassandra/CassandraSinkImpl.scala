package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import org.apache.logging.log4j.Logger
import com.ibm.streams.operator.StreamingInput
import com.ibm.streams.operator.Tuple
import com.weather.streamsx.cassandra.CasAnalyticsConnector.session
import com.weather.streamsx.util.{StringifyStackTrace => SST}

object CassandraSinkImpl {
  def mkWriter(cfg: java.util.Map[String, String]): CassandraSinkImpl = {
    import scala.collection.JavaConverters._
    new CassandraSinkImpl(cfg.asScala.toMap)
  }
}

class CassandraSinkImpl(cfg: Map[String, String]) extends CassandraAwaiter{

  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = cfg.getOrElse("writeoperationtimeout", "10000").toLong

  def insertTuple(stream: StreamingInput[Tuple], tuple: Tuple, keyspace: String, table: String, ttl: Long, nullMapName: String): Unit = {
    try{
      val bs: BoundStatement = TupleToStatement(tuple, session, keyspace, table, ttl, nullMapName)
        logFailure(awaitOne()(session.executeAsync(bs)))
    } catch { case e: Throwable => log.error(s"Failed to write agg to Cassandra.\n${SST(e)}", e) }
  }
}
