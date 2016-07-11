package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.ibm.streams.operator.Tuple
import com.weather.streamsx.cassandra.CasAnalyticsConnector.session
import com.weather.streamsx.util.{StringifyStackTrace => SST}


case class SinkArgs(
                     tuple: Tuple,
                     keyspace: String,
                     table: String,
                     ttl: Long,
                     nullMapName: String,
                     cacheSize: Int
                   )

object CassandraSinkImpl {
  def mkWriter(cfg: java.util.Map[String, String]): CassandraSinkImpl = {
    import scala.collection.JavaConverters._
    new CassandraSinkImpl(cfg.asScala.toMap)
  }
}

class CassandraSinkImpl(cfg: Map[String, String]) extends CassandraAwaiter{

  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = cfg.getOrElse("writeoperationtimeout", "10000").toLong

  def insertTuple(tuple: Tuple, keyspace: String, table: String, ttl: Long, nullMapName: String, cacheSize: Int): Unit = {

    val sinkArgs = SinkArgs(tuple, keyspace, table, ttl, nullMapName, cacheSize)

    try{
      val bs: BoundStatement = TupleToStatement(sinkArgs, session)
        logFailure(awaitOne()(session.executeAsync(bs)))
    } catch { case e: Throwable => log.error(s"Failed to write agg to Cassandra.\n${SST(e)}", e) }
  }
}
