package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import com.ibm.streams.operator.Tuple
import com.weather.streamsx.util.{StringifyStackTrace => SST}


case class SinkArgs(
                     tuple: Tuple,
                     keyspace: String,
                     table: String,
                     ttl: Long,
                     nullMapName: String,
                     cacheSize: Int,
                     cfgZnode: String
                   )

object CassandraSinkImpl {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def mkWriter(znodeName: String): CassandraSinkImpl = {
    try CassSinkClientConfig.read(znodeName) match {
      case Some(cc) => new CassandraSinkImpl(new CassandraConnector(cc))
      case _ => log.error(s"Failed to getData from $znodeName"); null
    } catch { case e: Exception => log.error("Failed to create SQS client", e); null }
  }

}

class CassandraSinkImpl(connector: CassandraConnector) extends CassandraAwaiter{

  override protected val log = org.slf4j.LoggerFactory.getLogger(getClass)
  override protected val writeOperationTimeout = connector.writeOperationTimeout

  def insertTuple(tuple: Tuple, keyspace: String, table: String, ttl: Long, nullMapName: String, cacheSize: Int, cfgZnode: String): Unit = {
    val sinkArgs = SinkArgs(tuple, keyspace, table, ttl, nullMapName, cacheSize, cfgZnode)
    try{
      val bs: BoundStatement = TupleToStatement(sinkArgs, connector.session)
        logFailure(awaitOne()(connector.session.executeAsync(bs)))
    } catch { case e: Throwable => log.error(s"Failed to write agg to Cassandra.\n${SST(e)}", e) }
  }
}
