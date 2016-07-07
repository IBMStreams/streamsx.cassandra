package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.types.RString
import collection.JavaConversions._
import Attr.getValueFromTuple


object TupleToStatement {

  def apply(t: Tuple, session: Session, keyspace: String, table: String, ttl: Long, nullMapName: String): BoundStatement = {
    val cache = new StatementCache(table, keyspace, ttl, session)
    val m = mkNullValueMap(t, nullMapName)
    val nonNullAttrs = mkAttrList(t, m, nullMapName)
    mkBoundStatement(cache.get(m), nonNullAttrs, t)
  }

  private def mkNullValueMap(t: Tuple, nullMapName: String): Map[String, Boolean] = {
    // get the map of field names to booleans indicating whether or not they are null
    val rstringMap = t.getMap(nullMapName).asInstanceOf[java.util.Map[RString, Boolean]].toMap
    rstringMap.map(kv => (kv._1.toString, kv._2)) //convert from rstring to String
  }

  private def mkAttrList(t: Tuple, m: Map[String, Boolean], nullMapName: String) = {
    val schema = t.getStreamSchema
    val buffer = scala.collection.mutable.ListBuffer.empty[Attribute]
    for(i <- 0 until schema.getAttributeCount) {buffer += schema.getAttribute(i)}
    val attributes = buffer.sortBy(a => a.getIndex).toList
    attributes.filterNot(a => a.getName == nullMapName).map(a => Attr(a, m)).filter(a => a.set).sortBy(a => a.name)
  }

  private def mkBoundStatement(ps: PreparedStatement, nonNullAttrs: List[Attr], tuple: Tuple): BoundStatement = {
    val values: List[Any] = { nonNullAttrs.map(a => getValueFromTuple(tuple, a)) }
    println(s"Values: $values")
    ps.bind(values.asInstanceOf[Seq[Object]]:_*)
  }
}
