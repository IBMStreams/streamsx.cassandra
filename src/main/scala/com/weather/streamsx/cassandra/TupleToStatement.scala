package com.weather.streamsx.cassandra


import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.Type
import com.ibm.streams.operator.meta.{CollectionType, MapType}
import com.ibm.streams.operator.types.RString
import collection.JavaConversions._
import scalaz._

case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr{
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)
  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))
}

object TupleToStatement {

  def apply(t: Tuple, session: Session, keyspace: String, table: String, ttl: Long, nullMapName: String): BoundStatement = {
    val schema = t.getStreamSchema
    val buffer = scala.collection.mutable.ListBuffer.empty[Attribute]
    for(i <- 0 until schema.getAttributeCount) {buffer += schema.getAttribute(i)}
    val attributes = buffer.sortBy(a => a.getIndex).toList

    // get the map of field names to booleans indicating whether or not they are null
    val rstringMap = t.getMap(nullMapName).asInstanceOf[java.util.Map[RString, Boolean]].toMap
    val m = rstringMap.map(kv => (kv._1.toString, kv._2))

    // remove the nullMap attribute from the fields
    val nonNullAttrs = attributes.filterNot(a => a.getName == nullMapName).map(a => Attr(a, m)).filter(a => a.set)

    val ps = mkPreparedStatement(nonNullAttrs, t, session, keyspace, table, ttl)
    mkBoundStatement(ps, nonNullAttrs, t)
  }

  def mkInsertStatement(fields: Seq[String], keyspace: String, table: String, ttl: Long) = {
    val fieldStr = fields.mkString(",")
    val q = ("?" * fields.length).mkString(",")
    val tableSpec = if (keyspace.isEmpty) table else s"$keyspace.$table"
    s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL $ttl"""
  }

  def mkPreparedStatement(nonNullAttrs: List[Attr], tuple: Tuple, session: Session, keyspace: String, table: String, ttl: Long): PreparedStatement = {
    val fields  = nonNullAttrs.map(a => a.name).toSeq
    //There should be a caching layer here
    val ps = session.prepare(mkInsertStatement(fields, keyspace, table, ttl))
    println(ps.getQueryString)
    println(fields)
    ps
  }

  def mkBoundStatement(ps: PreparedStatement, nonNullAttrs: List[Attr], tuple: Tuple): BoundStatement = {
    val values: List[Any] = { nonNullAttrs.map(a => getValueFromTuple(tuple, a)) }
    ps.bind(values.asInstanceOf[Seq[Object]]:_*)
  }

  def getValueFromTuple(tuple: Tuple, attr: Attr): Any = attr.typex.getLanguageType match {
    /*
       There's no Java equivalent type for either complex32 or complex64.
       See the table in the Streams documentation showing SPL to Java equivalent types for more info.
    */
    case "boolean" => tuple.getBoolean(attr.index)
    case "int8" => tuple.getByte(attr.index)
    case "int16" => tuple.getShort(attr.index)
    case "int32" => tuple.getInt(attr.index)
    case "int64" => tuple.getLong(attr.index)
    case "uint8" => tuple.getByte(attr.index)
    case "uint16" => tuple.getShort(attr.index)
    case "uint32" => tuple.getInt(attr.index)
    case "uint64" => tuple.getLong(attr.index)
    case "float32" => tuple.getFloat(attr.index)
    case "float64" => tuple.getDouble(attr.index)
    case "decimal32" => tuple.getBigDecimal(attr.index)
    case "decimal64" => tuple.getBigDecimal(attr.index)
    case "decimal128" => tuple.getBigDecimal(attr.index)
    case "timestamp" => tuple.getTimestamp(attr.index)
    case "rstring" => tuple.getString(attr.index)
    case "ustring" => tuple.getString(attr.index)
    case "blob" => tuple.getBlob(attr.index)
    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
    case l if l.startsWith("list") => {
      val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
      val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
      val rawList = tuple.getList(attr.index)
      castListToType[elementT.type](rawList)
    }
    case s if s.startsWith("set") => {
      val setType: CollectionType = attr.typex.asInstanceOf[CollectionType]
      val elementT: Class[_] = setType.getElementType.getObjectType
      val rawSet = tuple.getSet(attr.index)
      castSetToType[elementT.type](rawSet)
    }
    case m if m.startsWith("map") => {
      val mapType: MapType = attr.typex.asInstanceOf[MapType]
      val keyT: Class[_] = mapType.getKeyType.getObjectType
      val valT: Class[_] = mapType.getValueType.getObjectType
      val rawMap = tuple.getMap(attr.index)
      castMapToType[keyT.type, valT.type](rawMap)
    }
    case _ => Failure(CassandraWriterException( s"Unrecognized type: ${attr.typex.getLanguageType}", new Exception))
  }

  def castListToType[A <: Any](rawList: java.util.List[_]): java.util.List[A] = rawList.asInstanceOf[java.util.List[A]]

  def castSetToType[A <: Any](rawSet: java.util.Set[_]): java.util.Set[A] = rawSet.asInstanceOf[java.util.Set[A]]

  def castMapToType[K <: Any, V <: Any](rawMap: java.util.Map[_,_]): java.util.Map[K,V] = rawMap.asInstanceOf[java.util.Map[K,V]]
}
