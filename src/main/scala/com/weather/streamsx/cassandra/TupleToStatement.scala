package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.types.RString
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import collection.JavaConversions._
import com.ibm.streams.operator.meta.{MapType, CollectionType}
import scalaz.Failure


object TupleToStatement {

  def apply(args: SinkArgs, session: Session): BoundStatement = {
    val cache = new StatementCache(args, session)
    val m = mkNullValueMap(args.tuple, args.nullMapName)
    val nonNullAttrs = mkAttrList(args.tuple, m, args.nullMapName)
    mkBoundStatement(cache.get(m), nonNullAttrs, args.tuple)
  }

  private def mkNullValueMap(t: Tuple, nullMapName: String): Map[String, Boolean] = {
    // get the map of field names to booleans indicating whether or not they are null
    val rstringMap = t.getMap(nullMapName).asInstanceOf[java.util.Map[RString, Boolean]].toMap
    rstringMap.map(kv => (kv._1.toString, kv._2)) //convert from rstring to String
  }

  private def mkAttrList(t: Tuple, m: Map[String, Boolean], nullMapName: String) = {
    val schema = t.getStreamSchema
    val attributes = (0 until schema.getAttributeCount).map(i => schema.getAttribute(i)).filterNot(_.getName == nullMapName).sortBy(_.getName()).toList

    def ifSetMkAttr(m: Map[String, Boolean], a: Attribute): Option[Attr] = {
      if(m(a.getName)) Some(Attr(a, true))
      else None
    }

    attributes.flatMap(ifSetMkAttr(m, _))
  }

  private def mkBoundStatement(ps: PreparedStatement, nonNullAttrs: List[Attr], tuple: Tuple): BoundStatement = {
    val values: List[Any] = { nonNullAttrs.map(getValueFromTuple(tuple, _)) }
//    println(s"Values: $values")
    ps.bind(values.asInstanceOf[Seq[Object]]:_*)
  }

  def getValueFromTuple(tuple: Tuple, attr: Attr): Any = attr.typex.getLanguageType match {
    case "boolean" => tuple.getBoolean(attr.index)
    case "int8"  | "uint8" => tuple.getByte(attr.index)
    case "int16" | "uint16" => tuple.getShort(attr.index)
    case "int32" | "uint32" => tuple.getInt(attr.index)
    case "int64" | "uint64" => tuple.getLong(attr.index)
    case "float32" => tuple.getFloat(attr.index)
    case "float64" => tuple.getDouble(attr.index)
    case "decimal32" | "decimal64" | "decimal128" => tuple.getBigDecimal(attr.index)
    case "timestamp" => tuple.getTimestamp(attr.index)
    case "rstring" | "ustring" => tuple.getString(attr.index)
    case "blob" => tuple.getBlob(attr.index)
    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
    case l if l.startsWith("list") =>
      val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
      val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
      val rawList = tuple.getList(attr.index)
      castListToType[elementT.type](rawList)
    case s if s.startsWith("set") =>
      val setType: CollectionType = attr.typex.asInstanceOf[CollectionType]
      val elementT: Class[_] = setType.getElementType.getObjectType
      val rawSet = tuple.getSet(attr.index)
      castSetToType[elementT.type](rawSet)
    case m if m.startsWith("map") =>
      val mapType: MapType = attr.typex.asInstanceOf[MapType]
      val keyT: Class[_] = mapType.getKeyType.getObjectType
      val valT: Class[_] = mapType.getValueType.getObjectType
      val rawMap = tuple.getMap(attr.index)
      castMapToType[keyT.type, valT.type](rawMap)
    case _ => Failure(CassandraWriterException( s"Unrecognized type: ${attr.typex.getLanguageType}", new Exception))
  }

  def castListToType[A <: Any](rawList: java.util.List[_]): java.util.List[A] = {
    rawList.asInstanceOf[java.util.List[A]]
  }

  def castSetToType[A <: Any](rawSet: java.util.Set[_]): java.util.Set[A] = {
    rawSet.asInstanceOf[java.util.Set[A]]
  }

  def castMapToType[K <: Any, V <: Any](rawMap: java.util.Map[_,_]): java.util.Map[K,V] = {
    rawMap.asInstanceOf[java.util.Map[K,V]]
  }
}
