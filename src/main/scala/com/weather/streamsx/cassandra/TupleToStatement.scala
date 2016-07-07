package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.types.RString
import collection.JavaConversions._
import com.ibm.streams.operator.meta.{MapType, CollectionType}


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



  def getValueFromTuple(tuple: Tuple, attr: Attr): Any = attr.typex.getLanguageType match {
    case "boolean" => tuple.getBoolean(attr.index)
    //    case "enum" =>
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
    //    case "complex32" => There's no Java equivalent type for either complex32 or complex64.
    //    case "complex64" => See the table in the Streams documentation showing SPL to Java equivalent types for more info.
    case "timestamp" => tuple.getTimestamp(attr.index)
    case "rstring" => tuple.getString(attr.index)
    case "ustring" => tuple.getString(attr.index)
    case "blob" => tuple.getBlob(attr.index)
    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
    case l if l.startsWith("list") => {
      val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
      val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
      //    println(s"the languageType is ${listType.getLanguageType}") // This is the SPL type, such as list<int32>
      //    val compositeType = listType.getAsCompositeElementType // This is the collection type, java.util.List in this case
      //    println(s"THE ELEMENT TYPE IS: ${elementT.getName} AND THE COMPOSITE CLASS TYPE IS ${compositeType.getName}")
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
    case _ => s"APPARENTLY I DUNNO WTF THIS TYPE IS: ${attr.typex.getLanguageType}"
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
