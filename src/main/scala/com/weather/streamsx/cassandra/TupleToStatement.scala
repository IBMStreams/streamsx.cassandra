package com.weather.streamsx.cassandra


import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.Type
import com.ibm.streams.operator.meta.{CollectionType, MapType}
import com.ibm.streams.operator.types.RString
import collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}

case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr{
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)
  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))
}


//TODO: get keyspace and table as parameters in the operator. Gonna define some temps here for the meantime.

object TupleToStatement {

//  val keyspaceTEMP = "keyspace"
//  val tableTEMP = "table"
//  val ttlTEMP: Long = 604800 //one week,

  def apply(t: Tuple, session: Session, keyspace: String, table: String, ttl: Long, nullMapName: String): BoundStatement = {
    val schema = t.getStreamSchema


    val buffer = scala.collection.mutable.ListBuffer.empty[Attribute]
    for(i <- 0 until schema.getAttributeCount) {buffer += schema.getAttribute(i)}

    // this is going to be useful for handling general collection maps, but if getting the nullMap as Map[String, Boolean] fails,
    // then so much the better

//    val maptype: MapType = schema.getAttribute(nullMapName).getType.asInstanceOf[MapType]
//
//    val keyT: Type = maptype.getKeyType
//    val valT: Type = maptype.getValueType
//
//    keyT.getObjectType


    val mizzap = t.getMap(nullMapName).asInstanceOf[java.util.Map[RString, Boolean]]
    val rstringMap = mizzap.toMap
    val m = rstringMap.map(kv => (kv._1.toString, kv._2))

    val attributes = buffer.sortBy(a => a.getIndex).toList

    // remove the nullMap attribute from this list
    val fields: List[Attr] = attributes.filterNot(a => a.getName == nullMapName).map(a => Attr(a, m))
    val nonNullAttrs = fields.filter(a => a.set)
    val ps = mkPreparedStatement(nonNullAttrs, t, session, keyspace, table, ttl)

    val bs = getBoundStatement(ps, nonNullAttrs, t)
    println(bs.toString)
    bs
//    null
  }

  def mkInsert(fields: Seq[String], keyspace: String, table: String, ttl: Long) = {
    val fieldStr = fields.mkString(",")
    val q = ("?" * fields.length).mkString(",")
    val tableSpec = if (keyspace.isEmpty) table else s"$keyspace.$table"
    s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL $ttl"""
  }

  def mkPreparedStatement(nonNullAttrs: List[Attr], tuple: Tuple, session: Session, keyspace: String, table: String, ttl: Long): PreparedStatement = {
    val fields  = nonNullAttrs.map(a => a.name).toSeq
    //There should be a caching layer here
    val ps = session.prepare(mkInsert(fields, keyspace, table, ttl))
    println(ps.getQueryString)
    println(fields)
    ps
  }

  def getBoundStatement(ps: PreparedStatement, nonNullAttrs: List[Attr], tuple: Tuple): BoundStatement = {
    val values: List[Any] = { nonNullAttrs.map(a => getValueFromTuple(tuple, a)) }
    println(values)
    val zero = values(0)
    val one = values(1)

    def f[T](v: T) = v match {
      case _: Long    => "Long"
      case _: String => "String"
      case _         => "Unknown"
    }
    println(s"value of greeting is $zero which is type ${f(zero)}")
    println(s"value of count    is $one which is type ${f(one)}")
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

  def castSetToType[A <: Any](rawSet: java.util.Set[_]): Set[A] = {
    rawSet.asInstanceOf[java.util.Set[A]].toSet
  }

  def castMapToType[K <: Any, V <: Any](rawMap: java.util.Map[_,_]): Map[K,V] = {
    rawMap.asInstanceOf[java.util.Map[K,V]].toMap
  }


}
