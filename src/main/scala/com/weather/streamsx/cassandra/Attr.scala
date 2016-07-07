package com.weather.streamsx.cassandra

import com.ibm.streams.operator.{Tuple, Attribute, Type}
import scalaz.Failure

case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr{
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)
  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))


//  val getValueFromTuple: (Tuple, Attr) => Any = (tuple, attr) => { attr.typex.getLanguageType match {
//    /*
//       There's no Java equivalent type for either complex32 or complex64.
//       See the table in the Streams documentation showing SPL to Java equivalent types for more info.
//    */
//    case "boolean" => tuple.getBoolean(attr.index)
//    case "int8" => tuple.getByte(attr.index)
//    case "int16" => tuple.getShort(attr.index)
//    case "int32" => tuple.getInt(attr.index)
//    case "int64" => tuple.getLong(attr.index)
//    case "uint8" => tuple.getByte(attr.index)
//    case "uint16" => tuple.getShort(attr.index)
//    case "uint32" => tuple.getInt(attr.index)
//    case "uint64" => tuple.getLong(attr.index)
//    case "float32" => tuple.getFloat(attr.index)
//    case "float64" => tuple.getDouble(attr.index)
//    case "decimal32" => tuple.getBigDecimal(attr.index)
//    case "decimal64" => tuple.getBigDecimal(attr.index)
//    case "decimal128" => tuple.getBigDecimal(attr.index)
//    case "timestamp" => tuple.getTimestamp(attr.index)
//    case "rstring" => tuple.getString(attr.index)
//    case "ustring" => tuple.getString(attr.index)
//    case "blob" => tuple.getBlob(attr.index)
//    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
//    case l if l.startsWith("list") =>
//      val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
//      val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
//    val rawList = tuple.getList(attr.index)
//      castListToType[elementT.type](rawList)
//    case s if s.startsWith("set") =>
//      val setType: CollectionType = attr.typex.asInstanceOf[CollectionType]
//      val elementT: Class[_] = setType.getElementType.getObjectType
//      val rawSet = tuple.getSet(attr.index)
//      castSetToType[elementT.type](rawSet)
//    case m if m.startsWith("map") =>
//      val mapType: MapType = attr.typex.asInstanceOf[MapType]
//      val keyT: Class[_] = mapType.getKeyType.getObjectType
//      val valT: Class[_] = mapType.getValueType.getObjectType
//      val rawMap = tuple.getMap(attr.index)
//      castMapToType[keyT.type, valT.type](rawMap)
//    case _ => Failure(CassandraWriterException( s"Unrecognized type: ${attr.typex.getLanguageType}", new Exception))
//  }
//
//  def castListToType[A <: Any](rawList: java.util.List[_]): java.util.List[A] = rawList.asInstanceOf[java.util.List[A]]
//
//  def castSetToType[A <: Any](rawSet: java.util.Set[_]): java.util.Set[A] = rawSet.asInstanceOf[java.util.Set[A]]
//
//  def castMapToType[K <: Any, V <: Any](rawMap: java.util.Map[_,_]): java.util.Map[K,V] = rawMap.asInstanceOf[java.util.Map[K,V]]
//
//}

}
