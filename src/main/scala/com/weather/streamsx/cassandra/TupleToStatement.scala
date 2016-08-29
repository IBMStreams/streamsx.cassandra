package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.ibm.streams.operator.meta.{MapType, CollectionType}
import scala.collection.immutable.BitSet
import scalaz.Failure

class TupleBasedStructures(t: Tuple, session: Session, cfg: CassSinkClientConfig) {
  val attributeList = TupleToStatement.mkAttrList(t)
  val indexMap = new DualHash(attributeList)
  val cache = new StatementCache(cfg, session, indexMap)
}

object TupleToStatement {

  //TODO: Reach out to Senthil about unit testing for Streams Java operators

  def apply(tuple: Tuple, tbs: TupleBasedStructures, cassCfg: CassSinkClientConfig, nullValueMap: Map[String, Any]): BoundStatement = {
    val attributeList = mkAttrList(tuple)
    val valuesMap: Map[String, Any] = attributeList.map(getValueFromTuple(tuple, _)).toMap
    mkBoundStatement(valuesMap, nullValueMap, tbs)
  }

  private[cassandra] def mkBoundStatement(
                                           valuesMap: Map[String, Any],
                                           nullValueMap: Map[String, Any],
                                           tbs: TupleBasedStructures
                                         ): BoundStatement = {
    val (bitSet, nonNulls) = mkBitSet(valuesMap, nullValueMap, tbs.indexMap)
    val ps = tbs.cache(bitSet)
    val bindingValues = nonNulls.map(kv => kv._2)
    ps.bind(bindingValues.asInstanceOf[Seq[Object]]:_*)
  }

  private def mkBitSet(rawValues: Map[String, Any], nullValues: Map[String, Any], indexMap: DualHashy): (BitSet, List[(String, Any)]) = {
    def filterNulls(kv: (String, Any)): Option[(String, Any)] = (nullValues.get(kv._1), rawValues(kv._1)) match {
      case (Some(kk), v) if v == kk => None
      case _ => Some(kv)
    }
    val nonNulls: List[(String, Any)] = rawValues.flatMap(filterNulls).toList.sortBy(kv => indexMap(kv._1))
    val bitList = nonNulls.flatMap(kv => indexMap(kv._1))
    (BitSet(bitList:_*), nonNulls)
  }

  // TODO: see if this can be converted to use the iterator
  def mkAttrList(t: Tuple): List[Attribute] = {
    val schema = t.getStreamSchema
    (0 until schema.getAttributeCount).map(schema.getAttribute).sortBy(_.getName).toList
  }

  def getValueFromTuple(tuple: Tuple, attr: Attribute): (String, Any) = {
    val value: Any = attr.getType.getLanguageType match {
      case "boolean" => tuple.getBoolean(attr.getIndex)
      case "int8"  | "uint8" => tuple.getByte(attr.getIndex).toInt   //TODO C* 3.0+ has support for different data types for byte and short
      case "int16" | "uint16" => tuple.getShort(attr.getIndex).toInt //TODO C* 3.0+ has support for different data types for byte and short
      case "int32" | "uint32" => tuple.getInt(attr.getIndex)
      case "int64" | "uint64" => tuple.getLong(attr.getIndex).asInstanceOf[java.math.BigInteger]
      case "float32" => tuple.getFloat(attr.getIndex)
      case "float64" => tuple.getDouble(attr.getIndex)
      case "decimal32" | "decimal64" | "decimal128" => tuple.getBigDecimal(attr.getIndex)
      case "timestamp" => tuple.getTimestamp(attr.getIndex)
      case "rstring" | "ustring" => tuple.getString(attr.getIndex)
      case "blob" => tuple.getBlob(attr.getIndex)
      case "xml" => tuple.getXML(attr.getIndex).toString //Cassandra doesn't have XML as data type, thank goodness
      case l if l.startsWith("list") => mkList(tuple, attr)
      case s if s.startsWith("set") => mkSet(tuple, attr)
      case m if m.startsWith("map") => mkMap(tuple, attr)
      case _ => Failure(CassandraWriterException( s"Unrecognized type: ${attr.getType.getLanguageType}", new Exception))
    }
    attr.getName -> value
  }

  private def mkList(tuple: Tuple, attr: Attribute): Any = {
    def castListToType[A <: Any](rawList: java.util.List[_]): java.util.List[A] = {
      rawList.asInstanceOf[java.util.List[A]]
    }
    val listType: CollectionType = attr.getType.asInstanceOf[CollectionType]
    val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
    val rawList = tuple.getList(attr.getIndex)
    castListToType[elementT.type](rawList)
  }

  private def mkSet(tuple: Tuple, attr: Attribute): Any = {
    def castSetToType[A <: Any](rawSet: java.util.Set[_]): java.util.Set[A] = {
      rawSet.asInstanceOf[java.util.Set[A]]
    }
    val setType: CollectionType = attr.getType.asInstanceOf[CollectionType]
    val elementT: Class[_] = setType.getElementType.getObjectType
    val rawSet = tuple.getSet(attr.getIndex)
    castSetToType[elementT.type](rawSet)
  }

  private def mkMap(tuple: Tuple, attr: Attribute): Any = {
    def castMapToType[K <: Any, V <: Any](rawMap: java.util.Map[_,_]): java.util.Map[K,V] = {
      rawMap.asInstanceOf[java.util.Map[K,V]]
    }
    val mapType: MapType = attr.getType.asInstanceOf[MapType]
    val keyT: Class[_] = mapType.getKeyType.getObjectType
    val valT: Class[_] = mapType.getValueType.getObjectType
    val rawMap = tuple.getMap(attr.getIndex)
    castMapToType[keyT.type, valT.type](rawMap)
  }
}
