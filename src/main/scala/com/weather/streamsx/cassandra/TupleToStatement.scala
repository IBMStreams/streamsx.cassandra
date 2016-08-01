package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.ibm.streams.operator.meta.{MapType, CollectionType}
import scala.collection.immutable.BitSet
import scalaz.Failure

object TupleToStatement {

  /*
    Yes, yes, yes, this var is not kosher, this creates a race condition, etc.
    However, this var is only going to mutated at the very front of the program,
    and it will always get initialized to the same thing, even if its by multiple threads.
    So yes, this is not kosher, but it's fine.
   */
  var indexMap: DualHashy = null

  def apply(tuple: Tuple, session: Session, cfg: CassSinkClientConfig, nullValueMap: Map[String, Any]): BoundStatement = {
    val attributeList = mkAttrList(tuple)
    if(indexMap == null) indexMap = new DualHash(attributeList)
    val cache = new StatementCache(cfg, session, indexMap)
    val valuesMap: Map[String, Any] = attributeList.map(getValueFromTuple(tuple, _)).toMap
    mkBoundStatement(valuesMap, nullValueMap, cache)
  }

  private[cassandra] def mkBoundStatement(
                                           valuesMap: Map[String, Any],
                                           nullValueMap: Map[String, Any],
                                           cache: StatementCache
                                         ): BoundStatement = {
    val (bitSet, nonNulls) = mkBitSet(valuesMap, nullValueMap, indexMap)
    val ps = cache(bitSet)
    val bindingValues = nonNulls.map(kv => kv._2)
    ps.bind(bindingValues.asInstanceOf[Seq[Object]]:_*)
  }

  private def mkBitSet(m: Map[String, Any], nullValues: Map[String, Any], indexMap: DualHashy): (BitSet, List[(String, Any)]) = {
    //TODO account for empty collections here or somewhere else
    
    def filterNulls(kv: (String, Any)): Option[(String, Any)] = {
      if(nullValues.contains(kv._1)) {
        m(kv._1) match {
          case v if v == nullValues(kv._1) => None
          case _ => Some(kv)
        }
      }
      else Some(kv)
    }
    val nonNulls = m.flatMap(filterNulls).toList.sortBy(kv => indexMap(kv._1))
    val bitList = nonNulls.flatMap(kv => indexMap(kv._1)).toList
    (BitSet(bitList:_*), nonNulls)
  }

  private def mkAttrList(t: Tuple): List[Attribute] = {
    val schema = t.getStreamSchema
    val attributes = (0 until schema.getAttributeCount).map(i => schema.getAttribute(i)).sortBy(_.getName()).toList
    attributes
  }

  def getValueFromTuple(tuple: Tuple, attr: Attribute): (String, Any) = {
    val value: Any = attr.getType.getLanguageType match {
      case "boolean" => tuple.getBoolean(attr.getIndex)
      case "int8"  | "uint8" => tuple.getByte(attr.getIndex)
      case "int16" | "uint16" => tuple.getShort(attr.getIndex)
      case "int32" | "uint32" => tuple.getInt(attr.getIndex)
      case "int64" | "uint64" => tuple.getLong(attr.getIndex)
      case "float32" => tuple.getFloat(attr.getIndex)
      case "float64" => tuple.getDouble(attr.getIndex)
      case "decimal32" | "decimal64" | "decimal128" => tuple.getBigDecimal(attr.getIndex)
      case "timestamp" => tuple.getTimestamp(attr.getIndex)
      case "rstring" | "ustring" => tuple.getString(attr.getIndex)
      case "blob" => tuple.getBlob(attr.getIndex)
      case "xml" => tuple.getXML(attr.getIndex).toString //Cassandra doesn't have XML as data type, thank goodness
      case l if l.startsWith("list") =>
        val listType: CollectionType = attr.getType.asInstanceOf[CollectionType]
        val elementT: Class[_] = listType.getElementType.getObjectType // This is the class of the individual elements: Int, String, etc.
      val rawList = tuple.getList(attr.getIndex)
        castListToType[elementT.type](rawList)
      case s if s.startsWith("set") =>
        val setType: CollectionType = attr.getType.asInstanceOf[CollectionType]
        val elementT: Class[_] = setType.getElementType.getObjectType
        val rawSet = tuple.getSet(attr.getIndex)
        castSetToType[elementT.type](rawSet)
      case m if m.startsWith("map") =>
        val mapType: MapType = attr.getType.asInstanceOf[MapType]
        val keyT: Class[_] = mapType.getKeyType.getObjectType
        val valT: Class[_] = mapType.getValueType.getObjectType
        val rawMap = tuple.getMap(attr.getIndex)
        castMapToType[keyT.type, valT.type](rawMap)
      case _ => Failure(CassandraWriterException( s"Unrecognized type: ${attr.getType.getLanguageType}", new Exception))
    }
    attr.getName -> value
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
