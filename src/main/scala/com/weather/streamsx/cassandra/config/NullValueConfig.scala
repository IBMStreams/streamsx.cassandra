package com.weather.streamsx.cassandra.config

import com.ibm.streams.operator.Attribute

import com.weather.streamsx.cassandra.exception.CassandraWriterException


//TODO check and see if this breaks if the nullValue config has more than 22 items

object NullValueConfig {

  def apply(m: Map[String, String], list: List[Attribute]): Map[String, Any] = {
    val fieldToSPLType: Map[String, String] = list.map(a => (a.getName, a.getType.getLanguageType)).toMap

    def eachMapEntry(kv: (String, String)): (String, Any) = {
      val name = kv._1
      val strValue = kv._2
      val splType = fieldToSPLType(name)
      val value: Any = cast(strValue, splType)
      (name, value)
    }

    m.map(eachMapEntry)
  }

  def cast(s: String, splType: String): Any = splType match {
    case "boolean" => s.toBoolean
    case "int8"  | "uint8" => s.toInt   //TODO C* 3.0+ has support for different data types for byte and short
    case "int16" | "uint16" => s.toInt //TODO C* 3.0+ has support for different data types for byte and short
    case "int32" | "uint32" => s.toInt
    case "int64" | "uint64" => s.toLong
    case "float32" => s.toFloat
    case "float64" => s.toDouble
    case "decimal32" | "decimal64" | "decimal128" => new java.math.BigDecimal(s)
    case "rstring" | "ustring" => s
    case "xml" => s
    case _ => throw CassandraWriterException(s"No support available for configuring null values for the following SPL type: $splType. " +
      s"Note that maps, lists, and sets will be considered null by default if empty.")
  }

}