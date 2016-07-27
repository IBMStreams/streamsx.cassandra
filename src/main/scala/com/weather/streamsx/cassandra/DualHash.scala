package com.weather.streamsx.cassandra

import com.ibm.streams.operator.Attribute

class DualHash(list: List[Attribute]){
  val stringToInt: Map[String, Int] = list.map(a => a.getName -> a.getIndex).toMap
  val intToString: Map[Int, String] = list.map(a => a.getIndex -> a.getName).toMap

  def apply(s: String): Int = stringToInt(s)
  def apply(i: Int): String = intToString(i)
}
