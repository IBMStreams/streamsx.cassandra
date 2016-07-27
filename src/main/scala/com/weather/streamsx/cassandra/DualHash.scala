package com.weather.streamsx.cassandra

import com.ibm.streams.operator.Attribute

class DualHash(list: List[Attribute]){
  val stringToInt: Map[String, Int] = list.map(a => a.getName -> a.getIndex).toMap
  val intToString: Map[Int, String] = list.map(a => a.getIndex -> a.getName).toMap

  def apply(s: String): Option[Int] = stringToInt(s) match {
    case i: Int => Some(i)
    case _ => None
  }
  def apply(i: Int): Option[String] = intToString(i) match {
    case s: String => Some(s)
    case _ => None
  }
}
