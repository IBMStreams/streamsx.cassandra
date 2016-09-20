package com.weather.streamsx.cassandra

import com.ibm.streams.operator.Attribute

class DualHash(list: List[Attribute]) extends DualHashy {
  private val nameToInt: Map[String, Int] = list.map(a => a.getName -> a.getIndex).toMap
  private val intToName: Map[Int, String] = list.filter(_.getName.nonEmpty).map(a => a.getIndex -> a.getName).toMap

  def apply(s: String): Option[Int] = nameToInt.get(s)
  def apply(i: Int): Option[String] = intToName.get(i)
}

/*
  * This trait is pointless except I wanted to remove the dependency on ibm.operator.Attribute
  * for unit testing
  */
trait DualHashy {
  def apply(s: String): Option[Int]
  def apply(i: Int): Option[String]
}
