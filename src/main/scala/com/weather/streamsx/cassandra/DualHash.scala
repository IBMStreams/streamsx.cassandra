package com.weather.streamsx.cassandra

import com.ibm.streams.operator.Attribute

class DualHash(list: List[Attribute]) extends DualHashy{
  val nameToInt: Map[String, Int] = list.map(a => a.getName -> a.getIndex).toMap
  val intToName: Map[Int, String] = list.map(a => a.getIndex -> a.getName).toMap

  def apply(s: String): Option[Int] = nameToInt(s) match {
    case i: Int => Some(i)
    case _ => None
  }
  def apply(i: Int): Option[String] = intToName(i) match {
    case s: String => Some(s)
    case _ => None
  }
}

/*
  * This trait is pointless except I wanted to remove the dependency on ibm.operator.Attribute
  * for unit testing
  */
trait DualHashy {
  def apply(s: String): Option[Int]
  def apply(i: Int): Option[String]
}
