package com.weather.streamsx.cassandra

import com.ibm.streams.operator.Attribute

class DualHash(list: List[Attribute]) extends DualHashy{
  val nameToInt: Map[String, Int] = list.map(a => a.getName -> a.getIndex).toMap
  val intToName: Map[Int, String] = list.map(a => a.getIndex -> a.getName).toMap

  def apply(s: String): Option[Int] = {
    try Some(nameToInt(s))
    catch {
      case e: Exception => None
    }
  }

  def apply(i: Int): Option[String] = {
    try {
      intToName(i) match {
        case s: String if s.nonEmpty => Some(s)
        case _ => None
      }
    }
    catch {
      case e: Exception => None
    }
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
