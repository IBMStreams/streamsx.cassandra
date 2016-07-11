package com.weather.streamsx.cassandra

import com.ibm.streams.operator.{Attribute, Type}

case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr {
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)

  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))
}