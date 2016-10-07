package com.weather.streamsx.cassandra.util

object StringifyStackTrace {
  def apply(e: Throwable): String = {
    val sw = new java.io.StringWriter
    val pw = new java.io.PrintWriter(sw)
    e.printStackTrace(pw)
    sw.toString
  }
}
