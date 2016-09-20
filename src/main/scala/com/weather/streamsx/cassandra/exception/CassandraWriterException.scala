package com.weather.streamsx.cassandra.exception

case class CassandraWriterException(m: String, ex: Throwable = new Exception) extends Exception(m,ex)