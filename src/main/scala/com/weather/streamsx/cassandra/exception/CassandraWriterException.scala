package com.weather.streamsx.cassandra.exception

case class CassandraWriterException(m: String, ex: Throwable) extends Exception(m,ex)
