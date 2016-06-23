package com.weather.streamsx.cassandra

case class CassandraWriterException(m: String, ex: Throwable) extends Exception(m,ex)