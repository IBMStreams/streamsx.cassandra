package com.weather.streamsx.cassandra.config

import com.weather.analytics.zooklient.ZooKlient
import com.weather.streamsx.cassandra.exception.CassandraWriterException

import scala.util.parsing.json.JSON

object NullValueConfig {
  def apply(zkCli: ZooKlient, znodeName: String): Map[String, Any] = try {
    val rawString = zkCli.readRawString(znodeName).get
    JSON.parseFull(rawString).get.asInstanceOf[Map[String, Any]]
  } catch {
    case e: Exception => throw new CassandraWriterException("Could not read null value configuration from node", e)
  }
}
