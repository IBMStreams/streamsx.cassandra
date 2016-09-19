package com.weather.streamsx.cassandra.config

import com.weather.analytics.zooklient.ZooKlient
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import scala.util.parsing.json.JSON

//TODO check and see if this breaks if the nullValue config has more than 22 items

object NullValueConfig {

  def apply(zkCli: ZooKlient, znodeName: String): Option[Map[String, Any]] = {

    def parseZnode(): Option[Map[String, Any]] = {
      val json = zkCli.readRawString(znodeName) match {
        case Some(str) => JSON.parseFull(str)
        case _ => throw new CassandraWriterException(s"Failed to get data from ZNode $znodeName", new Exception)
      }
      json match {
        case Some(j) => Some(j.asInstanceOf[Map[String, Any]])
        case _ => None
      }
    }

    znodeName match {
      case null | "" => None
      case _ => parseZnode()
    }

  }
}
