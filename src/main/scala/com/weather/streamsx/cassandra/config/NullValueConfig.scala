package com.weather.streamsx.cassandra.config

import com.weather.analytics.zooklient.ZooKlient
import scala.util.parsing.json.JSON

//TODO account for empty collections here or somewhere else

object NullValueConfig {
//  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def apply(zkCli: ZooKlient, znodeName: String): Option[Map[String, Any]] = {
    val json = zkCli.readRawString(znodeName) match {
      case Some(str) => JSON.parseFull(str)
      case _ => None
    }
    json match {
      case Some(j) => Some(j.asInstanceOf[Map[String, Any]])
      case _ => None
    }
  }
}
