package com.weather.streamsx.cassandra.connection

import com.weather.streamsx.cassandra.zooklient.{ZooKlient, ZooKlientCfg}
import com.weather.streamsx.cassandra.zooklient

object ZKClient {

  def apply( znodePrefix: String = "streamsx.cassandra", connectStr: Option[String] = None): zooklient.ZooKlient = {
    val connectString = connectStr match {
      case None => sys.env.getOrElse("STREAMS_ZKCONNECT", "localhost:2181")
      case Some(s) => s
    }

    val cfg = ZooKlientCfg(znodePrefix, connectString)
    ZooKlient(cfg)
  }
}
