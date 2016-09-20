package com.weather.streamsx.cassandra.connection

import com.weather.analytics.zooklient.{ZooKlient, ZooKlientCfg}

object ZKClient {

  def apply( znodePrefix: String = "streamsx.cassandra", connectStr: Option[String] = None): ZooKlient = {
    val connectString = connectStr match {
      case None => sys.env.getOrElse("STREAMS_ZKCONNECT", "localhost:2181")
      case Some(s) => s
    }

    val cfg = ZooKlientCfg(znodePrefix, connectString)
    ZooKlient(cfg)
  }
}
