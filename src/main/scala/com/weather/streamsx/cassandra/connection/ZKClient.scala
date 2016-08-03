package com.weather.streamsx.cassandra.connection

import com.weather.analytics.zooklient.{ZooKlient, ZooKlientCfg}

object ZKClient {
  def apply(): ZooKlient  = {
    val cfg = ZooKlientCfg("streamsx.cassandra", sys.env.getOrElse("STREAMS_ZKCONNECT", "localhost:2181"))
    ZooKlient(cfg)
  }

  private[cassandra] def apply(znodePrefix: String, connectStr: String): ZooKlient  = {
    val cfg = ZooKlientCfg(znodePrefix, connectStr)
    ZooKlient(cfg)
  }
}
