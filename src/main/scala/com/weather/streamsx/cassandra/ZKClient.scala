package com.weather.streamsx.cassandra

import com.weather.analytics.zooklient.{ZooKlient, ZooKlientCfg}

object ZKClient {
  private val cfg = ZooKlientCfg("streamsx.cassandra", sys.env.getOrElse("STREAMS_ZKCONNECT", "localhost:2181"))
  val zkCli = ZooKlient(cfg)
  val zk = zkCli.client
}