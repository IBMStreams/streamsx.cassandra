package com.weather.streamsx.cassandra

import com.weather.streamsx.util.zookeeper.{ZkCfg, ZkCli}

object ZkClient {
  private val cfg = ZkCfg("streamsx.cassandra", sys.env.getOrElse("STREAMS_ZKCONNECT", "localhost:21810"))
  val zkCli = ZkCli(cfg)
  val zk = zkCli.client
}