package com.weather.streamsx.cassandra

import com.weather.streamsx.cassandra.config.PrimitiveTypeConfig
import com.weather.streamsx.cassandra.connection.ZKClient
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class NullValueTest  extends FlatSpec with Matchers with BeforeAndAfterAll{
  val cfg =
    """
      |{
      |  "consistencyLevel": "local_quorum",
      |  "dateFormat": "yy-MM-dd HH:mm:ss",
      |  "localdc": "",
      |  "port": 9042,
      |  "remapClusterMinutes": 15,
      |  "seeds": "10.0.2.2",
      |  "writeOperationTimeout": 10000,
      |  "authEnabled": false,
      |  "authUsername": "cinple",
      |  "authPassword": "omgwtfbBq",
      |  "sslEnabled": false,
      |  "sslKeystore": "/etc/certs/dev_analytics.p12",
      |  "sslPassword": "omgwtfbbq",
      |  "keyspace" : "testkeyspace",
      |  "table" : "testtable",
      |  "ttl" : "2592000",
      |  "nullValueMap" : {
      |     "a" : 1
      |  }
      |}
    """.stripMargin

  val nodeName = "test"
  val zkTestServer = new TestingServer(4444)
  val cli: CuratorFramework = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString, new RetryOneTime(2000))
  cli.start()
  cli.create().forPath("/test", cfg.getBytes)
  val cool: Array[Byte] = cli.getData.forPath("/test")
  val yeah: String = new String(cool)

  "The string from embedded ZK" should "come back okay, otherwise these tests aren't worth doing" in {
    println(yeah)
    assert(cfg == yeah)
  }

  val zkCli = ZKClient("test", zkTestServer.getConnectString)

  "The case class" should "get parsed from the json" in {
    try {
      val cc = PrimitiveTypeConfig.read("", zkCli)
      println(cc)
    } catch {
      case e: Exception => fail(e)
    }
  }


}
