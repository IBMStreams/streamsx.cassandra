package com.weather.streamsx.cassandra

import com.weather.streamsx.cassandra.connection.ZKClient
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

import scala.util.parsing.json.JSON


@RunWith(classOf[JUnitRunner])
class NullValueConfigTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  val cfg =
    """
      |{
      |  "a" : 1,
      |  "b" : "",
      |  "c" : -2.2
      |}
    """.stripMargin

  val json:Option[Any] = JSON.parseFull(cfg)
  val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]

  val compareMap = Map("a" -> 1, "b" -> "", "c" -> -2.2)

  "The parsed JSON" should "match the test map" in {
    map should equal (compareMap)
  }

  val nodeName = "test"
  val zkTestServer = new TestingServer(4444)
  val cli: CuratorFramework = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString, new RetryOneTime(2000))
  cli.start()
  cli.create().forPath("/test", cfg.getBytes)
  val zkCli = ZKClient("test", zkTestServer.getConnectString)


  "The raw string" should "get fetched and parsed" in {
    val mizzap = JSON.parseFull(zkCli.readRawString("").get).get.asInstanceOf[Map[String, Any]]

    mizzap should equal(compareMap)
  }

  cli.close()
}
