package com.weather.streamsx.cassandra

import com.weather.streamsx.cassandra.connection.ZKClient
import com.weather.streamsx.cassandra.mock.MockZK
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

import scala.util.parsing.json.JSON

@RunWith(classOf[JUnitRunner])
class NullValueConfigTest extends FlatSpec with Matchers with BeforeAndAfterAll{
  override def beforeAll(): Unit = {
    MockZK.start()
  }

  override def afterAll(): Unit = {
//    MockZK.shutdown()
  }

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

  "The raw string" should "get fetched and parsed" in {
    MockZK.createZNode("/test", cfg)

    val mizzap = JSON.parseFull(MockZK.getZNode("/test")).get.asInstanceOf[Map[String, Any]]
    mizzap should equal(compareMap)
  }
}
