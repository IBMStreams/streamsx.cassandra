/*
TODO: THESE NEED TO BE RE-WRITTEN FOR CONFIGURATION OBJECTS
 */



//package com.weather.streamsx.cassandra
//
//import com.weather.streamsx.cassandra.config.NullValueConfig
//import com.weather.streamsx.cassandra.connection.ZKClient
//import com.weather.streamsx.cassandra.exception.CassandraWriterException
//import com.weather.streamsx.cassandra.mock.MockZK
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
//
//import scala.util.parsing.json.JSON
//
//@RunWith(classOf[JUnitRunner])
//class NullValueConfigTest extends FlatSpec with Matchers with BeforeAndAfterAll{
//
//  val mockZK = new MockZK()
//
//  override def beforeAll(): Unit = {
//    mockZK.start()
//  }
//
//  override def afterAll(): Unit = {
//    mockZK.shutdown()
//  }
//
//  val cfg =
//    """
//      |{
//      |  "a" : 1,
//      |  "b" : "",
//      |  "c" : -2.2
//      |}
//    """.stripMargin
//
//  val invalidCfg =
//    """
//      |{
//      |  a : 1,
//      |  "b" : ""
//      |  "c" : -2.2
//      |}
//    """.stripMargin
//
//  val emptyCfg = "{}"
//
//  val json:Option[Any] = JSON.parseFull(cfg)
//  val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
//
//  val compareMap = Map("a" -> 1, "b" -> "", "c" -> -2.2)
//
//  "The parsed JSON" should "match the test map" in {
//    map should equal (compareMap)
//  }
//
//  "The raw string" should "get fetched and parsed" in {
//    mockZK.createZNode("/test", cfg)
//    val mizzap = JSON.parseFull(mockZK.getZNode("/test")).get.asInstanceOf[Map[String, Any]]
//    mizzap should equal(compareMap)
//  }
//
//  "NullValueConfig" should "work" in {
//    val zkCli: zooklient.ZooKlient = ZKClient(connectStr = Some(mockZK.connectString))
//    NullValueConfig(zkCli, "/test") shouldBe compareMap
//  }
//
//  it should "throw an exception if it can't parse the json" in {
//    mockZK.createZNode("/testtwo", invalidCfg)
//    val zkCli: zooklient.ZooKlient = ZKClient(connectStr = Some(mockZK.connectString))
//    a [CassandraWriterException] should be thrownBy NullValueConfig(zkCli, "/testtwo")
//  }
//
//  it should "do something reasonable with empty JSON" in {
//    mockZK.createZNode("/testthree", emptyCfg)
//    val zkCli: zooklient.ZooKlient = ZKClient(connectStr = Some(mockZK.connectString))
//    val x = NullValueConfig(zkCli, "/testthree")
//    x shouldBe Map.empty[String, Any]
//  }
//}
