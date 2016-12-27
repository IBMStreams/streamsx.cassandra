/*
TODO: THESE NEED TO BE RE-WRITTEN FOR CONFIGURATION OBJECTS
 */



//package com.weather.streamsx.cassandra
//
//import com.weather.streamsx.cassandra.exception.CassandraWriterException
//import com.weather.streamsx.cassandra.mock.MockZK
//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
//
//@RunWith(classOf[JUnitRunner])
//class WriterStartUpExceptions extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  val keyspace = "keyz"
//  val table = "tablez"
//
//  val cassStr0 = ""
//
//  val cassStr1 =
//    s"""
//       |{
//       |  "consistencyLevel": "local_quorum",
//       |  "dateFormat": "yy-MM-dd HH:mm:ss",
//       |  "localdc": "",
//       |  "port": 0,
//       |  "remapClusterMinutes": 15,
//       |  "seeds": "0.0.0.0",
//       |  "writeOperationTimeout": 10000,
//       |  "authEnabled": true,
//       |  "authUsername": "cassandra",
//       |  "authPassword": "cassandra",
//       |  "sslEnabled": false,
//       |  "sslKeystore": "lol",
//       |  "sslPassword": "liketotally",
//       |  "keyspace" : "$keyspace",
//       |  "table" : "$table",
//       |  "ttl" : 2592000,
//       |  "cacheSize" : 1000
//       |}
//    """.stripMargin
//
//  val cassStr2 =
//    s"""
//       |{
//       |  "consistencyLevel": "local_quorum",
//       |  "dateFormat": "yy-MM-dd HH:mm:ss",
//       |  "localdc": "",
//       |  "port": ${MockCassandra.port},
//       |  "remapClusterMinutes": 15,
//       |  "seeds": "${MockCassandra.ip}",
//       |  "writeOperationTimeout": 10000,
//       |  "authEnabled": true,
//       |  "authUsername": "cassandra",
//       |  "authPassword": "cassandra",
//       |  "sslEnabled": false,
//       |  "sslKeystore": "lol",
//       |  "sslPassword": "liketotally",
//       |  "keyspace" : "$keyspace",
//       |  "table" : "$table",
//       |  "ttl" : 2592000,
//       |  "cacheSize" : 1000
//       |}
//     """.stripMargin
//
//
//  ignore should "not start if it can't talk to ZK" in {
//    // MockZK not started.
//    // This test will just hang and retrying the znode connection. That's fine, that's good behavior, but let's ignore it for the test run.
//    an [Exception] should be thrownBy CassandraSinkImpl.mkWriter("/notAZnode", "/alsoNotAZone", "")
//  }
//
//  "The operator" should "not start if it can't get the Znode" in {
//    val mockZK = new MockZK()
//    mockZK.start()
//
//    // todo import the apache stuff necessary to type this more closely
//    an [Exception] should be thrownBy CassandraSinkImpl.mkWriter("/notAZnode", "/alsoNotAZone", mockZK.connectString)
//
//
////    val s = intercept[Exception] {
////      CassandraSinkImpl.mkWriter("/notAZnode", "/alsoNotAZone", mockZK.connectString)
////    }
//    mockZK.shutdown()
//  }
//
//  it should "not start if there's nothing in the connection znode" in {
//    val mockZK = new MockZK()
//    mockZK.start()
//    mockZK.createZNode("/cassConn", cassStr0)
//
//    an [Exception] should be thrownBy CassandraSinkImpl.mkWriter("/cassConn", "/alsoNotAZone", mockZK.connectString)
//
//    mockZK.shutdown()
//  }
//
//  it should "not start if it can't connect to the given znode" in {
//    val mockZK = new MockZK()
//    mockZK.start()
//    mockZK.createZNode("/cassConn", cassStr0)
//
//    an [Exception] should be thrownBy CassandraSinkImpl.mkWriter("/cassConn", "/alsoNotAZone", mockZK.connectString)
//
//    mockZK.shutdown()
//  }
//
//  it should "be fine if there's no data in the znode" in {
//    val mockZK = new MockZK()
//    mockZK.start()
//    mockZK.createZNode("/cassConn", cassStr2)
//
//    val s = CassandraSinkImpl.mkWriter("/cassConn", "", mockZK.connectString)
//
//    mockZK.shutdown()
//  }
//
//  it should "not start if there's invalid connection data in the znode" in {
//    val mockZK = new MockZK()
//    mockZK.start()
//    mockZK.createZNode("/cassConn", cassStr1)
//    val ss =
//      """
//        |{"stuff" : "things"}
//      """.stripMargin
//    mockZK.createZNode("/nullz", ss)
//
//    a [CassandraWriterException] should be thrownBy CassandraSinkImpl.mkWriter("/cassConn", "/nullz", mockZK.connectString)
//
//    mockZK.shutdown()
//  }
//}
