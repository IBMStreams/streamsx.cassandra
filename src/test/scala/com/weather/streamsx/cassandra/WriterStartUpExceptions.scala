/*
TODO: THESE NEED TO BE RE-WRITTEN FOR CONFIGURATION OBJECTS
 */



package com.weather.streamsx.cassandra

import com.weather.streamsx.cassandra.mock.{MockCassandra, MockStreams}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class WriterStartUpExceptions extends FlatSpec with Matchers with BeforeAndAfterAll {

  val standinTupleStructure = "tuple<rstring stuff>"
  
  val keyspace = "keyz"
  val table = "tablez"

  val cassStr0 = ""

  val cassStr1 =
    s"""
       |{
       |  "consistencyLevel": "local_quorum",
       |  "dateFormat": "yy-MM-dd HH:mm:ss",
       |  "localdc": "",
       |  "port": 0,
       |  "remapClusterMinutes": 15,
       |  "seeds": "0.0.0.0",
       |  "writeOperationTimeout": 10000,
       |  "authEnabled": true,
       |  "authUsername": "cassandra",
       |  "authPassword": "cassandra",
       |  "sslEnabled": false,
       |  "sslKeystore": "lol",
       |  "sslPassword": "liketotally",
       |  "keyspace" : "$keyspace",
       |  "table" : "$table",
       |  "ttl" : 2592000,
       |  "cacheSize" : 1000
       |}
    """.stripMargin

  val cassStr2 =
    s"""
       |{
       |  "consistencyLevel": "local_quorum",
       |  "dateFormat": "yy-MM-dd HH:mm:ss",
       |  "localdc": "",
       |  "port": ${MockCassandra.port},
       |  "remapClusterMinutes": 15,
       |  "seeds": "${MockCassandra.ip}",
       |  "writeOperationTimeout": 10000,
       |  "authEnabled": true,
       |  "authUsername": "cassandra",
       |  "authPassword": "cassandra",
       |  "sslEnabled": false,
       |  "sslKeystore": "lol",
       |  "sslPassword": "liketotally",
       |  "keyspace" : "$keyspace",
       |  "table" : "$table",
       |  "ttl" : 2592000,
       |  "cacheSize" : 1000
       |}
     """.stripMargin

  val cassStr3 =
    s"""
       |{
       |  "consistencyLevel": "local_quorum",
       |  "dateFormat": "yy-MM-dd HH:mm:ss",
       |  "localdc": "",
       |  "port": ${MockCassandra.port},
       |  "remapClusterMinutes": 15,
       |  "seeds": "${MockCassandra.ip}",
       |  "writeOperationTimeout": 10000,
       |  "authEnabled": true,
       |  "authUsername": "cassandra",
       |  "authPassword": "cassandra",
       |  "sslEnabled": false,
       |  "sslKeystore": "lol",
       |  "sslPassword": "liketotally",
       |  "keyspace" : "$keyspace",
       |  "table" : "$table",
       |  "ttl" : 2592000,
       |  "cacheSize" : 1000
     """.stripMargin

  "The operator" should "not start if the connection info is not set" in {
    // MockZK not started.
    // This test will just hang and retrying the znode connection. That's fine, that's good behavior, but let's ignore it for the test run.
    an [Exception] should be thrownBy new MockStreams(standinTupleStructure, null, null)
  }

  it should "not start if the connection info is empty" in {
    // todo import the apache stuff necessary to type this more closely
    an [Exception] should be thrownBy new MockStreams(standinTupleStructure, cassStr0, "")
  }

  it should "not start if the connection info is malformed" in {
    // todo import the apache stuff necessary to type this more closely
    an [Exception] should be thrownBy new MockStreams(standinTupleStructure, cassStr3, "")
  }

  it should "be fine if there's no data in the nullmap" in {
    val s = new MockStreams(standinTupleStructure, cassStr2, "")
  }

  it should "not start if there's invalid connection data in the znode" in {
    val ss =
      """
        |{"stuff" : "things"}
      """.stripMargin

    an [Exception] should be thrownBy new MockStreams(standinTupleStructure, cassStr1, ss)
  }
}
