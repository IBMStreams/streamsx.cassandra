package com.weather.streamsx.cassandra

import com.datastax.driver.core.{ConsistencyLevel, SSLOptions, AuthProvider}
import com.ibm.streams.operator.{OutputTuple, Tuple}
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import com.weather.streamsx.cassandra.connection.CassandraConnector
import com.weather.streamsx.cassandra.mock.{MockStreams, MockZK}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PipelineTest(
                    keyspaceStr: String,
                    tableStr: String,
                    tableCreateStr: String,
                    nullValueJSON: String = "{}"
                  ) extends FlatSpec with Matchers with BeforeAndAfterAll {

  val keyspace = keyspaceStr
  val table = tableStr

  val ccfg = new CassSinkClientConfig(
    localdc = "",
    port = MockCassandra.port,
    remapClusterMinutes = 15,
    writeOperationTimeout = 10000L,
    authEnabled = false,
    authUsername = "",
    authPassword = "",
    sslEnabled = false,
    sslKeystore = "",
    sslPassword = "",
    dateFormat = DateTimeFormat.forPattern("yyMMdd"),
    authProvider = AuthProvider.NONE,
    sslOptions = None,
    consistencylevel = ConsistencyLevel.ALL,
    seeds = Array(MockCassandra.ip),
    keyspace = keyspace,
    table = table,
    ttl = 10000000L,
    cacheSize = 100
  )



  val cassConnect = new CassandraConnector(ccfg)

  val session = cassConnect.session

  override def beforeAll(): Unit = {
    MockZK.start()
    MockCassandra.start()

    val cassStr =
      s"""
         |{
         |  "consistencyLevel": "local_quorum",
         |  "port": ${MockCassandra.port},
         |  "seeds": "${MockCassandra.ip}",
         |  "keyspace" : "testkeyspace",
         |  "table" : "testtable",
         |  "ttl" : 2592000,
         |  "cacheSize" : 1000
         |}
    """.stripMargin

    // setup mock ZK nodes
    MockZK.createZNode("/cassConn", cassStr)
    MockZK.createZNode("/nullV", nullValueJSON)

    session.execute(s"drop keyspace if exists $keyspace") //necessary for when runtime errors prevent afterAll from being called
    session.execute(s"create keyspace $keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute(tableCreateStr)
  }

  override def afterAll(): Unit = {
    session.execute(s"drop keyspace if exists $keyspace")
  }

//  val structureMap = Map( "greeting" -> "rstring",
//                          "count" -> "uint64",
//                          "testList" -> "list<int32>",
//                          "testSet" -> "set<int32>",
//                          "testMap" -> "map<int32, boolean>",
//                          "nullInt" -> "int32"
//                        )
//  val tupleStructure = "tuple<rstring greeting, uint64 count, list<int32> testList, set<int32> testSet, map<int32, boolean> testMap, int32 nullInt>"
//  val generator = new MockStreams(tupleStructure)

  def genAndSubmitTuple(m: Map[String, String]): (Tuple, Map[String, Any]) = {
    val tupleStructure = {
      val tupleOpen = "tuple<"
      val meat = m.map(kv => s"${kv._2} ${kv._1}").mkString(", ")
      val tupleClose = ">"
      s"$tupleOpen$meat$tupleClose"
    }
    val generator = new MockStreams(tupleStructure)
    var t = generator.newEmptyTuple()

    def addValToTuple(kv: (String, String), t: OutputTuple): (OutputTuple, (String, Any)) = {
      val entry: (String, Any) = kv._1 -> generator.addField(kv._1, kv._2, t)._2
      (t, entry)
    }

    val nameToValue: Map[String, Any] = {
      m.map(kv => addValToTuple(kv, t)._2)
    }

    generator.submit(t)

    (t, nameToValue)
  }


}
