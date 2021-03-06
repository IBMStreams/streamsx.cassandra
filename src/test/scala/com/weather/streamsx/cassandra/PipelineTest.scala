package com.weather.streamsx.cassandra

import com.datastax.driver.core.{ConsistencyLevel, PlainTextAuthProvider}
import com.ibm.streams.operator.{OutputTuple, Tuple}
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import com.weather.streamsx.cassandra.connection.CassandraConnector
import com.weather.streamsx.cassandra.mock.{MockCassandra, MockStreams}
import org.joda.time.format.DateTimeFormat
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
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

  val ipArr: Array[String] = MockCassandra.ip.split(",")
  val cassStr =
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

  val ccfg = new CassSinkClientConfig(
    localdc = "",
    port = MockCassandra.port,
    remapClusterMinutes = 15,
    writeOperationTimeout = 10000L,
    authEnabled = true,
    authUsername = "cassandra",
    authPassword = "cassandra",
    sslEnabled = false,
    sslKeystore = "",
    sslPassword = "",
    dateFormat = DateTimeFormat.forPattern("yyMMdd"),
    authProvider = new PlainTextAuthProvider("cassandra", "cassandra"),
    sslOptions = None,
    consistencylevel = ConsistencyLevel.ALL,
    seeds = ipArr,
    keyspace = keyspace,
    table = table,
    ttl = 10000000L,
    cacheSize = 100
  )

  val cassConnect = new CassandraConnector(ccfg)
  val session = cassConnect.session


  override def beforeAll(): Unit = {

    session.execute(s"drop keyspace if exists $keyspace") //necessary for when runtime errors prevent afterAll from being called
    session.execute(s"create keyspace $keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.execute(tableCreateStr)
  }

  override def afterAll(): Unit = {
    session.execute(s"drop keyspace if exists $keyspace")
    session.close()
    cassConnect.shutdown()

  }

  def genAndSubmitTuple(m: Map[String, String]): (Tuple, Map[String, Any]) = {
    val tupleStructure = {
      val tupleOpen = "tuple<"
      val meat = m.map(kv => s"${kv._2} ${kv._1}").mkString(", ")
      val tupleClose = ">"
      s"$tupleOpen$meat$tupleClose"
    }
    val generator = new MockStreams(tupleStructure, cassStr, nullValueJSON)
    val t = generator.newEmptyTuple()

    def addValToTuple(kv: (String, String), t: OutputTuple): (OutputTuple, (String, Any)) = {
      val entry: (String, Any) = kv._1 -> MockStreams.assignAValue(kv, t)._2._2
      (t, entry)
    }

    val nameToValue: Map[String, Any] = {
      m.map(kv => addValToTuple(kv, t)._2)
    }

    generator.submit(t)
    (t, nameToValue)
  }
}
