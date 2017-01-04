//package com.weather.streamsx.cassandra
//
//import com.datastax.driver.core.{PlainTextAuthProvider, ConsistencyLevel, Row}
//import com.ibm.streams.operator.{OutputTuple, Tuple}
//import com.weather.streamsx.cassandra.config.CassSinkClientConfig
//import com.weather.streamsx.cassandra.connection.CassandraConnector
//import com.weather.streamsx.cassandra.mock.{MockStreams, MockZK}
//import org.joda.time.format.DateTimeFormat
//import org.junit.runner.RunWith
//import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
//import org.scalatest.junit.JUnitRunner
//
//@RunWith(classOf[JUnitRunner])
//class AuthTest extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  val keyspace = "authKeyspace"
//  val table = "authTable"
//  val tableCreateStr =   s"""
//                           |create table IF NOT EXISTS $keyspace.$table (
//                           |  greeting varchar,
//                           |  count bigint,
//                           |  cool varchar,
//                           |  PRIMARY KEY (count)
//                           |) with caching = 'none';
//                         """.stripMargin
//  val insertStr =
//    s"""
//       |INSERT INTO $keyspace.$table (greeting, count, cool) VALUES ('a', 1, 'b');
//     """.stripMargin
//  val user = "foo"
//  val pass = "bar"
//
//  val ipArr: Array[String] = MockCassandra.ip.split(",")
////  val ipArr: Array[String] = Array("127.0.0.1")
//
//  val ccfgAdmin = new CassSinkClientConfig(
//    localdc = "",
//    port = MockCassandra.port,
//    remapClusterMinutes = 15,
//    writeOperationTimeout = 10000L,
//    authEnabled = true,
//    authUsername = "cassandra",
//    authPassword = "cassandra",
//    sslEnabled = false,
//    sslKeystore = "",
//    sslPassword = "",
//    dateFormat = DateTimeFormat.forPattern("yyMMdd"),
//    authProvider = new PlainTextAuthProvider("cassandra", "cassandra"),
//    sslOptions = None,
//    consistencylevel = ConsistencyLevel.ALL,
//    seeds = ipArr,
//    keyspace = keyspace,
//    table = table,
//    ttl = 10000000L,
//    cacheSize = 100
//  )
//
//  val ccfgRestricted = new CassSinkClientConfig(
//    localdc = "",
//    port = MockCassandra.port,
//    remapClusterMinutes = 15,
//    writeOperationTimeout = 10000L,
//    authEnabled = true,
//    authUsername = user,
//    authPassword = pass,
//    sslEnabled = false,
//    sslKeystore = "",
//    sslPassword = "",
//    dateFormat = DateTimeFormat.forPattern("yyMMdd"),
//    authProvider = new PlainTextAuthProvider(user, pass),
//    sslOptions = None,
//    consistencylevel = ConsistencyLevel.ALL,
//    seeds = ipArr,
//    keyspace = keyspace,
//    table = table,
//    ttl = 10000000L,
//    cacheSize = 100
//  )
//
//  val cassConnectAdmin = new CassandraConnector(ccfgAdmin)
//
//  val mockZK = new MockZK()
//
//  val cassStr =
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
//       |  "authUsername": "$user",
//       |  "authPassword": "$pass",
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
//  // then create
//  mockZK.start()
//  mockZK.createZNode("/cassConn", cassStr)
//  mockZK.createZNode("/nullV", "{}")
//
//
//
//
//
//  val structureMap = Map( "greeting" -> "rstring",
//                            "count" -> "uint64",
//                            "cool" -> "rstring"
//                          )
//    def row2greeting(r: Row): Map[String, Any] = {
//      Map(
//        "greeting" -> r.getString("greeting"),
//        "count" -> r.getLong("count"),
//        "cool" -> r.getString("cool")
//      )
//    }
//
//  def genAndSubmitTuple(m: Map[String, String]): (Tuple, Map[String, Any]) = {
//    val tupleStructure = {
//      val tupleOpen = "tuple<"
//      val meat = m.map(kv => s"${kv._2} ${kv._1}").mkString(", ")
//      val tupleClose = ">"
//      s"$tupleOpen$meat$tupleClose"
//    }
//    val generator = new MockStreams(tupleStructure, mockZK.connectString)
//    val t = generator.newEmptyTuple()
//
//    def addValToTuple(kv: (String, String), t: OutputTuple): (OutputTuple, (String, Any)) = {
//      val entry: (String, Any) = kv._1 -> MockStreams.assignAValue(kv, t)._2._2
//      (t, entry)
//    }
//
//    val nameToValue: Map[String, Any] = {
//      m.map(kv => addValToTuple(kv, t)._2)
//    }
//
//    generator.submit(t)
//    (t, nameToValue)
//  }
//
//  "The operator" should "throw an error if there are auth issues" in {
//    cassConnectAdmin.session.execute(s"drop keyspace if exists $keyspace")
//    cassConnectAdmin.session.execute(s"create keyspace $keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
//    cassConnectAdmin.session.execute(tableCreateStr)
//    cassConnectAdmin.session.execute(s"CREATE USER IF NOT EXISTS $user WITH PASSWORD '$pass'")
//    cassConnectAdmin.session.execute(s"REVOKE ALL ON $keyspace.$table FROM $user")
//    val cassConnectRestricted = new CassandraConnector(ccfgRestricted)
//
//    an [Exception] should be thrownBy genAndSubmitTuple(structureMap)
//
//    cassConnectRestricted.shutdown()
//  }
//  override def afterAll(): Unit = {
//
//    cassConnectAdmin.session.execute(s"drop keyspace if exists $keyspace")
//    cassConnectAdmin.session.execute(s"DROP USER $user")
//    cassConnectAdmin.session.close()
//    cassConnectAdmin.shutdown()
//
//    mockZK.deleteZnode("/cassConn")
//    mockZK.deleteZnode("/nullV")
//    mockZK.shutdown()
//  }
//}
