package com.weather.streamsx.cassandra.mock

import com.weather.streamsx.cassandra.{MockCassandra, CassandraSink}
import com.ibm.streams.flow.declare._

import com.ibm.streams.flow.javaprimitives.JavaOperatorTester
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph
import com.ibm.streams.operator.{StreamingOutput, OutputTuple}

class MockStreams(splStyleTupleStructureDeclaration: String) {

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
      |  "authEnabled": false,
      |  "authUsername": "cinple",
      |  "authPassword": "omgwtfbBq",
      |  "sslEnabled": false,
      |  "sslKeystore": "/etc/certs/dev_analytics.p12",
      |  "sslPassword": "omgwtfbbq",
      |  "keyspace" : "testkeyspace",
      |  "table" : "testtable",
      |  "ttl" : 2592000,
      |  "cacheSize" : 1000
      |}
    """.stripMargin


  // setup mock ZK nodes
  MockZK.createZNode("/cassConn", cassStr)
  MockZK.createZNode("/nullV", "{}")
  MockCassandra.start()

  val graph: OperatorGraph = OperatorGraphFactory.newGraph()
  val op: OperatorInvocation[CassandraSink] = graph.addOperator(classOf[CassandraSink])
//  op.setIntParameter("port", 0)
  op.setStringParameter("connectionConfigZNode", "/cassConn")
  op.setStringParameter("nullMapZnode", "/nullV")
  op.setStringParameter("zkConnectionString", MockZK.connectString)
  // Create the object representing the type of tuple that is coming into the operator
  val tuplez: InputPortDeclaration = op.addInput(splStyleTupleStructureDeclaration)
  // Create the testable version of the graph
  val testableGraph: JavaTestableGraph  = new JavaOperatorTester().executable(graph)
  // Create the injector to inject test tuples.
  val injector: StreamingOutput[OutputTuple] = testableGraph.getInputTester(tuplez)
  // Execute the initialization of operators within graph.
  testableGraph.initialize().get().allPortsReady().get()

  // omg can I actually get a tuple out of this???
  def newEmptyTuple(): OutputTuple = injector.newTuple()

  testableGraph.shutdown().get().shutdown().get()
}
