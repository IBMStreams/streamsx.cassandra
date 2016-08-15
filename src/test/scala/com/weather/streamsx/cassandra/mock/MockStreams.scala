package com.weather.streamsx.cassandra.mock

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import com.ibm.streams.flow.{handlers, declare}
import com.weather.streamsx.cassandra.CassandraSink
import com.ibm.streams.flow.declare._

import com.ibm.streams.flow.handlers.MostRecent;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streams.operator.{StreamingOutput, OutputTuple, Tuple}
import com.ibm.streams.operator.internal._

class MockStreams(splStyleTupleStructureDeclaration: String) {

  // setup mock ZK nodes
  MockZK.createZNode("lol", "{}")
  MockZK.createZNode("lolol", "{}")

  val graph: OperatorGraph = OperatorGraphFactory.newGraph()
  val op: OperatorInvocation[CassandraSink] = graph.addOperator(classOf[CassandraSink])
//  op.setIntParameter("port", 0)
  op.setStringParameter("connectionConfigZNode", "/lol")
  op.setStringParameter("nullMapZnode", "/lolol")
  op.setStringParameter("zkConnectionString", MockZK.connectString)
  // Create the object representing the type of tuple that is coming into the operator
  val tuplez: InputPortDeclaration = op.addInput(splStyleTupleStructureDeclaration)
  // Create the testable version of the graph
  val testableGraph: JavaTestableGraph  = new JavaOperatorTester().executable(graph)
  // Create the injector to inject test tuples.
  val injector: StreamingOutput[OutputTuple] = testableGraph.getInputTester(tuplez)
  // Execute the initialization of operators within graph.
  testableGraph.initialize().get().allPortsReady().get();

  // omg can I actually get a tuple out of this???
  def newEmptyTuple(): OutputTuple = injector.newTuple()
}
