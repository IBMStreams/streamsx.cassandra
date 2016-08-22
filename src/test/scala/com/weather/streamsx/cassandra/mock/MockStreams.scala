package com.weather.streamsx.cassandra.mock

import com.weather.streamsx.cassandra.{MockCassandra, CassandraSink}
import com.ibm.streams.flow.declare._

import com.ibm.streams.flow.javaprimitives.JavaOperatorTester
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph
import com.ibm.streams.operator.{Tuple, StreamingOutput, OutputTuple}

import scala.util.Random

class MockStreams(splStyleTupleStructureDeclaration: String) {



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

  def shutdown(): Unit = testableGraph.shutdown().get().shutdown().get()

  def submit(tuple: Tuple): Unit = injector.submit(tuple)

  def addField(fieldname: String, splType: String, outputTuple: OutputTuple): (OutputTuple, Any) = {
    val value: Any = splType match {
      case "boolean" => {
        val b = Random.nextBoolean()
        outputTuple.setBoolean(fieldname, b)
        b
      }
      case "int8" => {
        val b: Byte = ((Math.abs(Random.nextInt) % 256) - 128).toByte
        outputTuple.setByte(fieldname, b)
        b
      }
      case "int16" => {
        val s = ((Math.abs(Random.nextInt) % 32768) - 16384).toShort
        outputTuple.setShort(fieldname, s)
        s
        s
      }
      case "int32" => {
        val i = Random.nextInt()
        outputTuple.setInt(fieldname, i)
        i
      }
      case "int64" => {
        val l = Random.nextLong()
        outputTuple.setLong(fieldname, l)
      }
      case "uint8" => {
        val b = (Math.abs(Random.nextInt) % 256).toByte
        outputTuple.setByte(fieldname, b)
        b
      }
      case "uint16" => {
        val s = (Math.abs(Random.nextInt) % 32768).toShort
        outputTuple.setShort(fieldname, s)
        s
      }
      case "uint32" => {
        val l = ((Math.abs(Random.nextLong())) % 4294967296L).toInt
        outputTuple.setInt(fieldname, l)
        l
      }
      //      case "uint64" => outputTuple.getLong(attr.index)
      //      case "float32" => outputTuple.getFloat(attr.index)
      //      case "float64" => outputTuple.getDouble(attr.index)
      //      case "decimal32" => outputTuple.getBigDecimal(attr.index)
      //      case "decimal64" => outputTuple.getBigDecimal(attr.index)
      //      case "decimal128" => outputTuple.getBigDecimal(attr.index)
      //      case "timestamp" => outputTuple.getTimestamp(attr.index)
      case "rstring" | "ustring" => {
        val str = Random.alphanumeric.take(10).toString()
        outputTuple.setString(fieldname, str)
        str
      }
      //      case "blob" => outputTuple.getBlob(attr.index)
      //      case "xml" => outputTuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
      //      case "list" => outputTuple.getList(attr.index)
      //      //I wonder if there will need to be more specific qualifications with list<boolean>, list<int>, etc
      //      case "map" => outputTuple.getMap(attr.index)
      //      //    case "outputTuple" => outputTuple.getoutputTuple(attr.index)
      //      case "" => "figure out better error logging than this"
    }
    (outputTuple, value)
  }
}

