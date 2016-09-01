package com.weather.streamsx.cassandra.mock

import java.io.InputStream
import java.util
import javax.xml.transform.stream.StreamSource

import com.ibm.streams.operator.types.XML
import com.weather.streamsx.cassandra.exception.CassandraWriterException
import com.weather.streamsx.cassandra.CassandraSink
import com.ibm.streams.flow.declare._
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph
import com.ibm.streams.operator.{StreamingOutput, OutputTuple}
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scalaz.Failure
import scala.collection.JavaConverters._

object MockStreams {
  def genValue(splType: String): Any = splType match {
    case "boolean" => Random.nextBoolean()
    case "int8" => ((Math.abs(Random.nextInt) % 256) - 128).toByte
    case "int16" => ((Math.abs(Random.nextInt) % 32768) - 16384).toShort
    case "int32" => Random.nextInt()
    case "int64" | "uint64" => Random.nextLong()
    case "uint8" => (Math.abs(Random.nextInt) % 256).toByte
    case "uint16" => (Math.abs(Random.nextInt) % 32768).toShort
    case "uint32" => (Math.abs(Random.nextLong()) % 4294967296L).toInt
    case "float32" => Random.nextFloat()
    case "float64" => Random.nextDouble()
    case "decimal32" | "decimal64" | "decimal128"  => new java.math.BigDecimal(Random.nextFloat())
    //      case "timestamp" => new Timestamp()
    case "rstring" | "ustring" => {
      val num = randomPosNum(20)
      randomString(num)
    }
    case _ => Failure(CassandraWriterException( s"Unrecognized type: $splType", new Exception))
  }

  def randomString(length: Int): String = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  def randomPosNum(limit: Int): Int =   {
    val i = Random.nextInt % limit
    if(i < 0) i * -1
    else i
  }

  // kv = key value pair = (fieldname, spltype)
  def assignAValue(kv: (String, String), t: OutputTuple): (OutputTuple, (String, Any)) = kv._2 match {
    case l: String if l.startsWith("list") => setStringList(l, t, kv._1)
    case s: String if s.startsWith("set") => setStringSet(s, t, kv._1)
    case m: String if m.startsWith("map") => setStringToStringMap(m, t, kv._1)
    case "boolean" => val b = genValue("boolean").asInstanceOf[Boolean]; t.setBoolean(kv._1, b); (t, (kv._2, b))
    case "int8" => val b = genValue("int8").asInstanceOf[Byte]; t.setByte(kv._1, b); (t, (kv._2, b))
    case "uint8" => val b = genValue("uint8").asInstanceOf[Byte]; t.setByte(kv._1, b); (t, (kv._2, b))
    case "int16" => val s = genValue("int16").asInstanceOf[Short]; t.setShort(kv._1, s); (t, (kv._2, s))
    case "uint16" => val s = genValue("uint16").asInstanceOf[Short]; t.setShort(kv._1, s); (t, (kv._2, s))
    case "int32" => val i = genValue("int32").asInstanceOf[Int]; t.setInt(kv._1, i); (t, (kv._2, i))
    case "uint32" => val i = genValue("uint32").asInstanceOf[Int]; t.setInt(kv._1, i); (t, (kv._2, i))
    case "int64" => val l = genValue("int64").asInstanceOf[Long];  t.setLong(kv._1, l); (t, (kv._2, l))
    case "uint64" => val l = genValue("uint64").asInstanceOf[Long];  t.setLong(kv._1, l); (t, (kv._2, l))
    case "float32" => val f = genValue("float32").asInstanceOf[Float]; t.setFloat(kv._1, f); (t, (kv._2, f))
    case "float64" => val d = genValue("float64").asInstanceOf[Double]; t.setDouble(kv._1, d); (t, (kv._2, d))
    case "decimal32" | "decimal64" | "decimal128" => val bd = genValue("decimal32").asInstanceOf[java.math.BigDecimal]; t.setBigDecimal(kv._1, bd); (t, (kv._2, bd))
    case "rstring" | "ustring" => val s = genValue("rstring").asInstanceOf[String]; t.setString(kv._1, s); (t, (kv._2, s))
    case _ => (null, (kv._2, null))
  }




  def setStringList(l: String, t: OutputTuple, fieldName: String): (OutputTuple, (String, Any)) = {
    val splType = l.stripPrefix("list<").stripSuffix(">").trim
    val numEntries = Random.nextInt % 20
    val list = ListBuffer[String]()
    for(i <- 0 until numEntries) list += genValue("rstring").asInstanceOf[String]
    val lizt: java.util.List[String] = list.toList.asJava
    println(s"I'M ABOUT TO INSERT THIS SET INTO A TUPLE: $lizt")
    t.setList(fieldName, lizt)
    (t, (fieldName, lizt))
  }

  def setStringSet(l: String, t: OutputTuple, fieldName: String): (OutputTuple, (String, Any)) = {
    val splType = l.stripPrefix("set<").stripSuffix(">").trim
    val numEntries = Random.nextInt % 20
    val list = ListBuffer[String]()
    for(i <- 0 until numEntries) list += genValue("rstring").asInstanceOf[String]
    val set: java.util.Set[String] = list.toSet.asJava
//
//    val whatever: java.util.Set[String] = new java.util.HashSet()
//    whatever.add("aa")
//    whatever.add("bb")
    println(s"I'M ABOUT TO INSERT THIS SET INTO A TUPLE: $set")

    t.setSet(fieldName, set)
    (t, (fieldName, set))
  }

  def setStringToStringMap(l: String, t: OutputTuple, fieldName: String): (OutputTuple, (String, Any)) = {
    val (splKeyType: String, splValType: String) = {
      val sp = l.stripPrefix("map<").stripSuffix(">").trim
      val arr = sp.split(", ")
      (arr(0), arr(1))
    }
    val numEntries = Random.nextInt % 20
    val list = ListBuffer[(String, String)]()
    for(i <- 0 until numEntries) list += genValue("rstring").asInstanceOf[String] -> genValue("rstring").asInstanceOf[String]
    val map: java.util.Map[String, String] = list.toMap.asJava
    println(s"I'M ABOUT TO INSERT THIS MAP INTO A TUPLE: $map")
    t.setMap(fieldName, map)
    (t, (fieldName, map))
  }
}

class MockStreams(splStyleTupleStructureDeclaration: String) {
  private val graph: OperatorGraph = OperatorGraphFactory.newGraph()
  private val op: OperatorInvocation[CassandraSink] = graph.addOperator(classOf[CassandraSink])
  op.setStringParameter("connectionConfigZNode", "/cassConn")
  op.setStringParameter("nullMapZnode", "/nullV")
  op.setStringParameter("zkConnectionString", MockZK.connectString)
  // Create the object representing the type of tuple that is coming into the operator
  private val tuplez: InputPortDeclaration = op.addInput(splStyleTupleStructureDeclaration)
  // Create the testable version of the graph
  private val testableGraph: JavaTestableGraph  = new JavaOperatorTester().executable(graph)
  // Create the injector to inject test tuples.
  private val injector: StreamingOutput[OutputTuple] = testableGraph.getInputTester(tuplez)
  // Execute the initialization of operators within graph.
  testableGraph.initialize().get().allPortsReady().get()

  // omg can I actually get a tuple out of this???
  def newEmptyTuple(): OutputTuple = injector.newTuple()

  def shutdown(): Unit = testableGraph.shutdown().get().shutdown().get()

  def submit(tuple: OutputTuple): Unit = injector.submit(tuple)
}
