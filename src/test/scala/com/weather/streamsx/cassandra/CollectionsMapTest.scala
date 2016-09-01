//package com.weather.streamsx.cassandra
//
//import com.datastax.driver.core.Row
//
//import scala.collection.JavaConverters._
//
//// just set
//class CollectionsMapTest extends PipelineTest(
//  "testKEYSMAP",
//  "testTABLEMAP",
//  """
//     |create table IF NOT EXISTS testKEYSLIST.testTABLEMAP (
//     |      count bigint,
//     |      mapA map<varchar, varchar>,
//     |  PRIMARY KEY (count)
//     |) with caching = 'none';
//  """.stripMargin
//){
//
//  override def beforeAll(): Unit = {
//    println("I'M CALLING BEFORE ALL FOR THE MAP TEST")
//    super.beforeAll()
//  }
//
//  override def afterAll(): Unit = {
//    println("I'M CALLING AFTER ALL FOR THE MAP TEST")
//    super.afterAll()
//  }
//
//    val structureMap = Map(
//                            "count" -> "uint64",
//                            "mapA"  -> "map<rstring, rstring>"
//                          )
//    def row2greeting(r: Row): Map[String, Any] = {
//      Map(
//        "count" -> r.getLong("count"),
//        "mapA" -> r.getMap("mapA", classOf[String], classOf[String])
//      )
//    }
//
//  "The operator" should "write only one tuple to C*" in {
//    val (tuple, valuesMap) = genAndSubmitTuple(structureMap)
//    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
//    val received = row2greeting(rows.head)
//
//    rows should have size 1
//    received shouldBe valuesMap
//  }
//}
