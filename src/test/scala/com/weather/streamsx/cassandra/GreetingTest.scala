//package com.weather.streamsx.cassandra
//
//import com.datastax.driver.core.Row
//
//import scala.collection.JavaConverters._
//
//class GreetingTest extends PipelineTest(
//  "testk",
//  "testt",
//  """
//     |create table IF NOT EXISTS testk.testt (
//     |  greeting varchar,
//     |  count bigint,
//     |  cool varchar,
//     |  PRIMARY KEY (count)
//     |) with caching = 'none';
//  """.stripMargin
//){
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//  }
//
//  override def afterAll(): Unit = {
//    super.afterAll()
//  }
//
//    val structureMap = Map( "greeting" -> "rstring",
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
//  "The operator" should "write only one tuple to C*" in {
//    val (tuple, valuesMap) = genAndSubmitTuple(structureMap)
//    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
//    val received = row2greeting(rows.head)
//
//    rows should have size 1
//    received shouldBe valuesMap
//  }
//}
