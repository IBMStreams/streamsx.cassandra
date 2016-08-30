package com.weather.streamsx.cassandra

import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

class CollectionsTest extends PipelineTest(
  "testKEYS",
  "testTABLE",
  """
     |create table IF NOT EXISTS testKEYS.testTABLE (
     |      count bigint,
     |      mapA map<varchar, boolean>,
     |      mapB  map<bigint, varchar>,
     |      listA list<int>,
     |      listB list<int>,
     |      setA set<varchar>,
     |      setB set<float>,
     |  PRIMARY KEY (count)
     |) with caching = 'none';
  """.stripMargin
){

  override def beforeAll(): Unit = {
    println("I'M CALLING BEFORE ALL FOR THE COLLECTIONS TEST")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    println("I'M CALLING AFTER ALL FOR THE COLLECTIONS TEST")
    super.afterAll()
  }

    val structureMap = Map(
                            "count" -> "uint64",
                            "mapA"  -> "map<rstring, boolean>",
                            "mapB"  -> "map<int64, rstring>",
                            "listA" -> "list<int32>",
                            "listB" -> "list<uint8>",
                            "setA"  -> "set<rstring>",
                            "setB"  -> "set<float32>"
                          )
    def row2greeting(r: Row): Map[String, Any] = {
      Map(
        "count" -> r.getLong("count"),
        "map1" -> r.getMap("mapA", classOf[String], classOf[Boolean]),
        "map2" -> r.getMap("mapB", classOf[Long], classOf[String]),
        "list1" -> r.getList("listA", classOf[Int]),
        "list2" -> r.getList("listB", classOf[Int]),
        "set1" -> r.getSet("setA", classOf[String]),
        "set2" -> r.getSet("setB", classOf[Float])
      )
    }

  "The operator" should "write only one tuple to C*" in {
    val (tuple, valuesMap) = genAndSubmitTuple(structureMap)
    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
    val received = row2greeting(rows.head)

    rows should have size 1
    received shouldBe valuesMap
  }
}
