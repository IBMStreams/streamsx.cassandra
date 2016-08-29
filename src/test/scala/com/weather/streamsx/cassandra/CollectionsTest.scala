package com.weather.streamsx.cassandra

import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

class CollectionsTest extends PipelineTest(
  "testKEYS",
  "testTABLE",
  """
     |create table IF NOT EXISTS testk.testt (
     |      count bigint,
     |      map1 map<varchar, boolean>,
     |      map2  map<bigint, varchar>,
     |      list1 list<int>,
     |      list2 list<int>,
     |      set1 set<varchar>,
     |      set2 set<float>
     |  PRIMARY KEY (count)
     |) with caching = 'none';
  """.stripMargin
){

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

    val structureMap = Map(
                            "count" -> "uint64",
                            "map1" -> "map<rstring, boolean>",
                            "map2" -> "map<int64, ustring>",
                            "list1" -> "list<int32>",
                            "list2" -> "list<uint8>",
                            "set1" -> "set<rstring>",
                            "set2" -> "set<float32>"
                          )
    def row2greeting(r: Row): Map[String, Any] = {
      Map(
        "count" -> r.getLong("count"),
        "map1" -> r.getMap("map1", classOf[String], classOf[Boolean]),
        "map2" -> r.getMap("map1", classOf[Long], classOf[String]),
        "list1" -> r.getList("list1", classOf[Int]),
        "list2" -> r.getList("list2", classOf[Int]),
        "set1" -> r.getSet("set1", classOf[String]),
        "set2" -> r.getSet("set1", classOf[Float])
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
