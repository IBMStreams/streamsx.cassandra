package com.weather.streamsx.cassandra

import com.datastax.driver.core.Row
import com.ibm.streams.operator.{OutputTuple, Tuple}
import com.weather.streamsx.cassandra.mock.MockStreams

import scala.collection.JavaConverters._

  object NullWriteTest {
    val table = "testkEEEE"
    val keyspace = "testtAAAA"
    val tableStr =   s"""
                       |create table IF NOT EXISTS $keyspace.$table (
                       |  count bigint,
                       |  notnull int,
                       |  nullz int,
                       |  PRIMARY KEY (count)
                       |) with caching = 'none';
                     """.stripMargin

    val nullVals =
      s"""
         |{
         |  "nullz" : ${Int.MaxValue}
         |}
       """.stripMargin
  }


class NullWriteTest extends PipelineTest(
  NullWriteTest.keyspace,
  NullWriteTest.table,
  NullWriteTest.tableStr,
  NullWriteTest.nullVals
){

  override def beforeAll(): Unit = {
    println("I'M CALLING BEFOREALL FOR GREETING TEST!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    println("I'M CALLING AFTERALL FOR GREETING TEST!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    super.afterAll()
  }

    val structureMap = Map( "count" -> "uint64",
                            "notnull" -> "int32",
                            "nullz" -> "int32"
                          )
    def row2greeting(r: Row): Map[String, Any] = {
      Map(
        "count" -> r.getLong("count"),
        "notnull" -> r.getInt("notnull"),
        "nullz" -> r.getInt("nullz")
      )
    }

  def genTuple(m: Map[String, String]): (Tuple, Map[String, Any]) = {
    val tupleStructure = {
      val tupleOpen = "tuple<"
      val meat = m.map(kv => s"${kv._2} ${kv._1}").mkString(", ")
      val tupleClose = ">"
      s"$tupleOpen$meat$tupleClose"
    }
    println(s"THIS IS MY TUPLE STRUCTURE: $tupleStructure")
    val generator = new MockStreams(tupleStructure, mockZK.connectString)
    val t = generator.newEmptyTuple()

    t.setLong("count", 12345678L)
    t.setInt("notnull", Int.MaxValue)
    t.setInt("nullz", Int.MaxValue)

    generator.submit(t)

    (t, Map(
      "count" -> 12345678L,
      "notnull" -> Int.MaxValue,
      "nullz" -> Int.MaxValue
    ))

  }

  "The operator" should "write only one tuple to C*" in {
    val (tuple, valuesMap) = genTuple(structureMap)




    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
    println(s"I GOT BACK FROM THE NULL WRITE TEST: $rows")
    val received = row2greeting(rows.head)

    rows should have size 1
    received("count") shouldBe valuesMap("count")
    received("notnull") shouldBe valuesMap("notnull")
    received("nullz") shouldNot be
  }
}
