//package com.weather.streamsx.cassandra
//
//import com.datastax.driver.core.Row
//import scala.collection.JavaConverters._
//
//class AllPrimitiveTypesTest extends PipelineTest(
//  "testkeyspace",
//  "testtable",
//  """
//     |create table IF NOT EXISTS testkeyspace.testtable (
//     |boolean boolean,
//     |int8 int,
//     |uint8 int,
//     |int16 int,
//     |uint16 int,
//     |int32 int,
//     |uint32 int,
//     |int64 bigint,
//     |uint64 bigint,
//     |float32 float,
//     |float64 double,
//     |decimal32 decimal,
//     |decimal64 decimal,
//     |decimal128 decimal,
//     |rstring varchar,
//     |ustring varchar,
//     |  PRIMARY KEY (int32)
//     |) with caching = 'none';
//  """.stripMargin
//){
//
//  override def beforeAll(): Unit = {
//    println("I'M CALLING BEFORE ALL FOR THE PRIMITIVE TYPES TEST")
//    super.beforeAll()
//  }
//
//  override def afterAll(): Unit = {
//    println("I'M CALLING AFTER ALL FOR THE PRIMITIVE TYPE TEST")
//    super.afterAll()
//  }
//
//    val structureMap = Map(
//      "boolean"    -> "boolean",
//      "int8"       -> "int8",
//      "uint8"      -> "uint8",
//      "int16"      -> "int16",
//      "uint16"     -> "uint16",
//      "int32"      -> "int32",
//      "uint32"     -> "uint32",
//      "int64"      -> "int64",
//      "uint64"     -> "uint64",
//      "float32"    -> "float32",
//      "float64"    -> "float64",
//      "decimal32"  -> "decimal32",
//      "decimal64"  -> "decimal64",
//      "decimal128" -> "decimal128",
//      "rstring"    -> "rstring",
//      "ustring"    -> "ustring"
//    )
//    def row2primitiveTypes(r: Row): Map[String, Any] = {
//      Map(
//        "boolean"    -> r.getBool("boolean"),
//        "int8"       -> r.getInt("int8"),
//        "uint8"      -> r.getInt("uint8"),
//        "int16"      -> r.getInt("int16"),
//        "uint16"     -> r.getInt("uint16"),
//        "int32"      -> r.getInt("int32"),
//        "uint32"     -> r.getInt("uint32"),
//        "int64"      -> r.getLong("int64"),
//        "uint64"     -> r.getLong("uint64"),
//        "float32"    -> r.getFloat("float32"),
//        "float64"    -> r.getDouble("float64"),
//        "decimal32"  -> r.getDecimal("decimal32"),
//        "decimal64"  -> r.getDecimal("decimal64"),
//        "decimal128" -> r.getDecimal("decimal128"),
//        "rstring"    -> r.getString("rstring"),
//        "ustring"    -> r.getString("ustring")
//      )
//    }
//
//  "The operator" should "write only one tuple to C*" in {
//    val (tuple, valuesMap) = genAndSubmitTuple(structureMap)
//    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
//
//    rows should have size 1
//    val received = row2primitiveTypes(rows.head)
//
//    received("boolean") shouldBe valuesMap("boolean")
//    received("int8") shouldBe valuesMap("int8")
//    received("uint8") shouldBe valuesMap("uint8")
//    received("int16") shouldBe valuesMap("int16")
//    received("uint16") shouldBe valuesMap("uint16")
//    received("int32") shouldBe valuesMap("int32")
//    received("uint32") shouldBe valuesMap("uint32")
//    received("int64") shouldBe valuesMap("int64")
//    received("uint64") shouldBe valuesMap("uint64")
//    received("float32") shouldBe valuesMap("float32")
//    received("float64") shouldBe valuesMap("float64")
//    scala.math.BigDecimal(received("decimal32").asInstanceOf[java.math.BigDecimal]) shouldBe scala.math.BigDecimal(valuesMap("decimal32").asInstanceOf[java.math.BigDecimal]) +- 0.000001
//    scala.math.BigDecimal(received("decimal64").asInstanceOf[java.math.BigDecimal]) shouldBe scala.math.BigDecimal(valuesMap("decimal64").asInstanceOf[java.math.BigDecimal]) +- 0.000001
//    scala.math.BigDecimal(received("decimal128").asInstanceOf[java.math.BigDecimal]) shouldBe scala.math.BigDecimal(valuesMap("decimal128").asInstanceOf[java.math.BigDecimal]) +- 0.000001
//    received("rstring") shouldBe valuesMap("rstring")
//    received("ustring") shouldBe valuesMap("ustring")
//  }
//}
