package com.weather.streamsx.cassandra

import java.sql.Blob

import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

class AllPrimitiveTypesTest extends PipelineTest(
  "testk",
  "testt",
  """
     |create table IF NOT EXISTS testk.testt (
     |boolean boolean,
     |int8 int,
     |uint8 int,
     |int16 int,
     |uint16 int,
     |int32 int,
     |uint32 int,
     |int64 varint,
     |uint64 varint,
     |float32 float,
     |float64 double,
     |decimal32 decimal,
     |decimal64 decimal,
     |decimal128 decimal,
     |rstring varchar,
     |ustring varchar,
     |xml varchar
     |  PRIMARY KEY (int32)
     |) with caching = 'none';
  """.stripMargin
){

    val structureMap = Map(
      "boolean"    -> "boolean",
      "int8"       -> "int8",
      "uint8"      -> "uint8",
      "int16"      -> "int16",
      "uint16"     -> "uint16",
      "int32"      -> "int32",
      "uint32"     -> "uint32",
      "int64"      -> "int64",
      "uint64"     -> "uint64",
      "float32"    -> "float32",
      "float64"    -> "float64",
      "decimal32"  -> "decimal32",
      "decimal64"  -> "decimal64",
      "decimal128" -> "decimal128",
//      "timestamp"  -> "timestamp",
      "rstring"    -> "rstring",
      "ustring"    -> "ustring",
//      "blob"       -> "blob",
      "xml"        -> "xml"
    )
    def row2primitiveTypes(r: Row): Map[String, Any] = {
      Map(
        "boolean"    -> r.getBool("boolean"),
        "int8"       -> r.getInt("int8"),
        "uint8"      -> r.getInt("uint8"),
        "int16"      -> r.getInt("int16"),
        "uint16"     -> r.getInt("uint16"),
        "int32"      -> r.getInt("int32"),
        "uint32"     -> r.getInt("uint32"),
        "int64"      -> r.getVarint("int64"),
        "uint64"     -> r.getVarint("uint64"),
        "float32"    -> r.getFloat("float32"),
        "float64"    -> r.getDouble("float64"),
        "decimal32"  -> r.getDecimal("decimal32"),
        "decimal64"  -> r.getDecimal("decimal64"),
        "decimal128" -> r.getDecimal("decimal128"),
//        "timestamp"  -> r.get("timestamp",
        "rstring"    -> r.getString("rstring"),
        "ustring"    -> r.getString("ustring"),
//        "blob"       -> r.getBytes("blob").asInstanceOf[Blob],
        "xml"        -> r.getString("xml")
      )
    }

    val (tuple, valuesMap) = genAndSubmitTuple(structureMap)
    val rows: Seq[Row] = session.execute(s"select * from $keyspace.$table").all.asScala.toSeq
    val received = row2primitiveTypes(rows.head)

  "The operator" should "write only one tuple to C*" in {
    rows should have size 1
  }

  it should "match the random values assigned" in {
    received shouldBe valuesMap
  }

  
  
  
  
  
  
  
}


//
//"boolean"    -> "boolean"
//"int8"       -> "int8"
//"uint8"      -> "uint8"
//"int16"      -> "int16"
//"uint16"     -> "uint16"
//"int32"      -> "int32"
//"uint32"     -> "uint32"
//"int64"      -> "int64"
//"uint64"     -> "uint64"
//"float32"    -> "float32"
//"float64"    -> "float64"
//"decimal32"  -> "decimal32"
//"decimal64"  -> "decimal64"
//"decimal128" -> "decimal128"
//"timestamp"  -> "timestamp"
//"rstring"    -> "rstring"
//"ustring"    -> "ustring"
//"blob"       -> "blob"
//"xml"        -> "xml"
//
//
//
//boolean boolean,
//int8 int,
//uint8 int,
//int16 int,
//uint16 int,
//int32 int,
//uint32 int,
//int64 varint,
//uint64 varint,
//float32 float,
//float64 double,
//decimal32 decimal,
//decimal64 decimal,
//decimal128 decimal,
//timestamp timestamp,
//rstring varchar,
//ustring varchar,
//blob blob,
//xml varchar