package com.weather.streamsx.cassandra

import com.datastax.driver.core.Session.State
import com.datastax.driver.core._
import com.google.common.util.concurrent.ListenableFuture
import com.ibm.streams.operator.{Type, Attribute}
import com.weather.streamsx.cassandra.connection.CassandraConnector
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

import scala.collection.immutable.BitSet

@RunWith(classOf[JUnitRunner])
class StatementCacheTest extends FlatSpec with Matchers with BeforeAndAfterAll{

  val m = Map(
    1 -> "a",
    2 -> "b",
    3 -> "c",
    4 -> "d",
    5 -> "e"
  )

  val mm = Map(
    "a" -> "A",
    "b" -> "B",
    "c" -> "C",
    "d" -> "D",
    "e" -> "E"
  )

  val nullValueMap = Map("a" -> "null", "b" -> "")

  val fdh = new FakeDualHash(m)

  val bitSet01 = BitSet(1, 2, 3, 4, 5)
  val bitSet02 = BitSet(2, 4)

  val cfg = MockCassandra.clientConfig

  val cache = new StatementCache(
    cfg,
    new CassandraConnector(cfg).session,
    fdh)
//
//  "The cache loading function" should "make a correct prepared statement" in {
//    val statement = cache.mkInsertStatement(bitSet01)
//    val handBuildStatement =
//      s"INSERT INTO ${cfg.keyspace}.${cfg.table} (a,b,c,d,e) VALUES (?,?,?,?,?) USING TTL ${cfg.ttl}"
//    statement should equal(handBuildStatement)
//  }
//
//  it should "properly handle omitted fields" in {
//    val statement = cache.mkInsertStatement(bitSet02)
//    val handBuildStatement =
//      s"INSERT INTO ${cfg.keyspace}.${cfg.table} (b,d) VALUES (?,?) USING TTL ${cfg.ttl}"
//    statement should equal(handBuildStatement)
//  }
//
//  "StatementToTuple" should "bind the right values to the right fields" in {
//    TupleToStatement.indexMap = fdh
//    val boundStatement = TupleToStatement.mkBoundStatement(mm, nullValueMap, cache)
//    val handBuiltStatement = "hahaha"
//    println(s"bound statement is ${boundStatement.preparedStatement().toString}")
//    boundStatement should equal(handBuiltStatement)
//  }

}

class FakeDualHash(m: Map[Int, String]) extends DualHashy{
  val nameToInt: Map[String, Int] = m.map(kv => kv._2 -> kv._1)
  val intToName: Map[Int, String] = m

  def apply(s: String): Option[Int] = nameToInt(s) match {
    case i: Int => Some(i)
    case _ => None
  }
  def apply(i: Int): Option[String] = intToName(i) match {
    case s: String => Some(s)
    case _ => None
  }
}