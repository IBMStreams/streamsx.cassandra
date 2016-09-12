package com.weather.streamsx.cassandra

import org.cassandraunit.utils.{EmbeddedCassandraServerHelper => helper}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

// I have yet to find a library for embedded Cassandra for unit testing that doesn't SUCK.
// I'm running cassandra locally while I run tests, which is not ideal, but it's better than these buggy Cassandra libraries.

object MockCassandra {

////  private val helper = EmbeddedCassandraServerHelper
//
//  def start(): Unit = helper.startEmbeddedCassandra("test-cassandra.yml")
//
//  //  def stop(): Unit = helper.stopEmbeddedCassandra() // The stop method was deprecated in cassandra-unit with no replacement
//  def clean(): Unit = helper.cleanEmbeddedCassandra()
//
//  start()
//  val ip: String = helper.getHost
//  val port: Int = helper.getNativeTransportPort // This is the CQL port

  val ip = "127.0.0.1,10.0.2.2"
  val port = 9042
}

//
//@RunWith(classOf[JUnitRunner])
//class MockCassandraTest extends FlatSpec with Matchers with BeforeAndAfterAll {
//  override def beforeAll() {
//    MockCassandra.start()
//  }
//
//  override def afterAll() {
//    MockCassandra.clean()
//  }
//
//  "MockCassandra" should "be running" in {
//    MockCassandra.ip should be
//    MockCassandra.port should be
//  }
//
//
//
//}