package com.weather.streamsx.cassandra

import org.cassandraunit.utils.{EmbeddedCassandraServerHelper => helper}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner

object MockCassandra {

//  private val helper = EmbeddedCassandraServerHelper

  def start(): Unit = helper.startEmbeddedCassandra()
//  def stop(): Unit = helper.stopEmbeddedCassandra() // The stop method was deprecated in cassandra-unit with no replacement
  def clean(): Unit = helper.cleanEmbeddedCassandra()

  start()
  val ip: String = helper.getHost
  val port: Int = helper.getNativeTransportPort // This is the CQL port
}


@RunWith(classOf[JUnitRunner])
class MockCassandraTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  override def beforeAll() {
//    MockCassandra.start()
  }

  override def afterAll() {
    MockCassandra.clean()
  }

  "MockCassandra" should "be running on localhost:9142" in {
    MockCassandra.ip shouldBe "localhost"
    MockCassandra.port shouldBe 9142
  }

  it should "be printing output on these tests, where is my output??" in {
    2 + 2 shouldBe 4
  }
}