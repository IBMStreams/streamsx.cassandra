package com.weather.streamsx.cassandra.mock

import com.weather.streamsx.cassandra.connection.ZKClient
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer

object MockZK {
  private val topLevelZnode = "streamsx.cassandra"
  private val zkPort = 4445
  private val retryMS = 2000
  private val zkTestServer = new TestingServer(zkPort)
  private[cassandra] val cli: CuratorFramework = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString, new RetryOneTime(retryMS))

  val connectString = zkTestServer.getConnectString

  cli.start()

  val zkCli = ZKClient(s"$topLevelZnode", Some(connectString))
  cli.create().forPath(s"/$topLevelZnode")

  def start(): Unit = {
    cli.start()
  }

  def createZNode(path: String, content: String): Unit = {
    cli.create().forPath(s"/$topLevelZnode$path", content.getBytes)
  }

  def getZNode(path: String): String = {
    zkCli.readRawString(path).get
  }

  def shutdown(): Unit = {
    cli.close()
  }
}
