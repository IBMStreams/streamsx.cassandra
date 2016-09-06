package com.weather.streamsx.cassandra.mock

import com.weather.streamsx.cassandra.connection.ZKClient
import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer

class MockZK {
  private val topLevelZnode = "streamsx.cassandra"
  private val zkPort = 4446
  private val retryMS = 100000
  private val zkTestServer = new TestingServer(zkPort)


  private[cassandra] val cli: CuratorFramework = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString, new RetryOneTime(retryMS))

  val connectString = zkTestServer.getConnectString

  val zkCli = ZKClient(s"$topLevelZnode", Some(connectString))

  def start(): Unit = {
    zkTestServer.start()
    cli.start()
    cli.create().forPath(s"/$topLevelZnode")
  }

  def createZNode(path: String, content: String): Unit = {
    cli.create().forPath(s"/$topLevelZnode$path", content.getBytes)
  }

  def deleteZnode(path: String): Unit = {
    cli.delete().forPath(s"/$topLevelZnode$path")
  }


  def getZNode(path: String): String = {
    zkCli.readRawString(path).get
  }

  def shutdown(): Unit = {
    cli.close()
    zkTestServer.close()
  }
}
