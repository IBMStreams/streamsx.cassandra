package com.weather.streamsx.cassandra.zooklient

import java.nio.charset.StandardCharsets.UTF_8
import cats.data.Xor
import io.circe._
import io.circe.syntax._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

case class ZooKlient(cfg: ZooKlientCfg) {
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
  log.info(s"ZkCfg: $cfg")

  private val retryPolicy = new ExponentialBackoffRetry(cfg.backoffBaseSleepTimeMs, cfg.backoffMaxRetries)
  private val cb = CuratorFrameworkFactory.builder()
    .connectString(cfg.connectString)
    .retryPolicy(retryPolicy)
    .connectionTimeoutMs(cfg.connectionTimeoutMs)
    .sessionTimeoutMs(cfg.sessionTimeoutMs)
    .namespace(cfg.applicationNamespace)

  val client = ((cfg.scheme, cfg.auth) match {
    case (Some(scheme), Some(auth)) => cb.authorization(scheme, auth.getBytes(UTF_8))
    case _ => cb
  }).build()

  client.start()

  def shutdown(): Unit =
    try client.close()
    catch { case e: Throwable => log.error("Failed to close zookeeper client", e) }

  private def fromJson[T](js: String)(implicit decoder: Decoder[T]): Option[T] = try jawn.decode[T](js) match {
    case Xor.Right(o) => Some(o)
    case Xor.Left(e) => log.error(s"decode error.\n$js", e); None
  } catch { case e: Exception => log.error(s"deserialization error.\n$js", e); None }

  def read[T](znode: String)(implicit decoder: Decoder[T]): Option[T] =
    try fromJson(new String(client.getData.forPath(znode), UTF_8))
    catch { case e: Throwable => log.error(s"Failed to getData from $znode. Check the connection parameters: $cfg", e); None }

  def write[T](znode: String, data: T)(implicit encoder: Encoder[T]): Unit = try {
    Option(client.checkExists.forPath(znode)).getOrElse(client.create.forPath(znode))
    client.setData().forPath(znode, data.asJson.noSpaces.getBytes(UTF_8))
  } catch { case e: Throwable => log.error(s"Failed to write to $znode", e) }

  def readRawString(znode: String): Option[String] = {
    try Some(new String(client.getData.forPath(znode), UTF_8))
    catch { case e: Throwable => log.error(s"Failed to getData from $znode. Check the connection parameters: $cfg", e); None }
  }
}
