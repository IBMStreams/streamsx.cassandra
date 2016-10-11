package com.weather.streamsx.cassandra.zooklient

import scala.collection.JavaConverters._

case class ZooKlientCfg(
                         applicationNamespace: String,
                         connectString: String,
                         scheme: Option[String] = None,
                         auth: Option[String] = None,
                         connectionTimeoutMs: Int = ZooKlientCfg.DEFAULT_CONNECTION_TIMEOUT_MS,
                         sessionTimeoutMs: Int = ZooKlientCfg.DEFAULT_SESSION_TIMEOUT_MS,
                         backoffBaseSleepTimeMs: Int = ZooKlientCfg.DEFAULT_BACKOFF_BASE_SLEEP_TIME_MS,
                         backoffMaxRetries: Int = ZooKlientCfg.DEFAULT_BACKOFF_MAX_RETRIES
                       )

object ZooKlientCfg {
  private def getOrThrow(c: Map[String, String])(key: String): String = c.get(key) match {
    case Some(a) => a
    case _ => throw new Exception(s"$key must be set")
  }

  private val DEFAULT_BACKOFF_BASE_SLEEP_TIME_MS = 1000
  private val DEFAULT_BACKOFF_MAX_RETRIES = 3
  private val DEFAULT_CONNECTION_TIMEOUT_MS = 10000
  private val DEFAULT_SESSION_TIMEOUT_MS = 10000

  def apply(cfg: java.util.Map[String, String]): ZooKlientCfg = {
    val cm = cfg.asScala.toMap
    val c = getOrThrow(cm)_
    ZooKlientCfg(
      c("applicationnamespace"),
      c("connectstring"),
      cm.get("scheme"),
      cm.get("auth"),
      cm.getOrElse("connectiontimeoutms", DEFAULT_CONNECTION_TIMEOUT_MS.toString).toInt,
      cm.getOrElse("sessiontimeoutms", DEFAULT_SESSION_TIMEOUT_MS.toString).toInt,
      cm.getOrElse("backoffbasesleeptimems", DEFAULT_BACKOFF_BASE_SLEEP_TIME_MS.toString).toInt,
      cm.getOrElse("backoffmaxretries", DEFAULT_BACKOFF_MAX_RETRIES.toString).toInt
    )
  }
}