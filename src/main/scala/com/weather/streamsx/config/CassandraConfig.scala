package com.weather.streamsx.config

import com.datastax.driver.core.SSLOptions._
import com.datastax.driver.core.{AuthProvider, ConsistencyLevel, PlainTextAuthProvider, SSLOptions}
import com.typesafe.config.Config
import com.weather.configuration.{ConfigurationContainer, CassandraConfiguration}
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scalaz._, Scalaz._

object CassandraConfig {
  def getConsistencyLevel(key: String, cfg: Map[String, String]): Validation[String, ConsistencyLevel] = cfg.get(key) match {
    case Some(l) =>
      try ConsistencyLevel.valueOf(l.toUpperCase).success
      catch { case e: Throwable => s"Error setting consistency level to $l".failure }
    case _ => s"Key not found in config map -- $key".failure
  }
}

class CassandraConfig(cname: String) extends Cfg(cname) with CassandraConfiguration {
  val authEnabled = config.getBoolean("auth.enabled")
  val authUsername = config.getString("auth.username")
  val authPassword = config.getString("auth.password")

  val authProvider = authEnabled match {
    case true => new PlainTextAuthProvider(authUsername, authPassword)
    case _ => AuthProvider.NONE
  }

  val consistencyLevel = ConsistencyLevel.valueOf(getOrDefault("consistencylevel", "one").toUpperCase)
  val localdc = config.getString("localdc")

  val sslEnabled = config.getBoolean("ssl.enabled")
  val sslKeystore = config.getString("ssl.keystore")
  val sslPassword = config.getString("ssl.password")

  val sslOptions = sslEnabled match {
    case true if sslKeystore.isEmpty => Some(new SSLOptions)
    case true =>
      val ksp = sslPassword.toCharArray
      val ks = KeyStore.getInstance("PKCS12")
      ks.load(getClass.getResourceAsStream(sslKeystore), ksp)
      val kmf = KeyManagerFactory.getInstance("SunX509")
      kmf.init(ks, ksp)

      val ctxt = SSLContext.getInstance("TLS")
      ctxt.init(kmf.getKeyManagers, null, null)
      Some(new SSLOptions(ctxt, DEFAULT_SSL_CIPHER_SUITES))
    case _ => None
  }
}