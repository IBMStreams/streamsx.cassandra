package com.weather.streamsx.cassandra.config

import java.net.InetAddress
import java.security.KeyStore
import javax.net.ssl.{KeyManagerFactory, SSLContext}

import com.datastax.driver.core.SSLOptions._
import com.datastax.driver.core.{AuthProvider, ConsistencyLevel, PlainTextAuthProvider, SSLOptions}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scalaz.Scalaz._
import scalaz._


case class CassSinkClientConfig(

                                 localdc: String,
                                 port: Int,
                                 remapClusterMinutes: Int,
                                 writeOperationTimeout: Long,
                                 authEnabled: Boolean,
                                 authUsername: String,
                                 authPassword: String,
                                 sslEnabled: Boolean,
                                 sslKeystore: String,
                                 sslPassword: String,
                                 dateFormat: DateTimeFormatter,
                                 authProvider: AuthProvider,
                                 sslOptions: Option[SSLOptions],
                                 consistencylevel: ConsistencyLevel,
                                 seeds: List[InetAddress],
                                 keyspace: String,
                                 table: String,
                                 ttl: Long,
                                 cacheSize: Int
                               )

object CassSinkClientConfig {

  def getConsistencyLevel(key: String): Validation[String, ConsistencyLevel] =
      try ConsistencyLevel.valueOf(key.toUpperCase).success
      catch {
        case e: Throwable => s"Error setting consistency level to $key".failure
      }

  def apply(ptc: PrimitiveTypeConfig): CassSinkClientConfig = {
    val sslOptions =  ptc.sslEnabled match {
      case true if ptc.sslKeystore.isEmpty => Some(new SSLOptions)
      case true =>
        val ksp = ptc.sslPassword.toCharArray
        val ks = KeyStore.getInstance("PKCS12")
        ks.load(getClass.getResourceAsStream(ptc.sslKeystore), ksp)
        val kmf = KeyManagerFactory.getInstance("SunX509")
        kmf.init(ks, ksp)

        val ctxt = SSLContext.getInstance("TLS")
        ctxt.init(kmf.getKeyManagers, null, null)
        Some(new SSLOptions(ctxt, DEFAULT_SSL_CIPHER_SUITES))
      case _ => None
    }
    val authProvider = ptc.authEnabled match {
      case true => new PlainTextAuthProvider(ptc.authUsername, ptc.authPassword)
      case _ => AuthProvider.NONE
    }
    val seeds = InetAddress.getAllByName(ptc.seeds).toList

    CassSinkClientConfig(
      localdc = ptc.localdc,
      port = ptc.port,
      remapClusterMinutes = ptc.remapClusterMinutes,
      writeOperationTimeout = ptc.writeOperationTimeout,
      authEnabled = ptc.authEnabled,
      authUsername = ptc.authUsername,
      authPassword = ptc.authPassword,
      sslEnabled = ptc.sslEnabled,
      sslKeystore = ptc.sslKeystore,
      sslPassword = ptc.sslPassword,
      dateFormat = DateTimeFormat.forPattern(ptc.dateFormat),
      authProvider = authProvider,
      sslOptions = sslOptions,
      consistencylevel = getConsistencyLevel(ptc.consistencyLevel).toOption.get,
      seeds = seeds,
      keyspace = ptc.keyspace,
      table = ptc.table,
      ttl = ptc.ttl,
      cacheSize = ptc.cacheSize
    )
  }



}
