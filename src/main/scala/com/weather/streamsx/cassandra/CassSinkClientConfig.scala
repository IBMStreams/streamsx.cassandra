package com.weather.streamsx.cassandra

import io.circe._, generic.semiauto._

import java.net.InetAddress
import java.security.KeyStore
import javax.net.ssl.{SSLContext, KeyManagerFactory}

import com.datastax.driver.core.SSLOptions._
import com.datastax.driver.core.{SSLOptions, PlainTextAuthProvider, AuthProvider, ConsistencyLevel}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

//import scalaz._, Scalaz._



//
//case class DifferentCaseClass(
//                               port: Int,
//                               remapclusterminutes: Int,
//                               seeds: List[InetAddress],
//                               consistencylevel: ConsistencyLevel,
//                               authEnabled: Boolean,
//                               authUsername: String,
//                               authPassword: String,
//                               sslEnabled: Boolean,
//                               sslKeystore: String,
//                               sslPassword: String,
//                               writeOperationTimeout: Long,
//                               dateFormat: DateTimeFormatter,
//                               authProvider: AuthProvider,
//                               localDC: String,
//                               sslOptions: Option[SSLOptions],
//                               consistencyLevel: ConsistencyLevel
//                             )

case class CassSinkClientConfig(
                                 port: Int,
                                 remapclusterminutes: Int,
                                 seeds: List[InetAddress],
                                 consistencylevel: ConsistencyLevel,
                                 authEnabled: Boolean,
                                 authUsername: String,
                                 authPassword: String,
                                 sslEnabled: Boolean,
                                 sslKeystore: String,
                                 sslPassword: String,
                                 writeOperationTimeout: Long,
                                 dateFormat: DateTimeFormatter,
                                 authProvider: AuthProvider,
                                 localDC: String,
                                 sslOptions: Option[SSLOptions],
                                 consistencyLevel: ConsistencyLevel
                               )

object CassSinkClientConfig {

//  def getConsistencyLevel(key: String, cfg: Map[String, String]): Validation[String, ConsistencyLevel] = cfg.get(key) match {
//    case Some(l) =>
//      try ConsistencyLevel.valueOf(l.toUpperCase).success
//      catch { case e: Throwable => s"Error setting consistency level to $l".failure }
//    case _ => s"Key not found in config map -- $key".failure
//  }

  val DEFAULT_PORT                   = "9042"
  val DEFAULT_REMAPCLUSTERMINUTES    = "15"
  val DEFAULT_SEEDS                  = "10.0.0.2"
  val DEFAULT_CONSISTENCYLEVEL       = "local_quorum"
  val DEFAULT_AUTHENABLED            = "false"
  val DEFAULT_AUTHUSERNAME           = ""
  val DEFAULT_AUTHPASSWORD           = ""
  val DEFAULT_SSLENABLED             = "false"
  val DEFAULT_SSLKEYSTORE            = ""
  val DEFAULT_SSLPASSWORD            = ""
  val DEFAULT_WRITEOPERATIONTIMEOUT  = "10000"
  val DEFAULT_LOCALDC                = ""

  def apply(config: Map[String, String]): CassSinkClientConfig = {
    val port = config.getOrElse("port", DEFAULT_PORT).toInt
    val remapclusterminutes = config.getOrElse("remapclusterminutes", DEFAULT_REMAPCLUSTERMINUTES).toInt
    val seeds = InetAddress.getAllByName(config.getOrElse("seeds", "10.0.0.2")).toList
    val authEnabled = config.getOrElse("authEnabled", "false").toBoolean
    val authUsername = config.getOrElse("authUsername", "")
    val authPassword = config.getOrElse("authPassword", "")
    val sslEnabled = config.getOrElse("sslEnabled", "false").toBoolean
    val sslKeystore = config.getOrElse("sslKeystore", "")
    val sslPassword = config.getOrElse("sslPassword", "")
    val dateFormat = DateTimeFormat.forPattern("yy-MM-dd HH:mm:ss")
    val writeOperationTimeout = config.getOrElse("writeoperationtimeout", "10000").toLong
    val localDC = config.getOrElse("localdc", DEFAULT_LOCALDC)
    val sslOptions =  sslEnabled match {
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
    val authProvider = authEnabled match {
      case true => new PlainTextAuthProvider(authUsername, authPassword)
      case _ => AuthProvider.NONE
    }
    val consistencyLevel: ConsistencyLevel  = ConsistencyLevel.ALL

//    val consistencyLevel: ConsistencyLevel  = getConsistencyLevel("consistencylevel", config).toOption.get


    CassSinkClientConfig(
      port = port,
      remapclusterminutes = remapclusterminutes,
      seeds = seeds,
      consistencylevel = consistencyLevel,
      authEnabled = authEnabled,
      authUsername = authUsername,
      authPassword = authPassword,
      sslEnabled = sslEnabled,
      sslKeystore = sslKeystore,
      sslPassword = sslPassword,
      writeOperationTimeout = writeOperationTimeout,
      dateFormat = dateFormat,
      authProvider = authProvider,
      localDC = localDC,
      sslOptions = sslOptions,
      consistencyLevel = consistencyLevel
    )
  }


  private[cassandra] implicit val rdrDecoder: Decoder[CassSinkClientConfig] = deriveDecoder[CassSinkClientConfig]
  private[cassandra] implicit val rdrEncoder: Encoder[CassSinkClientConfig] = deriveEncoder[CassSinkClientConfig]

  def read(znode: String): Option[CassSinkClientConfig] = ZkClient.zkCli.read[CassSinkClientConfig](znode)
  def write(znode: String, cc: CassSinkClientConfig): Unit = ZkClient.zkCli.write(znode, cc)


}
