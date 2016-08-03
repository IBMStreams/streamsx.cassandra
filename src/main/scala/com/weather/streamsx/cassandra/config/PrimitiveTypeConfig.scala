package com.weather.streamsx.cassandra.config

import com.weather.analytics.zooklient.ZooKlient
import io.circe._
import io.circe.generic.semiauto._


case class PrimitiveTypeConfig(
                                consistencyLevel: String,
                                dateFormat: String,
                                localdc: String,
                                port: Int,
                                remapClusterMinutes: Int,
                                seeds: String,
                                writeOperationTimeout: Long,
                                authEnabled: Boolean,
                                authUsername: String,
                                authPassword: String,
                                sslEnabled: Boolean,
                                sslKeystore: String,
                                sslPassword: String,
                                keyspace: String,
                                table: String,
                                ttl: Long,
                                cacheSize: Int
                              )

object PrimitiveTypeConfig {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)



  val DEFAULT_CONSISTENCYLEVEL       = "local_quorum"
  val DEFAULT_DATEFORMAT             = "yy-MM-dd HH:mm:ss"
  val DEFAULT_PORT                   = "9042"
  val DEFAULT_REMAPCLUSTERMINUTES    = "15"
  val DEFAULT_SEEDS                  = "10.0.0.2"
  val DEFAULT_AUTHENABLED            = "false"
  val DEFAULT_AUTHUSERNAME           = ""
  val DEFAULT_AUTHPASSWORD           = ""
  val DEFAULT_SSLENABLED             = "false"
  val DEFAULT_SSLKEYSTORE            = ""
  val DEFAULT_SSLPASSWORD            = ""
  val DEFAULT_WRITEOPERATIONTIMEOUT  = "10000"
  val DEFAULT_LOCALDC                = ""
  val DEFAULT_KEYSPACE               = ""
  val DEFAULT_TABLE                  = ""
  val DEFAULT_TTL                    = "2592000"
  val DEFAULT_CACHESIZE              = "1000"

  def apply(config: Map[String, String]): PrimitiveTypeConfig = {

    log.error(s"$config")
    println(s"THIS IS WHAT I PULLED AS THE CONFIG FROM ZOOKEEPER: $config")

    val consistencyLevel = config.getOrElse("consistencyLevel", DEFAULT_CONSISTENCYLEVEL)
    val dateFormat = config.getOrElse("dateFormat", DEFAULT_DATEFORMAT)
    val localDC = config.getOrElse("localdc", DEFAULT_LOCALDC)
    val port = config.getOrElse("port", DEFAULT_PORT).toInt
    val remapclusterminutes = config.getOrElse("remapclusterminutes", DEFAULT_REMAPCLUSTERMINUTES).toInt
    val seeds = config.getOrElse("seeds", DEFAULT_SEEDS)
    val writeOperationTimeout = config.getOrElse("writeoperationtimeout", DEFAULT_WRITEOPERATIONTIMEOUT).toLong
    val authEnabled = config.getOrElse("authEnabled", DEFAULT_AUTHENABLED).toBoolean
    val authUsername = config.getOrElse("authUsername", DEFAULT_AUTHUSERNAME)
    val authPassword = config.getOrElse("authPassword", DEFAULT_AUTHPASSWORD)
    val sslEnabled = config.getOrElse("sslEnabled", DEFAULT_SSLENABLED).toBoolean
    val sslKeystore = config.getOrElse("sslKeystore", DEFAULT_SSLKEYSTORE)
    val sslPassword = config.getOrElse("sslPassword", DEFAULT_SSLPASSWORD)
    val keyspace = config.getOrElse("keyspace", DEFAULT_KEYSPACE)
    val table = config.getOrElse("table", DEFAULT_TABLE)
    val ttl = config.getOrElse("ttl", DEFAULT_TTL).toLong
    val cacheSize = config.getOrElse("cacheSize", DEFAULT_CACHESIZE).toInt

    PrimitiveTypeConfig(
      consistencyLevel,
      dateFormat,
      localDC,
      port,
      remapclusterminutes,
      seeds,
      writeOperationTimeout,
      authEnabled,
      authUsername,
      authPassword,
      sslEnabled,
      sslKeystore,
      sslPassword,
      keyspace,
      table,
      ttl,
      cacheSize
    )
  }

  private[cassandra] implicit val rdrDecoder: Decoder[PrimitiveTypeConfig] = deriveDecoder[PrimitiveTypeConfig]
  private[cassandra] implicit val rdrEncoder: Encoder[PrimitiveTypeConfig] = deriveEncoder[PrimitiveTypeConfig]

  def read(zkCli: ZooKlient, znode: String): Option[PrimitiveTypeConfig] = zkCli.read[PrimitiveTypeConfig](znode)

}
