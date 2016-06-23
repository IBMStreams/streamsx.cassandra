package com.weather.streamsx.cassandra.config

import com.typesafe.config.Config
import com.weather.configuration.ConfigurationContainer
import com.weather.configuration.Hokum.{getConfig => C}
import org.joda.time.format.DateTimeFormat


abstract class Cfg(path: String) extends ConfigurationContainer {
  protected val config = StreamsCassConfig.cfg.getObject(s"streams.$path").toConfig
  def getOptionInt(c: Config, f: String) = if (c.hasPath(f)) Some(c.getInt(f)) else None
  def getOptionString(c: Config, f: String) = if (c.hasPath(f)) Some(c.getInt(f)) else None
  def getOrDefaultInt(c: Config, f: String, dflt: Int) = if (c.hasPath(f)) c.getInt(f) else dflt
}

object StreamsCassConfig {
  val cfg = C("streams", fileExtension = "conf")

  def apply() {}

  object dateutcfmt {
    val dtFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  }


  object cassanalytics extends CassandraConfig("cassandra-analytics")
  object cassun extends CassandraConfig("cassandra-sun") {
    val enableObsWrite = getOrDefault("enableobswrite", default = true)
  }

}