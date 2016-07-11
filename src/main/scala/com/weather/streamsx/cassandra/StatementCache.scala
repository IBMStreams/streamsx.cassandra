package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, PreparedStatement}
import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}

class StatementCache(table: String, keyspace: String, ttl: Long, session: Session) {

  private val loader: CacheLoader[Map[String, Boolean], PreparedStatement] = new CacheLoader[Map[String, Boolean], PreparedStatement] {
    override def load(key: Map[String, Boolean]): PreparedStatement = loaderFn(key)
  }

  private val loaderFn = (m: Map[String, Boolean]) => {
    val keys = m.filter(_._2).keys.toList.sorted
    val fieldStr = keys.mkString(",")
    val q = ("?" * keys.length).mkString(",") //todo look into Stream.continually.take(keys.length)
    val tableSpec = if (keyspace.isEmpty) table else s"$keyspace.$table"
    val insertStr = s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL $ttl"""
//    println(insertStr)
    session.prepare(insertStr)
  }

  private val cache: LoadingCache[Map[String, Boolean], PreparedStatement] = CacheBuilder.newBuilder()
    .maximumSize(1000) //TODO: make this a parameter
    .build(loader)

  def get(m: Map[String, Boolean]): PreparedStatement = cache.get(m)
}
