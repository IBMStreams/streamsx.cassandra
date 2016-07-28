package com.weather.streamsx.cassandra

import com.datastax.driver.core.{Session, PreparedStatement}
import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}
import com.weather.streamsx.cassandra.config.CassSinkClientConfig
import scala.collection.immutable.BitSet

class StatementCache(args: CassSinkClientConfig, session: Session, masterList: DualHash) {
  private val loader: CacheLoader[BitSet, PreparedStatement] = new CacheLoader[BitSet, PreparedStatement] {
    override def load(key: BitSet): PreparedStatement = loaderFn(key)
  }

  private[cassandra] val loaderFn = (b: BitSet) => {
    val keys = b.map(masterList(_)).toList
    val fieldStr = keys.mkString(",")
    println(s"fieldStr $fieldStr")
    val q = ("?" * keys.length).mkString(",")
    println(s"q: $q")
    val tableSpec = if (args.keyspace.isEmpty) args.table else s"${args.keyspace}.${args.table}"
    val insertStr = s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL ${args.ttl}"""
    println(s"insertStr: $insertStr")
    session.prepare(insertStr)
  }


  private val cache: LoadingCache[BitSet, PreparedStatement] = CacheBuilder.newBuilder()
    .maximumSize(args.cacheSize)
    .build(loader)

  def apply(m: BitSet): PreparedStatement = cache.get(m)
}