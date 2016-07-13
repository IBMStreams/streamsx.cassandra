//package com.weather.streamsx.cassandra
//
//import io.circe._
//import io.circe.generic.semiauto._
//
//object DecoderStuff {
//  implicit val rdrDecoder: Decoder[CassSinkClientConfig] = deriveDecoder[CassSinkClientConfig]
//  implicit val rdrEncoder: Encoder[CassSinkClientConfig] = deriveEncoder[CassSinkClientConfig]
//
//  def read(znode: String): Option[CassSinkClientConfig] = ZkClient.zkCli.read[CassSinkClientConfig](znode)
//  def write(znode: String, cc: CassSinkClientConfig): Unit = ZkClient.zkCli.write(znode, cc)
//}
