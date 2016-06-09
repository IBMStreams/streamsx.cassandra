package com.weather.streamsx

import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.Type


case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr{
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)
  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))
}


//TODO: get keyspace and table as parameters in the operator. Gonna define some temps here for the meantime.

object PutTuplesInCassandra {

  val keyspaceTEMP = "keyspace"
  val tableTEMP = "table"
  val ttlTEMP: Long = 5

  def apply(t: Tuple, m: Map[String, Boolean], session: Session): Unit = {
    val schema = t.getStreamSchema

    val buffer = scala.collection.mutable.ListBuffer.empty[Attribute]
    for(i <- 0 until schema.getAttributeCount) {buffer += schema.getAttribute(i)}

    val attributes = buffer.toList

    val fields: List[Attr] = attributes.map(a => Attr(a, m)).sortBy(_.index)
  }

  private[util] def mkInsert(fields: Seq[String], keyspace: String, table: String, ttl: Long) = {
    val fieldStr = fields.sorted.mkString(",")
    val q = ("?" * fields.length).mkString(",")
    val tableSpec = if (keyspace.isEmpty) table else s"$keyspace.$table"
    s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL $ttl"""
  }

  private def getPreparedStatement(list: List[Attr], tuple: Tuple, session: Session): PreparedStatement = {
    val nonNullAttrs = list.filter(a => a.set)

    //    val fields = list.filter(a => a.set).map(a => a.name).toSeq
    //    val indices = list.filter(a => a.set).map(a => a.index)

    val fields  = nonNullAttrs.map(a => a.name).toSeq
    val values: List[Object] = { nonNullAttrs.map(a => getValueFromTuple(tuple, a)) }
    val ps = session.prepare(mkInsert(fields, keyspaceTEMP, tableTEMP, ttlTEMP))
  }

  def getBoundStatement(list: List[Attr], tuple: Tuple, session: Session): BoundStatement = {


    ps.bind(values.asInstanceOf[Seq[Object]]:_*)
  }


  def getValueFromTuple(tuple: Tuple, attr: Attr): Object = attr.typex.getLanguageType match {
    case "boolean" => tuple.getBoolean(attr.index)
//    case "enum" =>
    case "int8" => tuple.getByte(attr.index)
    case "int16" => tuple.getShort(attr.index)
    case "int32" => tuple.getInt(attr.index)
    case "int64" => tuple.getLong(attr.index)
    case "uint8" => tuple.getByte(attr.index)
    case "uint16" => tuple.getShort(attr.index)
    case "uint32" => tuple.getInt(attr.index)
    case "uint64" => tuple.getLong(attr.index)
    case "float32" => tuple.getFloat(attr.index)
    case "float64" => tuple.getDouble(attr.index)
    case "decimal32" => tuple.getBigDecimal(attr.index)
    case "decimal64" => tuple.getBigDecimal(attr.index)
    case "decimal128" => tuple.getBigDecimal(attr.index)
    case "complex32" =>
    case "complex64" =>
    case "timestamp" => tuple.getTimestamp(attr.index)
    case "rstring" => tuple.getString(attr.index)
    case "ustring" => tuple.getString(attr.index)
    case "blob" => tuple.getBlob(attr.index)
    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
    case "list" => tuple.getList(attr.index) //I'm dubious, I don't think collection types are going to be this easy
    //I wonder if there will need to be more specific qualifications with list<boolean>, list<int>, etc
    case "map" => tuple.getMap(attr.index) //same dubiosity for maps as for lists
//    case "tuple" => tuple.getTuple(attr.index)
    case "" => "figure out better error logging than this"
  }


//  def getBoundStatement(list: List[Attr], ps: PreparedStatement): BoundStatement = {
//    values =
//
//    return null
//  }

/*

tuples come in from streams along with some indication of whether each value is null

  I can massage this format into a list of case class Attr()s

  From these Attr()s I need to create a prepared statement using only the fields that are not null
    There's going to be a lot of repitition in these prepared statements, there should be some caching
    The cache can be a LRU style where the presence of nulls in the statement maps to a bit string, and the bit string is what's compared.
    I'm distracted thinking about some debugging here for when fields change and/or the number of fields changes
      The first time a tuple comes in, hash the field names into some UUID.
      Check each tuple's field names against this hash.
      If the two hashes are ever not equal, replace the hash and invalidate the bitmask cache because there's new/different fields.
  When I have the prepared statement, I can turn that into a bound statement by binding it to the values that are not null from the tuple
  I can have a session that asynchronously inserts the bound statement into Cassandra

*/


}
