package com.weather.streamsx.cassandra


import com.datastax.driver.core.{Session, PreparedStatement, BoundStatement}
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.Attribute
import com.ibm.streams.operator.Type
import com.ibm.streams.operator.meta.{CollectionType, MapType}
import com.ibm.streams.operator.types.RString
import collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}

case class Attr(index: Int, name: String, typex: Type, set: Boolean)

object Attr{
  def apply(a: Attribute, b: Boolean): Attr = Attr(a.getIndex, a.getName, a.getType, b)
  def apply(a: Attribute, m: Map[String, Boolean]): Attr = apply(a, m(a.getName))
}


//TODO: get keyspace and table as parameters in the operator. Gonna define some temps here for the meantime.

object TupleToStatement {

//  val keyspaceTEMP = "keyspace"
//  val tableTEMP = "table"
//  val ttlTEMP: Long = 604800 //one week,

  def apply(t: Tuple, session: Session, keyspace: String, table: String, ttl: Long, nullMapName: String): BoundStatement = {
    val schema = t.getStreamSchema


    val buffer = scala.collection.mutable.ListBuffer.empty[Attribute]
    for(i <- 0 until schema.getAttributeCount) {buffer += schema.getAttribute(i)}

    // this is going to be useful for handling general collection maps, but if getting the nullMap as Map[String, Boolean] fails,
    // then so much the better

//    val maptype: MapType = schema.getAttribute(nullMapName).getType.asInstanceOf[MapType]
//
//    val keyT: Type = maptype.getKeyType
//    val valT: Type = maptype.getValueType
//
//    keyT.getObjectType


    val mizzap = t.getMap(nullMapName).asInstanceOf[java.util.Map[RString, Boolean]]
    val rstringMap = mizzap.toMap
    val m = rstringMap.map(kv => (kv._1.toString, kv._2))

    val attributes = buffer.sortBy(a => a.getIndex).toList

    // remove the nullMap attribute from this list
    val fields: List[Attr] = attributes.filterNot(a => a.getName == nullMapName).map(a => Attr(a, m))
    val nonNullAttrs = fields.filter(a => a.set)
    val ps = mkPreparedStatement(nonNullAttrs, t, session, keyspace, table, ttl)

    val bs = getBoundStatement(ps, nonNullAttrs, t)
    println(bs.toString)
    bs
//    null
  }

  def mkInsert(fields: Seq[String], keyspace: String, table: String, ttl: Long) = {
    val fieldStr = fields.mkString(",")
    val q = ("?" * fields.length).mkString(",")
    val tableSpec = if (keyspace.isEmpty) table else s"$keyspace.$table"
    s"""INSERT INTO $tableSpec ($fieldStr) VALUES ($q) USING TTL $ttl"""
  }

  def mkPreparedStatement(nonNullAttrs: List[Attr], tuple: Tuple, session: Session, keyspace: String, table: String, ttl: Long): PreparedStatement = {
    val fields  = nonNullAttrs.map(a => a.name).toSeq
    //There should be a caching layer here
    val ps = session.prepare(mkInsert(fields, keyspace, table, ttl))
    println(ps.getQueryString)
    println(fields)
    ps
  }

  def getBoundStatement(ps: PreparedStatement, nonNullAttrs: List[Attr], tuple: Tuple): BoundStatement = {
    val values: List[Any] = { nonNullAttrs.map(a => getValueFromTuple(tuple, a)) }
    println(values)
    val zero = values(0)
    val one = values(1)

    def f[T](v: T) = v match {
      case _: Long    => "Long"
      case _: String => "String"
      case _         => "Unknown"
    }
    println(s"value of greeting is $zero which is type ${f(zero)}")
    println(s"value of count    is $one which is type ${f(one)}")
    ps.bind(values.asInstanceOf[Seq[Object]]:_*)
  }

  def getValueFromTuple(tuple: Tuple, attr: Attr): Any = attr.typex.getLanguageType match {
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
//    case "complex32" => There's no Java equivalent type for either complex32 or complex64.
//    case "complex64" => See the table in the Streams documentation showing SPL to Java equivalent types for more info.
    case "timestamp" => tuple.getTimestamp(attr.index)
    case "rstring" => tuple.getString(attr.index)
    case "ustring" => tuple.getString(attr.index)
    case "blob" => tuple.getBlob(attr.index)
    case "xml" => tuple.getXML(attr.index).toString //Cassandra doesn't have XML as data type, thank goodness
    case s if s.startsWith("list") => {



      getListWithProperType(tuple, attr)
    }
    //I wonder if there will need to be more specific qualifications with list<boolean>, list<int>, etc
    case "map" => tuple.getMap(attr.index) //same dubiosity for maps as for lists
//    case "tuple" => tuple.getTuple(attr.index)
    case _ => s"APPARENTLY I DUNNO WTF THIS TYPE IS: ${attr.typex.getLanguageType}"
  }


  // extract the sub type whether by java class or string or whatever
  // write a method def getList(T): List[T]
  // use that method to get List[Int] or List[String] or whatever









  def getListWithProperType(tuple: Tuple, attr: Attr): List[Any] = {
    val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
    val blah = attr.typex.getObjectType
    val elementT: Class[_] = listType.getElementType.getObjectType
    val rawList = tuple.getList(attr.index)

    println(s"the languageType is ${listType.getLanguageType}")
    val ob = listType.getAsCompositeElementType

    val objectClass = attr.typex.getObjectType
    println(s"THE ELEMENT TYPE IS: ${elementT.getName} AND THE COMPOSITE CLASS TYPE IS ${ob.getName}")

    val obb = listType.

    listType.getLanguageType match {
      case "Int" => rawList.asInstanceOf[java.util.List[Int]].toList
      case _ => rawList.toList
    }
  }

//  def getListWithProperTypeXXXXXX[T](tuple: Tuple, attr: Attr): List[T] = {
//    val listType: CollectionType = attr.typex.asInstanceOf[CollectionType]
//
//    val ob = listType.getAsCompositeElementType
//
//    val objectClass = attr.typex.getObjectType
//    println(s"THE OBJECT CLASS IS: ${objectClass.getName} AND THE COMPOSITE CLASS TYPE IS ${ob.getName}")
//    val mirror = runtimeMirror(objectClass.getClassLoader)  // obtain runtime mirror
//    val sym = mirror.staticClass(objectClass.getName)  // obtain class symbol for `c`
//    val tpe = sym.selfType  // obtain type object for `c`
//    val typeTag = TypeTag(mirror, new TypeCreator {
//      def apply[U <: Universe with Singleton](m: api.Mirror[U]) =
//        if (m eq mirror) tpe.asInstanceOf[U # Type]
//        else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
//    })
//
//  }


  def getTypeTag[T: ru.TypeTag](obj: (T) => Any) = {
    ru.typeTag[T]
  }


}
