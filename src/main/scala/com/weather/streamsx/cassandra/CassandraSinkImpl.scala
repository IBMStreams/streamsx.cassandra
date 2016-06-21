package com.weather.streamsx.cassandra

import com.datastax.driver.core.BoundStatement
import org.apache.logging.log4j.Logger
import com.ibm.streams.operator.StreamingInput
import com.ibm.streams.operator.Tuple



class CassandraSinkImpl {

  // WHERE DOES THE MAP COME FROM??? DOESN'T MAKE ANY SENSE AS A STATIC VARIABLE!!!
  // IT HAS TO COME IN DYNAMICALLY AS PART OF THE TUPLE

  /*
      Two ideas on where the map should come from.
      Either
      1. it should be passed in as a map in the tuple and the name of it should be given as a variable
      2. the tuples should get recomposed from Tuple<a, b, c> to Tuple<Tuple<a, boolean>, Tuple<b, boolean>, Tuple<c, boolean>>

      I'm leaning towards #2 here

   */

  def insertTuple(stream: StreamingInput[Tuple], tuple: Tuple, keyspace: String, table: String, ttl: Long): Unit = {

//    val bs: BoundStatement = TupleToStatement(tuple, )


  }
}
