package com.weather.streamsx

import org.apache.logging.log4j.Logger

import com.ibm.streams.operator.AbstractOperator
import com.ibm.streams.operator.OperatorContext
import com.ibm.streams.operator.StreamingData.Punctuation
import com.ibm.streams.operator.StreamingInput
import com.ibm.streams.operator.Tuple
import com.ibm.streams.operator.model.InputPortSet
import com.ibm.streams.operator.model.InputPortSet.WindowMode
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode
import com.ibm.streams.operator.model.InputPorts
import com.ibm.streams.operator.model.PrimitiveOperator

//object CassandraSink {
//  private val log = org.slf4j.LoggerFactory.getLogger(getClass)
//  private val lock = new Object
//}


@PrimitiveOperator ( name = "CassandraSink", namespace = "com.weather.streamsx.CassandraSink", description = "Java Operator CassandraSink")
@InputPorts (
  Array (
    new InputPortSet (
      description = "Port that ingests tuples",
      cardinality = 1,
      optional = false,
      windowingMode = WindowMode.NonWindowed,
      windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious
    ), 
    new InputPortSet (
      description = "Optional input ports",
      optional = true,
      windowingMode = WindowMode.NonWindowed,
      windowPunctuationInputMode = WindowPunctuationInputMode.Oblivious
    ) 
  ) 
)
class CassandraSink extends AbstractOperator {
//  import CassandraSink.{log, lock}

  override def process(stream: StreamingInput[Tuple], tuple: Tuple): Unit = {
    println("I sure gots a tuple" + tuple.getInt(0))
  }
}
