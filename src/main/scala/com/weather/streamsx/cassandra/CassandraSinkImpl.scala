package com.weather.streamsx.cassandra

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


class CassandraSinkImpl {
//  import CassandraSink.{log, lock}

  def process(stream: StreamingInput[Tuple], tuple: Tuple): Unit = {

  }
}
