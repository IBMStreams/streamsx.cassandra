package com.weather.streamsx.TuplesToCassandra;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@com.ibm.streams.operator.model.PrimitiveOperator(name="TuplesSink", namespace="com.weather.streamsx.TuplesToCassandra", description="Java Operator TuplesSink")
@com.ibm.streams.operator.model.InputPorts(value={@com.ibm.streams.operator.model.InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @com.ibm.streams.operator.model.InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@com.ibm.streams.operator.internal.model.ShadowClass("com.weather.streamsx.TuplesToCassandra.TuplesSink")
@javax.annotation.Generated("com.ibm.streams.operator.internal.model.processors.ShadowClassGenerator")
public class TuplesSink$StreamsModel extends com.ibm.streams.operator.AbstractOperator
 {
}