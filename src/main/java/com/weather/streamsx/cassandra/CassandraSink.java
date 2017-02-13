package com.weather.streamsx.cassandra;

import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.metrics.*;

import com.weather.streamsx.cassandra.exception.CassandraWriterException;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Class for an operator that consumes tuples and does not produce an output stream.
 * This pattern supports a number of input streams and no output streams.
 * <P>
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li>
 * <li>process() handles a tuple arriving on an input port
 * <li>processPuncuation() handles a punctuation mark arriving on an input port
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time,
 * such as a request to stop a PE or cancel a job.
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks,
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other,
 * which lead to these methods being called concurrently by different threads.</p>
 */
@PrimitiveOperator(name="CassandraSink", namespace="com.weather.streamsx.cassandra",
        description="Java Operator CassandraSink")
@InputPorts({
        @InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious),
        @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)
})
public class CassandraSink extends AbstractOperator {

    protected Logger log = Logger.getLogger(this.getClass());


    private CassandraSinkImpl impl = null;
    private String connectionCfgObject = null;
    private String nullMapCfgObject = null;
    private String jsonAppConfig = null;
    private String jsonNullMap = null;

    private OperatorMetrics opMetrics = null;

    private Metric failures = null;
    private Metric successes = null;

    private String zkConnectionString = null;

    private Map<String, String> stringMapConnectionConfig = null;
    private Map<String, String> stringNullCfg = null;




    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void initialize(OperatorContext context)
            throws Exception {
        // Must call super.initialize(context) to correctly setup an operator.
        super.initialize(context);
        log.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        opMetrics = getOperatorContext().getMetrics();

        failures = opMetrics.createCustomMetric("nWriteFailures",
                "Number of tuples that failed to get written to Cassandra", Metric.Kind.COUNTER);
        successes = opMetrics.createCustomMetric("nWriteSuccesses",
                "Number of tuples that were written to Cassandra successfully", Metric.Kind.COUNTER);
//
//        stringMapConnectionConfig = context.getPE().getApplicationConfiguration(connectionCfgObject);
//        stringNullCfg = context.getPE().getApplicationConfiguration(nullMapCfgObject);


        // Connection config
        if(jsonAppConfig == null && connectionCfgObject == null){
            throw new Exception("Either jsonAppConfig or cryptoAppConfigName must be defined.");
        }
        else if(jsonAppConfig != null && connectionCfgObject != null) {
            throw new Exception("Both jsonAppConfig and cryptoAppConfigName are defined. Please use only one of these arguments.");
        }
        else if(jsonAppConfig == null) {
            stringMapConnectionConfig = context.getPE().getApplicationConfiguration(connectionCfgObject);
        }
        else {
            stringMapConnectionConfig = new Gson().fromJson( jsonAppConfig, new TypeToken<HashMap<String, String>>(){}.getType());
        }

        // Null map config
        if(jsonAppConfig == null && connectionCfgObject == null){
            stringNullCfg = null;
        }
        else if(jsonNullMap != null && nullMapCfgObject != null) {
            throw new Exception("Both jsonNullMap and nullMapCfgObject are defined. Please use only one of these arguments.");
        }
        else if(jsonNullMap == null) {
            stringNullCfg = context.getPE().getApplicationConfiguration(nullMapCfgObject);
        }
        else {
            stringNullCfg = new Gson().fromJson( jsonNullMap, new TypeToken<HashMap<String, String>>(){}.getType());
        }

        // Make the implementation
        if (impl == null) {
                impl = CassandraSinkImpl.mkWriter(stringMapConnectionConfig, stringNullCfg);
        }
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        if(impl != null){
            try{
                impl.insertTuple(tuple);
                if(successes != null) successes.increment();
            }
            catch(UnauthorizedException ue) {
                if(failures != null) failures.increment();
                throw ue;
            }
            catch(Exception e) {
                if(failures != null) failures.increment();
                log.error("Failed to write tuple to Cassandra.\n"+ stringifyStackTrace(e) );
            }
        }
        else throw CassandraWriterException.apply("The operator was not properly initialized", new Throwable());
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        log.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        if(impl != null) {
            impl.shutdown();
            impl = null;
        }
        // Must call super.shutdown()
        super.shutdown();
    }

    @Parameter(name="connectionCfgObject", description = "Name of the APPLICATION CONFIGURATION OBJECT where Cassandra connection configuration is stored. " +
            "Either jsonAppConfig or connectionCfgObject must be defined, defining both is an error. " +
            "Using connectionCfgObject is recommended.", optional = true)
    public void setConnectCfgObject(String s) { connectionCfgObject = s; }

    @Parameter(name="nullMapCfgObject", description = "Name of the APPLICATION CONFIGURATION OBJECT where the map of fieldnames to the value representing null is stored" +
            "Null maps do not necessarily need to be defined if your application does not require them. " +
            "If null maps are required, either nullMapCfgObject or jsonNullMap must be defined. Defining both is an error. " +
            "Using nullMapCfgObject is recommended.", optional = true)
    public void setNullMapCfgObject(String str) { nullMapCfgObject = str; }

    @Parameter(name="jsonAppConfig", description = "Connection configuration as a valid JSON String. " +
            "See the toolkit documentation for examples." +
            "Either jsonAppConfig or connectionCfgObject must be defined. Defining both is an error. " +
            "Using connectionCfgObject is recommended.", optional=true)
    public void setJsonAppConfig(String m) {jsonAppConfig = m;}

    @Parameter(name="jsonNullMap", description = "Null map configuration as a valid JSON String. " +
            "See the toolkit documentation for examples." +
            "Null maps do not necessarily need to be defined if your application does not require them. " +
            "If null maps are required, either nullMapCfgObject or jsonNullMap must be defined. Defining both is an error. " +
            "Using nullMapCfgObject is recommended.", optional = true)
    public void setNullMapJSON(String str) { jsonNullMap = str; }

    private String stringifyStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
